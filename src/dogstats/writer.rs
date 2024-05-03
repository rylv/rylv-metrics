#[cfg(target_os = "linux")]
use rustix::net::SocketAddrAny;
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;

use std::io::IoSlice;
use std::net::{SocketAddr, UdpSocket};

use crate::{MetricResult, StatsWriterType};

// Apple-specific imports for sendmmsg_x
use std::mem::transmute;
#[cfg(target_vendor = "apple")]
use std::os::fd::AsRawFd;

#[cfg(target_vendor = "apple")]
use crate::dogstats::net::{msghdr_x, sendmsg_x};

pub trait Writer {
    fn write(&self, buf: &[u8]) -> std::io::Result<usize>;

    #[cfg(target_os = "linux")]
    fn write_mvec(&self, pool_msg_headers: &mut [rustix::net::MMsgHdr<'_>]) -> MetricResult<usize>;

    #[cfg(target_os = "linux")]
    fn get_destination(&self) -> &SocketAddrAny;

    #[cfg(target_vendor = "apple")]
    fn get_destination_addr(&self) -> libc::sockaddr_in;

    #[cfg(target_vendor = "apple")]
    fn as_raw_fd(&self) -> libc::c_int;
}

impl<T> Writer for &T
where
    T: Writer,
{
    fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        (*self).write(buf)
    }

    #[cfg(target_os = "linux")]
    fn write_mvec(&self, pool_msg_headers: &mut [rustix::net::MMsgHdr<'_>]) -> MetricResult<usize> {
        (*self).write_mvec(pool_msg_headers)
    }

    #[cfg(target_os = "linux")]
    fn get_destination(&self) -> &SocketAddrAny {
        (*self).get_destination()
    }

    #[cfg(target_vendor = "apple")]
    fn get_destination_addr(&self) -> libc::sockaddr_in {
        (*self).get_destination_addr()
    }

    #[cfg(target_vendor = "apple")]
    fn as_raw_fd(&self) -> libc::c_int {
        (*self).as_raw_fd()
    }
}

pub struct UdpSocketWriter {
    pub sock: UdpSocket,
    #[cfg(target_os = "linux")]
    pub destination: SocketAddrAny,
    pub destination_addr: SocketAddr,
}

impl Writer for UdpSocketWriter {
    fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        let r = self.sock.send_to(buf, self.destination_addr);
        if let Err(ref err) = r {
            tracing::warn!("UDP send error: {err}");
        }
        r
    }

    #[cfg(target_os = "linux")]
    fn write_mvec(&self, pool_msg_headers: &mut [rustix::net::MMsgHdr<'_>]) -> MetricResult<usize> {
        if pool_msg_headers.is_empty() {
            Ok(0)
        } else {
            rustix::net::sendmmsg(
                self.sock.as_fd(),
                pool_msg_headers,
                rustix::net::SendFlags::empty(),
            )
            .map_err(std::convert::Into::into)
        }
    }

    #[cfg(target_os = "linux")]
    fn get_destination(&self) -> &SocketAddrAny {
        &self.destination
    }

    #[cfg(target_vendor = "apple")]
    fn get_destination_addr(&self) -> libc::sockaddr_in {
        use std::net::SocketAddr;
        match self.destination_addr {
            SocketAddr::V4(addr) => {
                let octets = addr.ip().octets();
                #[allow(clippy::cast_possible_truncation)]
                libc::sockaddr_in {
                    sin_len: size_of::<libc::sockaddr_in>() as u8,
                    sin_family: libc::AF_INET as u8,
                    sin_port: addr.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(octets),
                    },
                    sin_zero: [0; 8],
                }
            }
            SocketAddr::V6(_) => {
                unreachable!("IPv6 not supported for Apple batch writer")
            }
        }
    }

    #[cfg(target_vendor = "apple")]
    fn as_raw_fd(&self) -> libc::c_int {
        self.sock.as_raw_fd()
    }
}

/// Trait for implementing custom metric writers.
///
/// Implement this trait to send metrics to custom destinations or
/// to add custom formatting/batching logic.
pub trait StatsWriterTrait {
    /// Returns whether metrics are copied to an internal buffer before sending.
    fn metric_copied(&self) -> bool;
    /// Writes metrics to the underlying writer.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` if the write operation fails.
    fn write(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: &str,
    ) -> MetricResult<()>;

    /// Flushes the writer.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` on I/O failure.
    fn flush(&mut self) -> MetricResult<usize>;

    /// Resets the writer state, clearing any internal buffers.
    fn reset(&mut self);
}

pub struct StatsWriterHolder {
    writer: Box<dyn StatsWriterTrait>,
}

impl StatsWriterHolder {
    pub fn new<T: Writer + 'static>(
        writer: T,
        writer_type: StatsWriterType,
        stats_prefix: String,
        max_udp_packet_size: u16,
        max_udp_batch_size: u32,
    ) -> Self {
        let stats_writer = match writer_type {
            StatsWriterType::Simple => Box::new(StatsWriterSimple::new(
                writer,
                stats_prefix,
                max_udp_packet_size,
            )) as Box<dyn StatsWriterTrait>,

            #[cfg(target_os = "linux")]
            StatsWriterType::LinuxBatch => Box::new(StatsWriterLinux::new(
                writer,
                stats_prefix,
                max_udp_batch_size,
                max_udp_packet_size,
            )) as Box<dyn StatsWriterTrait>,

            #[cfg(target_vendor = "apple")]
            StatsWriterType::AppleBatch => Box::new(StatsWriterApple::new(
                writer,
                stats_prefix,
                max_udp_batch_size,
                max_udp_packet_size,
            )) as Box<dyn StatsWriterTrait>,

            StatsWriterType::Custom(writer) => writer,
        };

        Self {
            writer: stats_writer,
        }
    }

    pub fn acquire(&mut self) -> StatsGuard<'_> {
        StatsGuard {
            writer: self.writer.as_mut(),
        }
    }
}

pub struct StatsGuard<'a> {
    writer: &'a mut dyn StatsWriterTrait,
}

impl Drop for StatsGuard<'_> {
    fn drop(&mut self) {
        self.writer.reset();
    }
}

impl StatsWriterTrait for StatsGuard<'_> {
    fn metric_copied(&self) -> bool {
        self.writer.metric_copied()
    }

    fn write<'data>(
        &mut self,
        metrics: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: &'data str,
    ) -> MetricResult<()> {
        self.writer.write(metrics, tags, value, metric_type)
    }

    fn flush(&mut self) -> MetricResult<usize> {
        self.writer.flush()
    }

    fn reset(&mut self) {
        self.writer.reset();
    }
}

#[cfg(target_os = "linux")]
pub struct StatsWriterLinux<T> {
    max_udp_packet_size: u16,
    writer: T,
    stats_prefix: String,

    // current state
    queued_transmits: Vec<super::writer_utils::Transmit<'static>>,
    current_transmit: super::writer_utils::Transmit<'static>,

    // for reuse in application lifetime
    pool_transmits: Vec<super::writer_utils::Transmit<'static>>,
    tmp_mmsghdrs: Vec<rustix::net::MMsgHdr<'static>>,
}

#[cfg(target_os = "linux")]
impl<T: Writer> StatsWriterLinux<T> {
    pub fn new(
        writer: T,
        stats_prefix: String,
        max_udp_batch_size: u32,
        max_udp_packet_size: u16,
    ) -> Self {
        let max_udp_batch_size = max_udp_batch_size as usize;
        Self {
            max_udp_packet_size,
            writer,
            stats_prefix,

            queued_transmits: Vec::with_capacity(max_udp_batch_size),
            current_transmit: super::writer_utils::Transmit::new(max_udp_packet_size),

            pool_transmits: Vec::with_capacity(max_udp_batch_size),
            tmp_mmsghdrs: Vec::with_capacity(max_udp_batch_size),
        }
    }

    fn queue_current_transmit(&mut self) {
        let new_current = self
            .pool_transmits
            .pop()
            .unwrap_or_else(|| super::writer_utils::Transmit::new(self.max_udp_packet_size));
        let old_transmit = std::mem::replace(&mut self.current_transmit, new_current);
        self.queued_transmits.push(old_transmit);
    }

    fn flush_queued_transmits(&mut self) -> MetricResult<usize> {
        let res = if self.queued_transmits.is_empty() {
            0
        } else {
            let destination = self.writer.get_destination();

            assert!(self.tmp_mmsghdrs.is_empty());

            for transmit in &mut self.queued_transmits {
                // SAFETY: pool_msg_headers is only used in this function, so it is safe to transmute
                // the pool_msg_headers is cached outside for performance reason
                let mmsghdr = unsafe {
                    std::mem::transmute::<rustix::net::MMsgHdr<'_>, rustix::net::MMsgHdr<'_>>(
                        transmit.create_mmsghdr(destination),
                    )
                };
                self.tmp_mmsghdrs.push(mmsghdr);
            }

            let result = self.writer.write_mvec(&mut self.tmp_mmsghdrs);
            self.tmp_mmsghdrs.clear();
            result?
        };

        // return to queue for future reuse
        while let Some(mut transmit) = self.queued_transmits.pop() {
            transmit.reset();
            self.pool_transmits.push(transmit);
        }
        Ok(res)
    }

    pub fn flush(&mut self) -> MetricResult<usize> {
        if self.current_transmit.len() > 0 {
            self.queue_current_transmit();
        }
        self.flush_queued_transmits()
    }
}

#[cfg(target_os = "linux")]
impl<T: Writer> StatsWriterTrait for StatsWriterLinux<T> {
    fn metric_copied(&self) -> bool {
        false
    }

    fn write(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: &str,
    ) -> MetricResult<()> {
        // Manually build this line
        // format!("{}:{}|{}|#{}\n", metric, value, metric_type, tags);
        let metric_len = metric_len(
            self.stats_prefix.as_str(),
            metrics,
            tags,
            value,
            metric_type,
        );

        let (metrics, tags, value, metric_type): (
            &[&'static str],
            &'static str,
            &'static str,
            &'static str,
        ) = unsafe {
            (
                transmute::<&[&str], &[&str]>(metrics),
                transmute::<&str, &str>(tags),
                transmute::<&str, &str>(value),
                transmute::<&str, &str>(metric_type),
            )
        };
        let stats_prefix: &'static str = unsafe { transmute(self.stats_prefix.as_str()) };

        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        #[allow(clippy::cast_possible_truncation)]
        if !self.current_transmit.enough_space_for(metric_len as u16) {
            self.queue_current_transmit();
        }

        self.current_transmit
            .push(IoSlice::new(stats_prefix.as_bytes()));

        for metric in metrics {
            self.current_transmit.push(IoSlice::new(metric.as_bytes()));
        }

        self.current_transmit.push(IoSlice::new(b":"));
        self.current_transmit.push(IoSlice::new(value.as_bytes()));
        self.current_transmit.push(IoSlice::new(b"|"));
        self.current_transmit
            .push(IoSlice::new(metric_type.as_bytes()));
        if !tags.is_empty() {
            self.current_transmit.push(IoSlice::new(b"|#"));
            self.current_transmit.push(IoSlice::new(tags.as_bytes()));
        }
        self.current_transmit.push(IoSlice::new(b"\n"));

        if self.queued_transmits.len() == self.queued_transmits.capacity() {
            tracing::warn!("queued transmits len: {}", self.queued_transmits.len());
            self.flush_queued_transmits()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        self.flush()
    }
    fn reset(&mut self) {
        // SAFETY: stats writers have been dropped, so there are no pointers to bump after the bump is reset
        self.queued_transmits.clear();
        self.tmp_mmsghdrs.clear();
    }
}

// ============================================================================
// Apple-specific batch writer using sendmsg_x
// ============================================================================
#[cfg(target_vendor = "apple")]
pub struct StatsWriterApple<T> {
    max_udp_packet_size: u16,
    writer: T,
    stats_prefix: String,

    // Used in processing time
    // This way we can reuse the same transmit multiples times using 'static lifetime
    // and little unsafe transmute because we know that the transmit is not used after the processing
    queued_transmits: Vec<super::writer_utils::Transmit<'static>>,
    current_transmit: super::writer_utils::Transmit<'static>,

    // for reuse in application lifetime
    // If not processing this pools are empty
    // This way we can reuse the same transmit multiples times using 'static lifetime
    // and little unsafe transmute because we know that the transmit is not used after the processing
    pool_transmits: Vec<super::writer_utils::Transmit<'static>>,

    // Used in processing time to avoid allocations
    tmp_mmsghdrs: Vec<msghdr_x>,
}

#[inline]
fn metric_len(prefix: &str, metrics: &[&str], tags: &str, value: &str, metric_type: &str) -> usize {
    // format!("{}:{}|{}\n", metric, value, metric_type) when tags is empty
    // format!("{}:{}|{}|#{}\n", metric, value, metric_type, tags) when tags is not empty
    let mut metric_len = prefix.len() + value.len() + metric_type.len() + tags.len() + 3; // ':' + '|' + '\n'

    if !tags.is_empty() {
        metric_len += 2; // '|#'
    }

    for metric in metrics {
        metric_len += metric.len();
    }
    metric_len
}

#[cfg(target_vendor = "apple")]
impl<T: Writer> StatsWriterApple<T> {
    pub fn new(
        writer: T,
        stats_prefix: String,
        max_udp_batch_size: u32,
        max_udp_packet_size: u16,
    ) -> Self {
        let max_udp_batch_size = max_udp_batch_size as usize;
        Self {
            max_udp_packet_size,
            writer,
            stats_prefix,
            queued_transmits: Vec::with_capacity(max_udp_batch_size),
            pool_transmits: Vec::with_capacity(max_udp_batch_size),
            tmp_mmsghdrs: Vec::with_capacity(max_udp_batch_size),
            current_transmit: super::writer_utils::Transmit::new(max_udp_packet_size),
        }
    }

    fn queue_current_transmit(&mut self) {
        let new_current = self
            .pool_transmits
            .pop()
            .unwrap_or_else(|| super::writer_utils::Transmit::new(self.max_udp_packet_size));
        let old_transmit = std::mem::replace(&mut self.current_transmit, new_current);
        self.queued_transmits.push(old_transmit);
    }

    fn flush_queued_transmits(&mut self) -> MetricResult<usize> {
        if self.queued_transmits.is_empty() {
            return Ok(0);
        }

        let destination_addr = self.writer.get_destination_addr();
        let sock_fd = self.writer.as_raw_fd();

        // Prepare msghdr_x structures for batch sending
        let mut sockaddr_storage = destination_addr;
        assert!(self.tmp_mmsghdrs.is_empty());

        for transmit in &mut self.queued_transmits {
            let iovecs = transmit.get_iovecs();

            // Calculate total data length for msg_datalen
            let total_len: libc::size_t = iovecs.iter().map(|iov| iov.len()).sum();

            #[allow(
                clippy::cast_possible_wrap,
                clippy::cast_possible_truncation,
                clippy::as_ptr_cast_mut
            )]
            self.tmp_mmsghdrs.push(msghdr_x {
                msg_name: (&raw mut sockaddr_storage).cast::<libc::c_void>(),
                msg_namelen: size_of_val(&sockaddr_storage) as libc::socklen_t,
                // SAFETY: IoSlice is repr(transparent) over libc::iovec on Unix
                msg_iov: iovecs.as_ptr() as *mut libc::iovec,
                msg_iovlen: iovecs.len() as libc::c_int,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
                msg_datalen: total_len,
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let result = unsafe {
            sendmsg_x(
                sock_fd,
                self.tmp_mmsghdrs.as_ptr(),
                self.tmp_mmsghdrs.len() as libc::c_uint,
                0,
            )
        };
        self.tmp_mmsghdrs.clear();

        if result < 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        // Return transmits to pool for reuse
        while let Some(mut transmit) = self.queued_transmits.pop() {
            transmit.reset();
            self.pool_transmits.push(transmit);
        }

        #[allow(clippy::cast_sign_loss)]
        Ok(result as usize)
    }

    pub fn flush(&mut self) -> MetricResult<usize> {
        if self.current_transmit.len() > 0 {
            self.queue_current_transmit();
        }
        self.flush_queued_transmits()
    }
}

#[cfg(target_vendor = "apple")]
impl<T: Writer> StatsWriterTrait for StatsWriterApple<T> {
    fn metric_copied(&self) -> bool {
        false
    }

    fn write<'data>(
        &mut self,
        metrics: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: &'data str,
    ) -> MetricResult<()> {
        let (metrics, tags, value, metric_type) = unsafe {
            (
                transmute::<&[&str], &[&str]>(metrics),
                transmute::<&str, &str>(tags),
                transmute::<&str, &str>(value),
                transmute::<&str, &str>(metric_type),
            )
        };
        let stats_prefix: &'static str = unsafe { transmute(self.stats_prefix.as_str()) };

        let metric_len = metric_len(
            self.stats_prefix.as_str(),
            metrics,
            tags,
            value,
            metric_type,
        );

        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        #[allow(clippy::cast_possible_truncation)]
        if !self.current_transmit.enough_space_for(metric_len as u16) {
            self.queue_current_transmit();
        }

        self.current_transmit
            .push(IoSlice::new(stats_prefix.as_bytes()));
        for metric in metrics {
            self.current_transmit.push(IoSlice::new(metric.as_bytes()));
        }
        self.current_transmit.push(IoSlice::new(b":"));
        self.current_transmit.push(IoSlice::new(value.as_bytes()));
        self.current_transmit.push(IoSlice::new(b"|"));
        self.current_transmit
            .push(IoSlice::new(metric_type.as_bytes()));
        if !tags.is_empty() {
            self.current_transmit.push(IoSlice::new(b"|#"));
            self.current_transmit.push(IoSlice::new(tags.as_bytes()));
        }
        self.current_transmit.push(IoSlice::new(b"\n"));

        if self.queued_transmits.len() == self.queued_transmits.capacity() {
            self.flush_queued_transmits()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        self.flush()
    }

    fn reset(&mut self) {
        // SAFETY NOTE: so there are no pointers to bump after the bump is reset
        // At this point current_transmit and queued_transmits should be empty because
        // this reset is executed after flush
        self.current_transmit.reset();
        self.queued_transmits.clear();

        self.tmp_mmsghdrs.clear();
    }
}

pub struct StatsWriterSimple<T> {
    max_udp_packet_size: u16,
    writer: T,
    stats_prefix: String,
    current_transmit: String,
}

impl<T: Writer> StatsWriterSimple<T> {
    pub fn new(writer: T, stats_prefix: String, max_udp_packet_size: u16) -> Self {
        Self {
            max_udp_packet_size,
            writer,
            stats_prefix,
            current_transmit: String::with_capacity(max_udp_packet_size as usize),
        }
    }

    fn flush_current_transmit(&mut self) -> MetricResult<usize> {
        if !self.current_transmit.is_empty() {
            let result = self.writer.write(self.current_transmit.as_bytes())?;
            // only flush when no error occurs
            self.current_transmit.clear();
            return Ok(result);
        }
        Ok(0)
    }
}

impl<T: Writer> StatsWriterTrait for StatsWriterSimple<T> {
    fn metric_copied(&self) -> bool {
        true
    }

    fn write<'data>(
        &mut self,
        metrics: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: &'data str,
    ) -> MetricResult<()> {
        // Calculate the metric length
        let metric_len = metric_len(
            self.stats_prefix.as_str(),
            metrics,
            tags,
            value,
            metric_type,
        );

        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        // If not enough space, queue current transmit
        if self.current_transmit.len() + metric_len > self.max_udp_packet_size as usize {
            self.flush_current_transmit()?;
        }

        // Build the metric string
        self.current_transmit.push_str(self.stats_prefix.as_str());
        for metric in metrics {
            self.current_transmit.push_str(metric);
        }
        self.current_transmit.push(':');
        self.current_transmit.push_str(value);
        self.current_transmit.push('|');
        self.current_transmit.push_str(metric_type);
        if !tags.is_empty() {
            self.current_transmit.push_str("|#");
            self.current_transmit.push_str(tags);
        }
        self.current_transmit.push('\n');

        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        self.flush_current_transmit()
    }

    fn reset(&mut self) {
        self.current_transmit.clear();
    }
}
