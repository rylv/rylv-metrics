#[cfg(target_os = "linux")]
use rustix::net::SocketAddrAny;
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;

use std::io::IoSlice;
use std::marker::PhantomData;
use std::net::{SocketAddr, UdpSocket};

use crate::{MetricKind, MetricResult, StatsWriterType};

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
        self.sock.send_to(buf, self.destination_addr)
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
            SocketAddr::V6(_) => libc::sockaddr_in {
                sin_len: 0,
                sin_family: 0,
                sin_port: 0,
                sin_addr: libc::in_addr { s_addr: 0 },
                sin_zero: [0; 8],
            },
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
    /// Returns whether formatting may use a temporary stack buffer.
    ///
    /// Custom writers must consume or copy input before `write` returns. Returning
    /// `false` requests arena-backed numeric formatting, but does not extend the
    /// lifetime of references supplied to this safe trait.
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
        metric_type: MetricKind,
    ) -> MetricResult<()>;

    /// Flushes the writer.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` on I/O failure.
    fn flush(&mut self) -> MetricResult<usize>;

    /// Resets the writer state, clearing any internal buffers.
    fn reset(&mut self);
}

// Only StatsGuard calls this internal interface. Backends may erase lifetimes
// to reuse descriptor allocations, but must release every retained reference in
// reset(), including after failed writes/flushes. No borrowed bytes are owned here.
trait BorrowingStatsWriter {
    fn metric_copied(&self) -> bool;

    /// Caller keeps all referenced bytes alive and immutable until `reset()`.
    /// The outer slice of metric parts only needs to live for this call.
    /// A backend returning `metric_copied() == true` must not retain input references
    /// during this session (until the next `reset()`).
    unsafe fn write_borrowed(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: MetricKind,
    ) -> MetricResult<()>;

    fn flush(&mut self) -> MetricResult<usize>;
    fn reset(&mut self);
}

impl<T: StatsWriterTrait> BorrowingStatsWriter for T {
    fn metric_copied(&self) -> bool {
        StatsWriterTrait::metric_copied(self)
    }

    unsafe fn write_borrowed(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        kind: MetricKind,
    ) -> MetricResult<()> {
        StatsWriterTrait::write(self, metrics, tags, value, kind)
    }

    fn flush(&mut self) -> MetricResult<usize> {
        StatsWriterTrait::flush(self)
    }
    fn reset(&mut self) {
        StatsWriterTrait::reset(self);
    }
}

#[cfg(feature = "custom_writer")]
struct CustomWriter(Box<dyn StatsWriterTrait + Send + Sync>);

#[cfg(feature = "custom_writer")]
impl StatsWriterTrait for CustomWriter {
    fn metric_copied(&self) -> bool {
        self.0.metric_copied()
    }
    fn write(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        kind: MetricKind,
    ) -> MetricResult<()> {
        self.0.write(metrics, tags, value, kind)
    }
    fn flush(&mut self) -> MetricResult<usize> {
        self.0.flush()
    }
    fn reset(&mut self) {
        self.0.reset();
    }
}

pub struct StatsWriterHolder {
    writer: Box<dyn BorrowingStatsWriter>,
}

impl StatsWriterHolder {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new<T: Writer + 'static>(
        writer: T,
        writer_type: StatsWriterType,
        max_udp_packet_size: u16,
        max_udp_batch_size: u32,
    ) -> Self {
        let stats_writer = match writer_type {
            StatsWriterType::Simple => {
                Box::new(StatsWriterSimple::new(writer, max_udp_packet_size))
                    as Box<dyn BorrowingStatsWriter>
            }

            #[cfg(target_os = "linux")]
            StatsWriterType::LinuxBatch => Box::new(StatsWriterLinux::new(
                writer,
                max_udp_batch_size,
                max_udp_packet_size,
            )) as Box<dyn BorrowingStatsWriter>,

            #[cfg(target_vendor = "apple")]
            StatsWriterType::AppleBatch => Box::new(StatsWriterApple::new(
                writer,
                max_udp_batch_size,
                max_udp_packet_size,
            )) as Box<dyn BorrowingStatsWriter>,

            #[cfg(feature = "custom_writer")]
            StatsWriterType::Custom(writer) => Box::new(CustomWriter(writer)),
        };

        Self {
            writer: stats_writer,
        }
    }

    pub fn acquire<'data>(&mut self) -> StatsGuard<'_, 'data> {
        StatsGuard::new(self.writer.as_mut())
    }
}

// The persistent backend owns descriptor capacity; this session owns its borrows.
// Drop clears retained pointers and returns descriptors to the reusable pool.
pub struct StatsGuard<'writer, 'data> {
    writer: &'writer mut dyn BorrowingStatsWriter,
    copies_input: bool,
    data: PhantomData<&'data str>,
}

impl Drop for StatsGuard<'_, '_> {
    fn drop(&mut self) {
        self.writer.reset();
    }
}

impl<'writer, 'data> StatsGuard<'writer, 'data> {
    fn new(writer: &'writer mut dyn BorrowingStatsWriter) -> Self {
        let copies_input = writer.metric_copied();
        Self {
            writer,
            copies_input,
            data: PhantomData,
        }
    }

    pub const fn metric_copied(&self) -> bool {
        self.copies_input
    }

    pub fn write(
        &mut self,
        metrics: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: MetricKind,
    ) -> MetricResult<()> {
        // SAFETY: all bytes are borrowed for this session's 'data lifetime.
        // Drop clears references before the caller can invalidate those bytes.
        unsafe {
            self.writer
                .write_borrowed(metrics, tags, value, metric_type)
        }
    }

    pub fn write_copied(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: MetricKind,
    ) -> MetricResult<()> {
        if !self.copies_input {
            return Err("writer retains borrowed input; use the scoped write method".into());
        }
        // SAFETY: the backend consumes/copies input before returning.
        unsafe {
            self.writer
                .write_borrowed(metrics, tags, value, metric_type)
        }
    }

    pub fn flush(&mut self) -> MetricResult<usize> {
        self.writer.flush()
    }
}

#[cfg(target_os = "linux")]
pub struct StatsWriterLinux<T> {
    max_udp_packet_size: u16,
    writer: T,

    // current state
    queued_transmits: Vec<super::writer_utils::Transmit<'static>>,
    current_transmit: super::writer_utils::Transmit<'static>,

    // for reuse in application lifetime
    pool_transmits: Vec<super::writer_utils::Transmit<'static>>,
    tmp_mmsghdrs: Vec<rustix::net::MMsgHdr<'static>>,
}

#[cfg(target_os = "linux")]
impl<T: Writer> StatsWriterLinux<T> {
    pub fn new(writer: T, max_udp_batch_size: u32, max_udp_packet_size: u16) -> Self {
        let max_udp_batch_size = max_udp_batch_size as usize;
        Self {
            max_udp_packet_size,
            writer,

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
impl<T: Writer> BorrowingStatsWriter for StatsWriterLinux<T> {
    fn metric_copied(&self) -> bool {
        false
    }

    unsafe fn write_borrowed(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: MetricKind,
    ) -> MetricResult<()> {
        let metric_type = metric_str(metric_type);

        // Manually build this line
        // format!("{}:{}|{}|#{}\n", metric, value, metric_type, tags);
        let metric_len = metric_len(metrics, tags, value, metric_type);

        // SAFETY: the scoped caller guarantees these bytes live until reset().
        // Erasing the lifetime lets the persistent backend reuse descriptor capacity.
        // These references never escape the backend or survive the session.
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

        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        #[allow(clippy::cast_possible_truncation)]
        if !self.current_transmit.enough_space_for(metric_len as u16) {
            self.queue_current_transmit();
        }

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
        // Clear every borrowed reference even if flush failed or was never called.
        // Keep descriptor allocations for the next session.
        self.tmp_mmsghdrs.clear();
        self.current_transmit.reset();
        while let Some(mut transmit) = self.queued_transmits.pop() {
            transmit.reset();
            self.pool_transmits.push(transmit);
        }
    }
}

// ============================================================================
// Apple-specific batch writer using sendmsg_x
// ============================================================================
#[cfg(target_vendor = "apple")]
pub struct StatsWriterApple<T> {
    max_udp_packet_size: u16,
    writer: T,

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
fn metric_len(metrics: &[&str], tags: &str, value: &str, metric_type: &str) -> usize {
    // format!("{}:{}|{}\n", metric, value, metric_type) when tags is empty
    // format!("{}:{}|{}|#{}\n", metric, value, metric_type, tags) when tags is not empty
    let mut metric_len = value.len() + metric_type.len() + tags.len() + 3; // ':' + '|' + '\n'

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
    pub fn new(writer: T, max_udp_batch_size: u32, max_udp_packet_size: u16) -> Self {
        let max_udp_batch_size = max_udp_batch_size as usize;
        Self {
            max_udp_packet_size,
            writer,
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

const fn metric_str(metric_type: MetricKind) -> &'static str {
    match metric_type {
        MetricKind::Count => "c",
        MetricKind::Gauge => "g",
        MetricKind::Timing => "ms",
    }
}

#[cfg(target_vendor = "apple")]
impl<T: Writer> BorrowingStatsWriter for StatsWriterApple<T> {
    fn metric_copied(&self) -> bool {
        false
    }

    unsafe fn write_borrowed<'data>(
        &mut self,
        metrics: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: MetricKind,
    ) -> MetricResult<()> {
        // SAFETY: the scoped caller guarantees these bytes live until reset().
        // Erasing the lifetime lets the persistent backend reuse descriptor capacity.
        // These references never escape the backend or survive the session.
        let (metrics, tags, value, metric_type) = unsafe {
            (
                transmute::<&[&str], &[&str]>(metrics),
                transmute::<&str, &str>(tags),
                transmute::<&str, &str>(value),
                transmute::<&str, &str>(metric_str(metric_type)),
            )
        };

        let metric_len = metric_len(metrics, tags, value, metric_type);

        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        #[allow(clippy::cast_possible_truncation)]
        if !self.current_transmit.enough_space_for(metric_len as u16) {
            self.queue_current_transmit();
        }

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
        // Clear every borrowed reference even if flush failed or was never called.
        // Keep descriptor allocations for the next session.
        self.tmp_mmsghdrs.clear();
        self.current_transmit.reset();
        while let Some(mut transmit) = self.queued_transmits.pop() {
            transmit.reset();
            self.pool_transmits.push(transmit);
        }
    }
}

pub struct StatsWriterSimple<T> {
    max_udp_packet_size: u16,
    writer: T,
    current_transmit: String,
}

impl<T: Writer> StatsWriterSimple<T> {
    pub fn new(writer: T, max_udp_packet_size: u16) -> Self {
        Self {
            max_udp_packet_size,
            writer,
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
        metric_type: MetricKind,
    ) -> MetricResult<()> {
        let metric_type = metric_str(metric_type);

        // Calculate the metric length
        let metric_len = metric_len(metrics, tags, value, metric_type);

        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        // If not enough space, queue current transmit
        if self.current_transmit.len() + metric_len > self.max_udp_packet_size as usize {
            self.flush_current_transmit()?;
        }

        // Build the metric string
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct MockWriter {
        written: RefCell<Vec<Vec<u8>>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                written: RefCell::new(Vec::new()),
            }
        }

        fn take_written(&self) -> Vec<Vec<u8>> {
            std::mem::take(&mut *self.written.borrow_mut())
        }
    }

    impl Writer for MockWriter {
        fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
            self.written.borrow_mut().push(buf.to_vec());
            Ok(buf.len())
        }

        #[cfg(target_os = "linux")]
        fn write_mvec(
            &self,
            _pool_msg_headers: &mut [rustix::net::MMsgHdr<'_>],
        ) -> MetricResult<usize> {
            Ok(0)
        }

        #[cfg(target_os = "linux")]
        fn get_destination(&self) -> &rustix::net::SocketAddrAny {
            unimplemented!()
        }

        #[cfg(target_vendor = "apple")]
        fn get_destination_addr(&self) -> libc::sockaddr_in {
            unimplemented!()
        }

        #[cfg(target_vendor = "apple")]
        fn as_raw_fd(&self) -> libc::c_int {
            unimplemented!()
        }
    }

    #[test]
    fn simple_writer_writes_metric_with_tags() {
        let mock = MockWriter::new();
        let mut writer = StatsWriterSimple::new(&mock, 1432);

        writer
            .write(&["my.metric"], "env:prod", "42", MetricKind::Count)
            .unwrap();
        let flushed = StatsWriterTrait::flush(&mut writer).unwrap();
        assert!(flushed > 0);

        let written = mock.take_written();
        assert_eq!(written.len(), 1);
        let line = String::from_utf8(written[0].clone()).unwrap();
        assert_eq!(line, "my.metric:42|c|#env:prod\n");
    }

    #[test]
    fn simple_writer_writes_metric_without_tags() {
        let mock = MockWriter::new();
        let mut writer = StatsWriterSimple::new(&mock, 1432);

        writer
            .write(&["my.metric"], "", "10", MetricKind::Gauge)
            .unwrap();
        StatsWriterTrait::flush(&mut writer).unwrap();

        let written = mock.take_written();
        let line = String::from_utf8(written[0].clone()).unwrap();
        assert_eq!(line, "my.metric:10|g\n");
    }

    #[test]
    fn simple_writer_timing_metric_type() {
        let mock = MockWriter::new();
        let mut writer = StatsWriterSimple::new(&mock, 1432);

        writer
            .write(&["dur"], "", "100", MetricKind::Timing)
            .unwrap();
        StatsWriterTrait::flush(&mut writer).unwrap();

        let written = mock.take_written();
        let line = String::from_utf8(written[0].clone()).unwrap();
        assert_eq!(line, "dur:100|ms\n");
    }

    #[test]
    fn simple_writer_rejects_oversized_metric() {
        let mock = MockWriter::new();
        let mut writer = StatsWriterSimple::new(&mock, 20);

        let result = writer.write(
            &["a.very.long.metric.name.that.exceeds.packet"],
            "tag:val",
            "999999",
            MetricKind::Count,
        );
        assert!(result.is_err());
    }

    #[test]
    fn simple_writer_flushes_when_full() {
        let mock = MockWriter::new();
        let mut writer = StatsWriterSimple::new(&mock, 40);

        writer
            .write(&["metric.a"], "", "1", MetricKind::Count)
            .unwrap();
        writer
            .write(&["metric.b"], "", "2", MetricKind::Count)
            .unwrap();
        StatsWriterTrait::flush(&mut writer).unwrap();

        let written = mock.take_written();
        assert!(!written.is_empty());
    }

    #[test]
    fn simple_writer_reset_clears_buffer() {
        let mock = MockWriter::new();
        let mut writer = StatsWriterSimple::new(&mock, 1432);

        writer.write(&["m"], "", "1", MetricKind::Count).unwrap();
        StatsWriterTrait::reset(&mut writer);
        let flushed = StatsWriterTrait::flush(&mut writer).unwrap();
        assert_eq!(flushed, 0);
    }

    #[test]
    fn metric_len_with_tags() {
        // "m:v|c|#t\n" = 1+1+1+1+1+1+2+1+1 = metric(1) + ':' + value(1) + '|' + type(1) + '|#' + tags(1) + '\n'
        let len = metric_len(&["m"], "t", "v", "c");
        assert_eq!(len, "m:v|c|#t\n".len());
    }

    #[test]
    fn metric_len_without_tags() {
        let len = metric_len(&["m"], "", "v", "c");
        assert_eq!(len, "m:v|c\n".len());
    }

    #[test]
    fn metric_str_maps_kinds() {
        assert_eq!(metric_str(MetricKind::Count), "c");
        assert_eq!(metric_str(MetricKind::Gauge), "g");
        assert_eq!(metric_str(MetricKind::Timing), "ms");
    }

    #[test]
    fn stats_guard_delegates_and_resets_on_drop() {
        let mut simple = StatsWriterSimple::new(MockWriter::new(), 1432);

        {
            let mut guard = StatsGuard::new(&mut simple);
            assert!(guard.metric_copied());
            guard.write(&["m"], "", "1", MetricKind::Count).unwrap();
            guard.flush().unwrap();
        } // guard drops here, calling reset

        // After drop, internal buffer should be cleared
        let flushed = StatsWriterTrait::flush(&mut simple).unwrap();
        assert_eq!(flushed, 0);
    }

    #[cfg(target_vendor = "apple")]
    type PlatformBatch<T> = StatsWriterApple<T>;
    #[cfg(target_os = "linux")]
    type PlatformBatch<T> = StatsWriterLinux<T>;

    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn batch_session_clears_references_on_error_and_unwind() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let mut batch = PlatformBatch::new(MockWriter::new(), 32, 16);
        for unwind in [false, true] {
            {
                let names = ["first".to_owned(), "other".to_owned()];
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let mut session = StatsGuard::new(&mut batch);
                    session
                        .write(&[&names[0]], "", "1", MetricKind::Count)
                        .unwrap();
                    session
                        .write(&[&names[1]], "", "2", MetricKind::Count)
                        .unwrap();
                    assert!(session
                        .write(&["too_long_for_this_packet"], "", "1", MetricKind::Count)
                        .is_err());
                    assert!(!unwind, "simulated caller failure");
                    // Deliberately leave current and queued packets unflushed.
                }));
                assert_eq!(result.is_err(), unwind);
            } // Input strings are freed before the persistent backend is reused.
            assert_eq!(batch.current_transmit.len(), 0);
            assert!(batch.current_transmit.get_iovecs().is_empty());
            assert!(batch.queued_transmits.is_empty());
            assert!(batch.tmp_mmsghdrs.is_empty());
            assert_eq!(batch.pool_transmits.len(), 1);
            assert!(batch
                .pool_transmits
                .iter()
                .all(|t| t.get_iovecs().is_empty()));
        }
    }

    #[cfg(all(
        feature = "allocationcounter",
        any(target_vendor = "apple", target_os = "linux")
    ))]
    #[test]
    fn batch_session_borrows_strings_without_allocating() {
        let mut batch = PlatformBatch::new(MockWriter::new(), 32, 1432);
        let names = ["first".to_owned(), "other".to_owned()];
        let tags = "env:owned".to_owned();
        let allocation = batch.current_transmit.get_iovecs().as_ptr();
        let measured = allocation_counter::measure(|| {
            for _ in 0..100 {
                let mut session = StatsGuard::new(&mut batch);
                for name in &names {
                    session
                        .write(&[name], &tags, "1", MetricKind::Count)
                        .unwrap();
                }
            }
        });
        assert_eq!(measured.count_total, 0);
        assert_eq!(batch.current_transmit.get_iovecs().as_ptr(), allocation);
    }

    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    #[cfg_attr(miri, ignore)] // Miri does not implement UDP batch syscalls.
    fn scoped_writers_send_distinct_owned_names_and_reuse_after_flush() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let destination_addr = receiver.local_addr().unwrap();
        #[cfg(target_vendor = "apple")]
        let platform = StatsWriterType::AppleBatch;
        #[cfg(target_os = "linux")]
        let platform = StatsWriterType::LinuxBatch;

        for kind in [StatsWriterType::Simple, platform] {
            let socket = UdpSocketWriter {
                sock: UdpSocket::bind("127.0.0.1:0").unwrap(),
                destination_addr,
                #[cfg(target_os = "linux")]
                destination: destination_addr.into(),
            };
            let mut holder = StatsWriterHolder::new(socket, kind, 1432, 32);
            for _ in 0..2 {
                let names = ["first".to_owned(), "other".to_owned()];
                let values = ["1".to_owned(), "2".to_owned()];
                let tags = "env:owned".to_owned();
                let mut session = holder.acquire();
                for (name, value) in names.iter().zip(&values) {
                    // The temporary parts array dies immediately after write.
                    session
                        .write(&[name], &tags, value, MetricKind::Count)
                        .unwrap();
                } // Cursor is gone; the owner keeps names, values and tags alive.
                session.flush().unwrap();
                drop(session);
                drop((names, values, tags));
                let mut packet = [0; 256];
                let len = receiver.recv(&mut packet).unwrap();
                assert_eq!(
                    &packet[..len],
                    b"first:1|c|#env:owned\nother:2|c|#env:owned\n"
                );
            }
        }
    }
}
