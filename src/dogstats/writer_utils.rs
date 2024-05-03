use std::io::IoSlice;

pub struct Transmit<'data> {
    parts: Vec<IoSlice<'data>>,

    #[cfg(target_os = "linux")]
    ancilliary: rustix::net::SendAncillaryBuffer<'data, 'data, 'data>,

    len: u16,
    max_udp_package_size: u16,
}

impl<'data> Transmit<'data> {
    pub fn new(max_udp_package_size: u16) -> Self {
        // TODO: create with some smart size
        Self {
            parts: Vec::with_capacity(1000),
            #[cfg(target_os = "linux")]
            ancilliary: rustix::net::SendAncillaryBuffer::default(),
            len: 0,
            max_udp_package_size,
        }
    }

    pub const fn enough_space_for(&self, space: u16) -> bool {
        self.len + space <= self.max_udp_package_size
    }

    pub fn push(&mut self, part: IoSlice<'data>) {
        #[allow(clippy::cast_possible_truncation)]
        {
            self.len += part.len() as u16;
        }
        self.parts.push(part);
    }

    #[cfg(target_os = "linux")]
    pub fn create_mmsghdr<'s, 'c: 's>(
        &'s mut self,
        dst_addr: &'c rustix::net::SocketAddrAny,
    ) -> rustix::net::MMsgHdr<'s> {
        rustix::net::MMsgHdr::new_with_addr(dst_addr, self.parts.as_slice(), &mut self.ancilliary)
    }

    pub const fn len(&self) -> u16 {
        self.len
    }

    #[cfg(target_vendor = "apple")]
    pub fn get_iovecs(&self) -> &[std::io::IoSlice<'data>] {
        &self.parts
    }

    pub fn reset(&mut self) {
        self.parts.clear();
        self.len = 0;
    }
}
