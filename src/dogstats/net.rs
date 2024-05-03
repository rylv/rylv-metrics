// Apple FFI declarations for sendmmsg_x
#[cfg(target_vendor = "apple")]
#[repr(C)]
#[allow(clippy::struct_field_names)]
pub struct msghdr_x {
    pub msg_name: *mut libc::c_void,
    pub msg_namelen: libc::socklen_t,
    pub msg_iov: *mut libc::iovec,
    pub msg_iovlen: libc::c_int,
    pub msg_control: *mut libc::c_void,
    pub msg_controllen: libc::socklen_t,
    pub msg_flags: libc::c_int,
    pub msg_datalen: libc::size_t,
}

#[cfg(target_vendor = "apple")]
extern "C" {
    pub fn sendmsg_x(
        s: libc::c_int,
        msgp: *const msghdr_x,
        cnt: libc::c_uint,
        flags: libc::c_int,
    ) -> libc::ssize_t;
}
