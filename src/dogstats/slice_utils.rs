// This method is polemic, but in some bench show better perf than using b1 == b2,
// In near future maybe will be removed, for now I will keep it
pub fn equal_slice(b1: &[u8], b2: &[u8]) -> bool {
    if b1.len() != b2.len() {
        return false;
    }

    let mut offset = 0usize;
    let len = b1.len();

    // SAFETY: bounds are checked before each read; `read_unaligned` permits
    // unaligned pointers and reads plain integers by value.
    unsafe {
        while offset + std::mem::size_of::<u64>() <= len {
            let left = std::ptr::read_unaligned(b1.as_ptr().add(offset).cast::<u64>());
            let right = std::ptr::read_unaligned(b2.as_ptr().add(offset).cast::<u64>());
            if left != right {
                return false;
            }
            offset += std::mem::size_of::<u64>();
        }

        if offset + std::mem::size_of::<u32>() <= len {
            let left = std::ptr::read_unaligned(b1.as_ptr().add(offset).cast::<u32>());
            let right = std::ptr::read_unaligned(b2.as_ptr().add(offset).cast::<u32>());
            if left != right {
                return false;
            }
            offset += std::mem::size_of::<u32>();
        }

        if offset + std::mem::size_of::<u16>() <= len {
            let left = std::ptr::read_unaligned(b1.as_ptr().add(offset).cast::<u16>());
            let right = std::ptr::read_unaligned(b2.as_ptr().add(offset).cast::<u16>());
            if left != right {
                return false;
            }
            offset += std::mem::size_of::<u16>();
        }
    }

    if offset < len {
        return b1[offset] == b2[offset];
    }

    true
}
