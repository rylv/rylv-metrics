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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_empty_slices() {
        assert!(equal_slice(b"", b""));
    }

    #[test]
    fn different_lengths() {
        assert!(!equal_slice(b"abc", b"abcd"));
    }

    #[test]
    fn equal_short() {
        assert!(equal_slice(b"abc", b"abc"));
    }

    #[test]
    fn differ_in_u32_range() {
        // 9 bytes: 8 bytes match (u64), then differ in u32-sized tail
        let a = b"12345678abcd";
        let mut b = *b"12345678abcd";
        b[9] = b'X';
        assert!(!equal_slice(a, &b));
    }

    #[test]
    fn differ_in_u16_range() {
        // 14 bytes: 8 (u64) + 4 (u32) + 2 differ in u16
        let a = b"12345678abcdXY";
        let mut b = *b"12345678abcdXY";
        b[13] = b'Z';
        assert!(!equal_slice(a, &b));
    }

    #[test]
    fn differ_in_last_byte() {
        // 15 bytes: 8 + 4 + 2 + 1 trailing byte
        let a = b"12345678abcdXYz";
        let mut b = *b"12345678abcdXYz";
        b[14] = b'!';
        assert!(!equal_slice(a, &b));
    }

    #[test]
    fn equal_large() {
        let a = b"12345678abcdXYz";
        assert!(equal_slice(a, a));
    }
}
