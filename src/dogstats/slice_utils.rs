// This method is polemic, but in some bench show better perf than using b1 == b2,
// In near future maybe will be removed, for now I will keep it
pub const fn equal_slice(b1: &[u8], b2: &[u8]) -> bool {
    if b1.len() != b2.len() {
        return false;
    }

    let mut offset = 0usize;
    let len = b1.len();

    // SAFETY: offset starts at zero and only advances after a full chunk is
    // checked. Each read is within both equally sized slices. Their bytes are
    // initialized, every integer bit pattern is valid, and `read_unaligned`
    // permits an unaligned address.
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

#[cfg(kani)]
mod verification {
    use super::equal_slice;

    // This is the index arithmetic of equal_slice, with `remaining` tracking
    // bytes not yet consumed. A loop contract proves it for every slice length
    // allowed by Rust, without choosing a maximum number of u64 chunks.
    #[kani::proof]
    fn offsets_cover_any_valid_length() {
        let len: usize = kani::any();
        kani::assume(len <= isize::MAX as usize);
        let mut offset = 0usize;
        let mut remaining = len;

        #[kani::loop_invariant(offset <= len && remaining <= len && offset + remaining == len && offset % 8 == 0)]
        #[kani::loop_decreases(remaining)]
        while offset + 8 <= len {
            offset += 8;
            remaining -= 8;
        }

        if offset + 4 <= len {
            offset += 4;
            remaining -= 4;
        }
        if offset + 2 <= len {
            offset += 2;
            remaining -= 2;
        }
        if offset < len {
            offset += 1;
            remaining -= 1;
        }

        assert_eq!(offset, len);
        assert_eq!(remaining, 0);
    }

    // Integer equality must agree with equality of every byte in that chunk,
    // regardless of the machine's byte order.
    #[kani::proof]
    fn integer_chunks_match_byte_equality() {
        let left: [u8; 8] = kani::any();
        let right: [u8; 8] = kani::any();
        unsafe {
            let left_64 = std::ptr::read_unaligned(left.as_ptr().cast::<u64>());
            let right_64 = std::ptr::read_unaligned(right.as_ptr().cast::<u64>());
            assert_eq!(left_64 == right_64, left == right);

            let left_32 = std::ptr::read_unaligned(left.as_ptr().cast::<u32>());
            let right_32 = std::ptr::read_unaligned(right.as_ptr().cast::<u32>());
            assert_eq!(left_32 == right_32, left[..4] == right[..4]);

            let left_16 = std::ptr::read_unaligned(left.as_ptr().cast::<u16>());
            let right_16 = std::ptr::read_unaligned(right.as_ptr().cast::<u16>());
            assert_eq!(left_16 == right_16, left[..2] == right[..2]);
        }
    }

    // The symbolic arrays cover every byte value, length, and starting offset
    // in these bounds. Kani also checks the pointer reads for undefined behavior.
    #[kani::proof]
    #[kani::unwind(44)]
    fn equal_slice_matches_standard_equality() {
        const MAX_LEN: usize = 33;
        const MAX_OFFSET: usize = 7;
        let left: [u8; MAX_LEN + MAX_OFFSET] = kani::any();
        let right: [u8; MAX_LEN + MAX_OFFSET] = kani::any();
        let left_len: usize = kani::any();
        let right_len: usize = kani::any();
        let left_offset: usize = kani::any();
        let right_offset: usize = kani::any();

        kani::assume(left_len <= MAX_LEN && right_len <= MAX_LEN);
        kani::assume(left_offset <= MAX_OFFSET && right_offset <= MAX_OFFSET);

        let left = &left[left_offset..left_offset + left_len];
        let right = &right[right_offset..right_offset + right_len];
        assert_eq!(equal_slice(left, right), left == right);
    }
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

    #[test]
    fn matches_slice_equality_across_lengths_offsets_and_mismatch_positions() {
        for len in 0..=33 {
            let bytes: Vec<u8> = (0..len + 8)
                .map(|i| {
                    u8::try_from(i)
                        .expect("test input shorter than 256 bytes")
                        .wrapping_mul(37)
                })
                .collect();

            for left_offset in 0..8 {
                for right_offset in 0..8 {
                    let left = &bytes[left_offset..left_offset + len];
                    let mut right_storage = vec![0; right_offset + len + 1];
                    right_storage[right_offset..right_offset + len].copy_from_slice(left);

                    let right = &mut right_storage[right_offset..right_offset + len];
                    assert_eq!(equal_slice(left, right), left == right);

                    for mismatch in 0..len {
                        right[mismatch] ^= 0x80;
                        assert_eq!(equal_slice(left, right), left == right,
                            "len={len}, offsets=({left_offset}, {right_offset}), mismatch={mismatch}");
                        right[mismatch] ^= 0x80;
                    }

                    let longer = &right_storage[right_offset..=right_offset + len];
                    assert_eq!(equal_slice(left, longer), left == longer);
                }
            }
        }
    }
}
