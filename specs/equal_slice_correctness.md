# `equal_slice` correctness

Run `make kani-check` to verify three Kani harnesses in `src/dogstats/slice_utils.rs`:

- `equal_slice_matches_standard_equality` checks the actual function for every pair of byte arrays, lengths from 0 through 33, and starting offsets from 0 through 7. Kani also checks the raw pointer reads on these inputs.
- `integer_chunks_match_byte_equality` checks that unaligned `u64`, `u32`, and `u16` comparisons agree with byte equality for arbitrary contents of each chunk.
- `offsets_cover_any_valid_length` uses a loop invariant and decreasing remainder to check the same 8/4/2/1 offset arithmetic for every length up to `isize::MAX`, without unwinding a fixed number of iterations.

The general correctness argument combines these facts. Unequal lengths return `false`. For equal lengths, a read occurs only when its full chunk lies inside both slices. Every `u8` is initialized, and every bit pattern is valid for the integer types being read. `read_unaligned` removes the alignment requirement. If a chunk differs, at least one of its bytes differs, so returning `false` is correct. Otherwise the offset advances past an equal chunk. The checked arithmetic covers every byte exactly once, including the final 4/2/1-byte tail, so reaching the final `true` means the slices are equal. The loop terminates because each iteration consumes eight bytes from a finite remainder.

Kani's proof over the actual function is bounded to 33 bytes. The unbounded offset proof and the chunk proof are separate harnesses; their composition into the claim for the actual function at arbitrary lengths is the argument above, rather than a single machine-checked function contract.
