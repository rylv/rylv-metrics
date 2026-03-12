.PHONY: clean
clean:
	cargo clean

.PHONY: clippy
clippy:
	@echo "=> Executing cargo clippy"
	@cargo clippy --all-features --color auto --all-targets -- -D warnings

.PHONY: fmt
fmt:
	@echo "=> Formatting code"
	@cargo fmt --all

.PHONY: test
test:
	@echo "=> Running tests"
	@cargo test

.PHONY: prepare-commit
prepare-commit:
	@echo "=> Preparing commit"
	@cargo fmt --all
	@cargo clippy --all-targets --all-features -- -D warnings
	@cargo test --all-features
	@git add -A
	@MSG_FINAL="$(MSG)"; \
	if [ -z "$$MSG_FINAL" ]; then \
		FILES=$$(git diff --cached --name-only | head -n 3 | paste -sd ", " -); \
		if [ -n "$$FILES" ]; then \
			MSG_FINAL="chore: update $$FILES"; \
		else \
			MSG_FINAL="chore: update code"; \
		fi; \
	fi; \
	echo "=> Commit message: $$MSG_FINAL"; \
	git commit -m "$$MSG_FINAL"

.PHONY: release
release:
	RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" cargo build --release

.PHONY: release-static
release-static:
	RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" cargo build --release --target=aarch64-unknown-linux-musl

.PHONY: release-zig
release-zig:
	cargo zigbuild --release --target=aarch64-unknown-linux-gnu
	cargo zigbuild --release --target=x86_64-unknown-linux-gnu

.PHONY: build
build:
	RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" cargo build

.PHONY: docker-build
docker-build:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 cargo build --all-features

.PHONY: docker-check
docker-check:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 cargo check --all-features

.PHONY: docker-test
docker-test:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 cargo test --all-features

.PHONY: coverage
coverage:
	cargo llvm-cov --workspace --html --ignore-filename-regex 'bins/'
	@echo "Coverage report generated at target/llvm-cov/html/index.html"

.PHONY: coverage-open
coverage-open:
	cargo llvm-cov --workspace --open --ignore-filename-regex 'bins/'

.PHONY: coverage-all
coverage-all:
	@echo "Running coverage with default features..."
	cargo llvm-cov --workspace --html --ignore-filename-regex 'bins/'
	@echo "Running coverage with no-default-features..."
	cargo llvm-cov --no-default-features --workspace --html --ignore-filename-regex 'bins/'
	@echo "All coverage reports generated at target/llvm-cov/html/index.html"

.PHONY: docker-coverage
docker-coverage:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 sh -c "cargo install cargo-llvm-cov && cargo llvm-cov --workspace --html --ignore-filename-regex 'bins/'"
	@echo "Coverage report generated at target-docker/llvm-cov/html/index.html"

.PHONY: bench
bench:
	RUSTFLAGS="-C force-frame-pointers=yes" cargo bench --bench sync_collector

.PHONY: bench-shared
bench-shared:
	RUSTFLAGS="-C force-frame-pointers=yes" cargo bench --bench collector_compare --features "tls-collector shared-collector"

.PHONY: bench-dhat
bench-dhat:
	RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" cargo bench --bench sync_collector --features dhat-heap
	dhat-to-flamegraph dhat-heap.json > dhat.svg
	firefox dhat.svg

.PHONY: bench-flamegraph
bench-flamegraph:
	RUSTFLAGS="-C force-frame-pointers=yes" cargo flamegraph --bench sync_collector -- --bench
	firefox flamegraph.svg

.PHONY: bench-samply
bench-samply:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp"

# Sorted/Prepared profiling targets (Criterion filters)
.PHONY: bench-samply-single-regular
bench-samply-single-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_compare/count_add_regular_tags"

.PHONY: bench-samply-single-sorted
bench-samply-single-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_compare/count_add_sorted_tags"

.PHONY: bench-samply-single-prepared
bench-samply-single-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_compare/count_add_prepared_metric"

.PHONY: bench-samply-parallel-udp-regular
bench-samply-parallel-udp-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_parallel_compare/udp_regular_parallel"

.PHONY: bench-samply-parallel-udp-sorted
bench-samply-parallel-udp-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_parallel_compare/udp_sorted_parallel"

.PHONY: bench-samply-parallel-udp-prepared
bench-samply-parallel-udp-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_parallel_compare/udp_prepared_parallel"

.PHONY: bench-samply-parallel-tls-regular
bench-samply-parallel-tls-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_parallel_compare/tls_regular_parallel"

.PHONY: bench-samply-parallel-tls-sorted
bench-samply-parallel-tls-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_parallel_compare/tls_sorted_parallel"

.PHONY: bench-samply-parallel-tls-prepared
bench-samply-parallel-tls-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "sorted_tags_parallel_compare/tls_prepared_parallel"

.PHONY: bench-samply-hist-single-regular
bench-samply-hist-single-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_compare/histogram_regular_tags"

.PHONY: bench-samply-hist-single-sorted
bench-samply-hist-single-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_compare/histogram_sorted_tags"

.PHONY: bench-samply-hist-single-prepared
bench-samply-hist-single-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_compare/histogram_prepared_metric"

.PHONY: bench-samply-hist-parallel-udp-regular
bench-samply-hist-parallel-udp-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_parallel_compare/udp_regular_parallel"

.PHONY: bench-samply-hist-parallel-udp-sorted
bench-samply-hist-parallel-udp-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_parallel_compare/udp_sorted_parallel"

.PHONY: bench-samply-hist-parallel-udp-prepared
bench-samply-hist-parallel-udp-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_parallel_compare/udp_prepared_parallel"

.PHONY: bench-samply-hist-parallel-tls-regular
bench-samply-hist-parallel-tls-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_parallel_compare/tls_regular_parallel"

.PHONY: bench-samply-hist-parallel-tls-sorted
bench-samply-hist-parallel-tls-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_parallel_compare/tls_sorted_parallel"

.PHONY: bench-samply-hist-parallel-tls-prepared
bench-samply-hist-parallel-tls-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench thread_local_compare --features "udp-tls custom_writer" "histogram_sorted_parallel_compare/tls_prepared_parallel"

.PHONY: bench-samply-sync-hist-single-regular
bench-samply-sync-hist-single-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_single_compare/regular_tags"

.PHONY: bench-samply-sync-hist-single-sorted
bench-samply-sync-hist-single-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_single_compare/sorted_tags"

.PHONY: bench-samply-sync-hist-single-prepared
bench-samply-sync-hist-single-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_single_compare/prepared_metric"

.PHONY: bench-samply-sync-hist-parallel-udp-regular
bench-samply-sync-hist-parallel-udp-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_parallel_udp_compare/regular_parallel"

.PHONY: bench-samply-sync-hist-parallel-udp-sorted
bench-samply-sync-hist-parallel-udp-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_parallel_udp_compare/sorted_parallel"

.PHONY: bench-samply-sync-hist-parallel-udp-prepared
bench-samply-sync-hist-parallel-udp-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_parallel_udp_compare/prepared_parallel"

.PHONY: bench-samply-sync-hist-parallel-tls-regular
bench-samply-sync-hist-parallel-tls-regular:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_parallel_tls_compare/regular_parallel"

.PHONY: bench-samply-sync-hist-parallel-tls-sorted
bench-samply-sync-hist-parallel-tls-sorted:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_parallel_tls_compare/sorted_parallel"

.PHONY: bench-samply-sync-hist-parallel-tls-prepared
bench-samply-sync-hist-parallel-tls-prepared:
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector --features "udp-tls" "sync_histogram_parallel_tls_compare/prepared_parallel"

.PHONY: bench-heaptrack
bench-heaptrack:
	RUSTFLAGS="-C force-frame-pointers=yes" heaptrack ./target/release/deps/sync_collector-d96b58080677933f --bench

.PHONY: main-dhat-single
main-dhat-single:
	RUST_BACKTRACE=1 cargo run --release --bin shared_dhat_single --features "shared-collector dhat-heap"
	dhat-to-flamegraph dhat-heap.json > dhat.svg
	firefox dhat.svg || open dhat.svg

.PHONY: main-dhat-multi
main-dhat-multi:
	RUST_BACKTRACE=1 cargo run --release --bin shared_dhat_multi --features "shared-collector dhat-heap"
	dhat-to-flamegraph dhat-heap.json > dhat.svg
	firefox dhat.svg || open dhat.svg

.PHONY: main-flamegraph
main-flamegraph:
	RUSTFLAGS="-C force-frame-pointers=yes" cargo flamegraph --features allocationcounter
	firefox flamegraph.svg

.PHONY: fuzz-list
fuzz-list:
	cargo +nightly fuzz list

.PHONY: fuzz-collector
fuzz-collector:
	cargo +nightly fuzz run fuzz_metric_collector -- -max_total_time=60

.PHONY: fuzz-names
fuzz-names:
	cargo +nightly fuzz run fuzz_metric_names -- -max_total_time=60

.PHONY: fuzz-tags
fuzz-tags:
	cargo +nightly fuzz run fuzz_tags -- -max_total_time=60

.PHONY: fuzz-limits
fuzz-limits:
	cargo +nightly fuzz run fuzz_packet_limits -- -max_total_time=60

.PHONY: fuzz-numbers
fuzz-numbers:
	cargo +nightly fuzz run fuzz_numeric_values -- -max_total_time=60

.PHONY: fuzz-all
fuzz-all:
	@echo "Running all fuzz targets for 60 seconds each..."
	cargo +nightly fuzz run fuzz_metric_collector -- -max_total_time=60
	cargo +nightly fuzz run fuzz_metric_names -- -max_total_time=60
	cargo +nightly fuzz run fuzz_tags -- -max_total_time=60
	cargo +nightly fuzz run fuzz_packet_limits -- -max_total_time=60
	cargo +nightly fuzz run fuzz_numeric_values -- -max_total_time=60
	@echo "All fuzz targets completed"

.PHONY: fuzz-cmin
fuzz-cmin:
	@echo "Minimizing corpus for all fuzz targets..."
	cargo +nightly fuzz cmin fuzz_metric_collector
	cargo +nightly fuzz cmin fuzz_metric_names
	cargo +nightly fuzz cmin fuzz_tags
	cargo +nightly fuzz cmin fuzz_packet_limits
	cargo +nightly fuzz cmin fuzz_numeric_values

.PHONY: fuzz-build
fuzz-build:
	@echo "Building all fuzz targets..."
	cargo +nightly fuzz build fuzz_metric_collector
	cargo +nightly fuzz build fuzz_metric_names
	cargo +nightly fuzz build fuzz_tags
	cargo +nightly fuzz build fuzz_packet_limits
	cargo +nightly fuzz build fuzz_numeric_values
	@echo "All fuzz targets built successfully"

.PHONY: miri-setup
miri-setup:
	@echo "Installing Miri (requires nightly toolchain)..."
	rustup toolchain install nightly --component miri
	@echo "Miri installed successfully"

.PHONY: miri
miri:
	@echo "=> Running Miri memory safety checks"
	@echo "Note: Only tests that don't require network I/O can run under Miri"
	MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check" cargo +nightly miri test --test miri_test

.PHONY: miri-verbose
miri-verbose:
	@echo "=> Running Miri with verbose output"
	MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check" cargo +nightly miri test --test miri_test -- --nocapture

.PHONY: miri-all
miri-all:
	@echo "=> Attempting to run all tests under Miri (will fail on network I/O)"
	@echo "This is expected - use 'make miri' for working tests only"
	MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check" cargo +nightly miri test --lib --tests || true

.PHONY: prepare-publish
prepare-publish:
	@echo "=> Preparing for cargo publish"
	@$(MAKE) prepare-commit
	@echo "=> Checking package contents"
	@cargo package --list
	@echo "=> Building docs with warnings as errors"
	@RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
	@echo "=> Running cargo publish dry-run"
	@cargo publish --dry-run
