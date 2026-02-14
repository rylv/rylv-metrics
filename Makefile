.PHONY: clean
clean:
	cargo clean

.PHONY: clippy
clippy:
	@echo "=> Executing cargo clippy"
	@cargo clippy --color auto --all-targets -- -D warnings

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
	@cargo test
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
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 cargo build

.PHONY: docker-check
docker-check:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 cargo check

.PHONY: docker-test
docker-test:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 cargo test

.PHONY: coverage
coverage:
	cargo llvm-cov --workspace --html
	@echo "Coverage report generated at target/llvm-cov/html/index.html"

.PHONY: coverage-open
coverage-open:
	cargo llvm-cov --workspace --open

.PHONY: coverage-all
coverage-all:
	@echo "Running coverage with default features..."
	cargo llvm-cov --workspace --html
	@echo "Running coverage with no-default-features..."
	cargo llvm-cov --no-default-features --workspace --html
	@echo "All coverage reports generated at target/llvm-cov/html/index.html"

.PHONY: docker-coverage
docker-coverage:
	docker run --rm -v $(PWD):/app -v $(HOME)/.cargo/registry:/usr/local/cargo/registry -v $(HOME)/.cargo/git:/usr/local/cargo/git -v $(PWD)/target-docker:/app/target -e CARGO_TARGET_DIR=/app/target -w /app rust:1.91.0 sh -c "cargo install cargo-llvm-cov && cargo llvm-cov --workspace --html"
	@echo "Coverage report generated at target-docker/llvm-cov/html/index.html"

.PHONY: bench
bench:
	RUSTFLAGS="-C force-frame-pointers=yes" cargo bench --bench sync_collector

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
	RUSTFLAGS="-C force-frame-pointers=yes" samply record cargo bench --bench sync_collector

.PHONY: bench-heaptrack
bench-heaptrack:
	RUSTFLAGS="-C force-frame-pointers=yes" heaptrack ./target/release/deps/sync_collector-d96b58080677933f --bench

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
