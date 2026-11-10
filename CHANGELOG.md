# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-11-10

### Added
- `RylvTag::from_parts` and `(key, value)` tuple conversion for compound tags.
- `MetricValue` for drained frames, with integer and floating-point variants.

### Changed
- Breaking: counter and gauge values now use `i64` instead of `u64`. Counters
  accept negative deltas and saturate at the `i64` limits; averaged gauges emit
  their real mean instead of an integer.
- Breaking: `MetricFrameRef::value` now has type `MetricValue` instead of `u64`,
  and `MetricKind` distinguishes averaged from last-write-wins gauges.
- Breaking: histogram and timing recording methods now return `MetricResult<()>`
  when a value is rejected by the configured bounds. Histogram upper bounds
  cannot exceed `i64::MAX`.
- Pin Cargo builds and CI to Rust 1.88.0, the declared MSRV.

### Fixed
- Rejected TLS histogram samples return their wrappers to the recycling pool.
- Float metric values use plain decimal formatting with up to six fractional
  digits; collector test helpers reuse the same formatter as the UDP writer.

## [0.4.0] - 2026-09-28

### Added
- `RylvTag` type for tag values
- `from_static` constructor for tags
- Last-write-wins gauge method
- `inline` and `cold` annotations on hot paths
- More unit tests across collectors

### Changed
- Renamed `gauge` to `gauge_avg`
- Raised Minimum Supported Rust Version (MSRV) to 1.88
- `MetricDrain::next_frame()` was replaced by `frames()` and an associated
  `Cursor<'a>: Iterator<Item = MetricFrameRef<'a>>`. Frames borrow the drained
  owner and can survive cursor destruction; custom drains must keep their
  backing data stable for that borrow.
- Shared and TLS drains own aggregators directly, removing the self-referential
  heap allocation. Entry removal and recycling happen when the owner is dropped.
- Dependency updates (see `Cargo.lock`)

### Fixed
- `tls_collector` tests now use `RylvTag` instead of `RylvStr` for tag arguments
- Batch UDP writers retain frame and formatting borrows until their scoped session
  ends, and clear pending references on errors or early exit before storage is reused.
- Recycled TLS timing histograms now observe the same pool cap as histograms.

### Internal
- Cleanup across collector and aggregator modules

## [0.3.2] - 2026-05-16

### Changed
- Removed `TLSCollectorOptions::max_recycled_global_histograms_per_pool`; the recycled global histogram pool limit is now an internal constant.

## [0.3.1] - 2026-04-14

### Fixed
- `TLSCollector` count reset during drain

## [0.3.0] - 2026-03-12

### Added
- `TLSCollector` — thread-local storage collector using `hashbrown::HashTable` and `parking_lot` for high-concurrency scenarios
- `SharedCollector` drain API with `MetricFrameRef` borrowed iteration via `DrainMetricCollectorTrait`
- `PreparedMetric` for pre-computed metric keys on hot paths
- `SortedTags` for reusable pre-sorted tag sets that skip per-call sorting
- `MetricCollector` now generic over any `DrainMetricCollectorTrait` inner collector
- Batch UDP writers: `LinuxBatch` (sendmmsg) and `AppleBatch` (sendmsg_x)
- Convenience macros: `histogram!`, `count!`, `count_add!`, `gauge_avg!`
- `HistogramConfig::with_count()`, `with_min()`, `with_avg()`, `with_max()` base metric toggles
- `Bounds` for inclusive min/max histogram value clamping
- Miri tests for `SharedCollector` and `TLSCollector` drain lifetime safety
- Integration tests for failure scenarios: unreachable destination, mid-stream socket close, cardinality explosion, graceful shutdown under load
- Automated release workflow (`.github/workflows/release.yml`) with tag-driven publish to crates.io and GitHub Releases
- Benchmark regression detection in CI for pull requests (critcmp with 15% threshold)
- `cargo check --all-features --all-targets` and `cargo test --all-features` CI steps
- New benchmarks: `collector_compare`, `thread_local_compare`, `sync_collector`
- New examples: `prepared_metric_shared`, `shared_basic`, `shared_runtime_retry`, `sorted_tags`, `sorted_tags_tls`, `sorted_tags_udp`

### Changed
- Reduced public API surface — cleaner re-exports from `lib.rs`
- `StatsWriterTrait::write()` now takes `MetricKind` instead of `&str` for metric type
- Miri CI now runs with `shared-collector` and `tls-collector` features enabled
- SAFETY comments expanded for all `unsafe` blocks in drain and writer paths

### Internal
- Reorganized aggregator module structure (`aggregator/mod.rs`, `aggregator/shared.rs`)
- Collector module split into `shared_collector.rs` and `tls_collector.rs`

## [0.2.1] - 2026-02-15

### Changed
- Optimized histogram percentile metric suffix generation with a per-flush cache in `job.rs`
- Added explicit cache drop before bump reset to make arena-lifetime intent clear and safer for future refactors

### Fixed
- Improved stability and predictability of internal percentile suffix handling during flush cycles

## [0.2.0] - 2026-02-14

### Added
- Generic histogram percentile configuration via `HistogramConfig`
- Histogram base metric toggles (`count`, `min`, `avg`, `max`)
- Dedicated compare benchmark (`benches/lookup_compare.rs`) for key-lookup hot paths
- Miri coverage for custom writer path (`tests/miri_test.rs`)
- `make prepare-commit` workflow (fmt + clippy + test + commit with auto message)
- `make prepare-publish` workflow (runs `prepare-commit`, package/doc checks, publish dry-run)

### Changed
- Default hasher strategy standardized to `std::hash::RandomState`
- Lookup key comparison path optimized with safe fail-fast checks (including joined tag length)
- Integration tests now use ephemeral Datadog destination ports to avoid cross-test interference

### Fixed
- Removed alignment-sensitive compare behavior that could panic on misaligned pointers
- Stabilized custom writer histogram integration tests by using deterministic flush/shutdown behavior
- Updated fuzz targets to current collector API

### Internal
- Refined benchmark/test setup and formatting/clippy hygiene for CI consistency

## [0.1.0] - 2026

### Added

- Initial release
- `MetricCollector` for collecting and aggregating metrics
- Support for histogram, counter, and gauge metric types
- `histogram!` macro for convenient histogram recording
- Multiple writer backends:
  - `Simple`: Standard UDP writer
  - `LinuxBatch`: Batch UDP using `sendmmsg` (Linux)
  - `AppleBatch`: Batch UDP using `sendmsg_x` (macOS)
  - `Custom`: User-provided writer implementation
- Client-side aggregation to reduce network overhead
- Configurable histogram significant figures
- Flexible tag support with static and owned strings
- Cross-platform support (Linux, macOS)
