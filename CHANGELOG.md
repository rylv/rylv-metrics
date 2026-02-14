# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
