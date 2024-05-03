# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
