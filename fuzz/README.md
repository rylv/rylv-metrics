# Fuzzing for rylv-metrics

This directory contains fuzz targets for testing the rylv-metrics library using cargo-fuzz.

## Requirements

Fuzzing requires nightly Rust:

```bash
rustup install nightly
```

## Fuzz Targets

The following fuzz targets are available:

1. **fuzz_metric_collector** - Tests the full MetricCollector with random operations (counters, gauges, histograms)
2. **fuzz_metric_names** - Focuses on edge cases in metric names (special characters, invalid UTF-8, etc.)
3. **fuzz_tags** - Tests tag handling with various malformed inputs
4. **fuzz_packet_limits** - Tests packet size limits and batching edge cases
5. **fuzz_numeric_values** - Tests numeric edge cases (overflow, underflow, max values)

## Running Fuzz Tests

### List all fuzz targets

```bash
cargo +nightly fuzz list
# or
make fuzz-list
```

### Run individual fuzz targets

```bash
# Run for 60 seconds
make fuzz-collector
make fuzz-names
make fuzz-tags
make fuzz-limits
make fuzz-numbers
```

### Run all fuzz targets

```bash
make fuzz-all
```

### Run with custom duration

```bash
# Run for 300 seconds (5 minutes)
cargo +nightly fuzz run fuzz_metric_collector -- -max_total_time=300
```

### Run with custom options

```bash
# With custom memory limit and timeout
cargo +nightly fuzz run fuzz_metric_collector -- \
    -max_total_time=3600 \
    -rss_limit_mb=4096 \
    -timeout=10
```

## Corpus Management

### Minimize corpus

Reduce the corpus to the smallest set of inputs that maintain the same coverage:

```bash
cargo +nightly fuzz cmin fuzz_metric_collector
# or
make fuzz-cmin
```

### View corpus

The corpus files are stored in `fuzz/corpus/<target_name>/`. Each file contains a test input that triggers unique code paths.

### Add custom inputs to corpus

You can add your own test inputs:

```bash
# Add a file to the corpus
echo "test.metric" > fuzz/corpus/fuzz_metric_names/custom_input
```

## Handling Crashes

If a fuzz target finds a crash, the crash input will be saved to:

```
fuzz/artifacts/<target_name>/crash-<hash>
```

To reproduce a crash:

```bash
cargo +nightly fuzz run fuzz_metric_names fuzz/artifacts/fuzz_metric_names/crash-<hash>
```

## Continuous Fuzzing

For longer fuzzing sessions:

```bash
# Run indefinitely until a crash is found
cargo +nightly fuzz run fuzz_metric_collector
```

## Coverage

To see code coverage from fuzzing:

```bash
cargo +nightly fuzz coverage fuzz_metric_collector
```

## Tips

1. **Memory usage**: Some fuzz targets may accumulate memory. Use `-rss_limit_mb` to set memory limits.
2. **Parallel fuzzing**: Run multiple fuzz targets in parallel in different terminals.
3. **CI integration**: Consider running fuzz tests for a fixed duration (e.g., 5-10 minutes) in CI.
4. **Reproducing issues**: Any input that causes a crash can be used as a test case.

## More Information

- [cargo-fuzz documentation](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [libFuzzer options](https://llvm.org/docs/LibFuzzer.html#options)
