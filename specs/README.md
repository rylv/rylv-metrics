# Quint models of collector draining

## SharedCollector

[`shared_collector_drain.qnt`](shared_collector_drain.qnt) models one counter key and
the generation swap in `SharedCollector::begin_drain`. A writer may load the
current `Arc` before the swap and finish afterwards. `Arc::try_unwrap` then
fails, leaving the detached generation pending until a later drain attempt.

The model checks that every completed write remains in the current, pending,
or one of two drained generations, or has been emitted exactly once. It also
checks that a generation with a live writer cannot become a `SharedDrain`, and
that the current, pending, and open drain slots own distinct logical
generations. Four example runs cover a blocked drain, a new write proceeding
while the old generation waits, two overlapping drains, and a pending
generation while an earlier drain remains open.

## TLSCollector

[`tls_collector_drain.qnt`](tls_collector_drain.qnt) models one counter key
recorded by two producer threads. It checks that merging the local buffers
accounts for every addition, that signed decrements affect only live buffers,
and that two open drains own separate batches. Four example
runs exercise merging, subtraction across threads, subtraction while a drain
is open, and overlapping generations.

## Run the models

Run the examples and randomized invariant checks with Node.js and npm:

```sh
make quint-check
```

To compare the model with the Rust implementation, run:

```sh
make quint-conformance
```

This generates 64 seeded Quint traces per model in ITF format with action
names, then replays every step against the corresponding Rust collector. The
SharedCollector test holds an `Arc` across a generation swap to represent a
writer paused after `load()`,
records through the same helper as the public counter method, and compares
current, pending, and drained counts after each action. The TLSCollector test
uses two persistent Rust threads and compares the active total and both open
drains after each action. The seeds make CI failures reproducible. Normal
`cargo test` runs skip trace replay unless `RYLV_QUINT_TRACE_DIR` or
`RYLV_QUINT_TLS_TRACE_DIR` points to a directory of generated ITF files.

A separate deterministic Rust test pauses the TLS merge just after swapping
one producer's local buffer. The producer writes again before the old buffer
is merged. It checks that the old value appears in the first drain and the
new value in the next drain. `make quint-conformance` runs this test too.

This is an abstraction of one counter key, at most two writers holding the
current generation, two open drains, and complete `frames()` traversal before
each modeled drain closes. A recycled empty buffer is treated like a fresh one.
It does not model DashMap internals, partial cursor use, histograms,
UDP delivery, or Rust memory ordering. The TLS trace actions are replayed in a
controlled sequence; the separate race test covers one specific interleaving
inside the merge. The randomized checks explore sampled traces; they are not an
exhaustive proof of the Rust code.
