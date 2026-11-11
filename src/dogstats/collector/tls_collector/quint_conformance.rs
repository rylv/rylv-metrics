use super::{TLSCollector, TLSCollectorOptions, TLSDrain};
use crate::dogstats::collector::{
    DrainMetricCollectorTrait, MetricCollectorTrait, MetricDrain, MetricKind,
};
use crate::{DefaultMetricHasher, RylvStr};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::{mpsc, Arc};

const METRIC: &str = "quint.tls.count";

fn number(state: &Value, field: &str) -> i64 {
    state["s"][field]["#bigint"]
        .as_str()
        .unwrap_or_else(|| panic!("missing ITF bigint: {field}"))
        .parse()
        .unwrap_or_else(|_| panic!("invalid ITF bigint: {field}"))
}

fn flag(state: &Value, field: &str) -> bool {
    state["s"][field]
        .as_bool()
        .unwrap_or_else(|| panic!("missing ITF bool: {field}"))
}

fn active_count(collector: &TLSCollector) -> i64 {
    let mut total = 0;
    for buffer in &collector.buffers {
        let local = buffer.lock();
        total += local.count.iter().map(|(_, value)| *value).sum::<i64>();
    }
    let global = collector.global_aggregator.lock();
    total + global.count.iter().map(|(_, value)| *value).sum::<i64>()
}

fn drain_count(drain: &TLSDrain<'_, DefaultMetricHasher>) -> i64 {
    drain.aggregator.as_ref().map_or(0, |aggregator| {
        aggregator.count.iter().map(|(_, value)| *value).sum()
    })
}

struct Replay<'a> {
    collector: &'a TLSCollector,
    drains: [Option<TLSDrain<'a, DefaultMetricHasher>>; 2],
    recorded: i64,
    removed: i64,
    emitted: i64,
}

impl<'a> Replay<'a> {
    fn new(collector: &'a TLSCollector) -> Self {
        Self {
            collector,
            drains: [None, None],
            recorded: 0,
            removed: 0,
            emitted: 0,
        }
    }

    fn check(&self, state: &Value, trace: &Path, step: usize) {
        let label = format!("{} state {step}", trace.display());
        assert_eq!(
            active_count(self.collector),
            number(state, "undrained"),
            "{label}: undrained"
        );
        for (index, drain) in self.drains.iter().enumerate() {
            let prefix = if index == 0 { "drain1" } else { "drain2" };
            assert_eq!(
                drain.is_some(),
                flag(state, &format!("{prefix}Open")),
                "{label}: {prefix} ownership"
            );
            assert_eq!(
                drain.as_ref().map_or(0, drain_count),
                number(state, prefix),
                "{label}: {prefix} count"
            );
        }
        assert_eq!(
            self.recorded,
            number(state, "recorded"),
            "{label}: recorded"
        );
        assert_eq!(self.removed, number(state, "removed"), "{label}: removed");
        assert_eq!(self.emitted, number(state, "emitted"), "{label}: emitted");
    }

    fn emit_and_close(&mut self, index: usize, expected: i64, label: &str) {
        let mut drain = self.drains[index].take().expect("open TLS drain");
        let observed: i64 = drain
            .frames()
            .filter(|frame| frame.kind == MetricKind::Count && frame.metric == METRIC)
            .map(|frame| frame.value.as_int().unwrap())
            .sum();
        assert_eq!(observed, expected, "{label}: emitted frame");
        self.emitted += observed;
    }

    fn apply(
        &mut self,
        previous: &Value,
        next: &Value,
        workers: &[(mpsc::Sender<()>, mpsc::Receiver<()>)],
        trace: &Path,
        step: usize,
    ) {
        let label = format!("{} state {step}", trace.display());
        let action = next["mbt::actionTaken"]
            .as_str()
            .unwrap_or_else(|| panic!("missing Quint action in {label}"));
        match action {
            "addThread0" | "addThread1" => {
                let index = usize::from(action == "addThread1");
                workers[index].0.send(()).unwrap();
                workers[index].1.recv().unwrap();
                self.recorded += 1;
            }
            "subtractOne" => {
                self.collector
                    .count_add(RylvStr::from_static(METRIC), -1, &mut []);
                self.removed += 1;
            }
            "openDrain1" | "openDrain2" => {
                let index = usize::from(action == "openDrain2");
                assert!(
                    self.drains[index].is_none(),
                    "{label}: drain slot already open"
                );
                self.drains[index] = Some(
                    self.collector
                        .try_begin_drain()
                        .expect("TLS drain always available"),
                );
            }
            "emitAndCloseDrain1" | "emitAndCloseDrain2" => self.emit_and_close(
                usize::from(action == "emitAndCloseDrain2"),
                number(next, "emitted") - number(previous, "emitted"),
                &label,
            ),
            other => panic!("unhandled Quint action {other} in {label}"),
        }
        self.check(next, trace, step);
    }
}

fn replay(trace: &Path) -> HashSet<String> {
    let raw = fs::read_to_string(trace).unwrap();
    let itf: Value = serde_json::from_str(&raw).unwrap();
    let states = itf["states"].as_array().expect("ITF states array");
    assert!(states.len() > 1, "empty Quint trace: {}", trace.display());

    let collector = Arc::new(TLSCollector::new(TLSCollectorOptions::default()));
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..2 {
            let (sender, receiver) = mpsc::channel();
            let (done_sender, done_receiver) = mpsc::channel();
            let worker_collector = Arc::clone(&collector);
            scope.spawn(move || {
                while receiver.recv().is_ok() {
                    worker_collector.count_add(RylvStr::from_static(METRIC), 1, &mut []);
                    done_sender.send(()).unwrap();
                }
            });
            workers.push((sender, done_receiver));
        }

        let mut replay = Replay::new(&collector);
        let mut actions = HashSet::new();
        replay.check(&states[0], trace, 0);
        for (step, pair) in states.windows(2).enumerate() {
            actions.insert(
                pair[1]["mbt::actionTaken"]
                    .as_str()
                    .expect("Quint action name")
                    .to_owned(),
            );
            replay.apply(&pair[0], &pair[1], &workers, trace, step + 1);
        }
        actions
    })
}

#[test]
fn quint_tls_trace_conformance() {
    let Ok(directory) = std::env::var("RYLV_QUINT_TLS_TRACE_DIR") else {
        return;
    };
    let mut traces: Vec<_> = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    traces.sort();
    assert!(!traces.is_empty(), "no Quint TLS traces in {directory}");
    let mut covered = HashSet::new();
    for trace in traces {
        covered.extend(replay(&trace));
    }
    for action in [
        "addThread0",
        "addThread1",
        "subtractOne",
        "openDrain1",
        "openDrain2",
        "emitAndCloseDrain1",
        "emitAndCloseDrain2",
    ] {
        assert!(
            covered.contains(action),
            "Quint TLS traces did not exercise {action}"
        );
    }
}

#[test]
fn write_after_local_swap_is_drained_in_next_generation() {
    let collector = Arc::new(TLSCollector::new(TLSCollectorOptions::default()));
    let (command_tx, command_rx) = mpsc::channel::<Option<i64>>();
    let (done_tx, done_rx) = mpsc::channel();
    let worker_collector = Arc::clone(&collector);
    let worker = std::thread::spawn(move || {
        while let Ok(Some(value)) = command_rx.recv() {
            worker_collector.count_add(RylvStr::from_static(METRIC), value, &mut []);
            done_tx.send(()).unwrap();
        }
    });

    command_tx.send(Some(3)).unwrap();
    done_rx.recv().unwrap();

    let mut hook_calls = 0;
    let detached = collector.flush_all_to_global_with_hook(|| {
        hook_calls += 1;
        command_tx.send(Some(5)).unwrap();
        done_rx.recv().unwrap();
    });
    assert_eq!(hook_calls, 1, "expected exactly one producer buffer");

    let mut first = TLSDrain::new(collector.as_ref(), detached);
    let first_values: Vec<_> = first
        .frames()
        .filter(|frame| frame.kind == MetricKind::Count && frame.metric == METRIC)
        .map(|frame| frame.value.as_int().unwrap())
        .collect();
    assert_eq!(first_values, [3]);
    drop(first);

    let mut second = collector.try_begin_drain().unwrap();
    let second_values: Vec<_> = second
        .frames()
        .filter(|frame| frame.kind == MetricKind::Count && frame.metric == METRIC)
        .map(|frame| frame.value.as_int().unwrap())
        .collect();
    assert_eq!(second_values, [5]);
    drop(second);

    assert_eq!(collector.try_begin_drain().unwrap().count_frames(), 0);
    command_tx.send(None).unwrap();
    worker.join().unwrap();
}
