use super::{record_count_add_in_aggregator, SharedCollector, SharedDrain};
use crate::dogstats::aggregator::Aggregator;
use crate::dogstats::collector::{DrainMetricCollectorTrait, MetricDrain, MetricKind};
use crate::{DefaultMetricHasher, RylvStr};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::hash::BuildHasher;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const METRIC: &str = "quint.count";

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

fn count<S: BuildHasher + Clone>(aggregator: &Aggregator<S>) -> i64 {
    aggregator
        .count
        .iter()
        .map(|entry| entry.value().load(Ordering::Relaxed))
        .sum()
}

struct Replay<'a> {
    collector: &'a SharedCollector,
    held: Vec<(i64, Arc<Aggregator>)>,
    pending_drain: Option<SharedDrain<'a, DefaultMetricHasher>>,
    drains: [Option<SharedDrain<'a, DefaultMetricHasher>>; 2],
    emitted: i64,
    recorded: i64,
}

impl<'a> Replay<'a> {
    fn new(collector: &'a SharedCollector) -> Self {
        Self {
            collector,
            held: Vec::new(),
            pending_drain: None,
            drains: [None, None],
            emitted: 0,
            recorded: 0,
        }
    }

    fn check(&self, state: &Value, trace: &Path, step: usize) {
        let label = format!("{} state {step}", trace.display());
        assert_eq!(
            count(&self.collector.current_aggregator.load()),
            number(state, "current"),
            "{label}: current"
        );
        assert_eq!(
            i64::try_from(
                self.held
                    .iter()
                    .filter(|(generation, _)| *generation == number(state, "currentGen"))
                    .count()
            )
            .unwrap(),
            number(state, "currentWriters"),
            "{label}: current writers"
        );

        let pending_arc = self.collector.pending_to_process_aggregator.lock().unwrap();
        let pending = self
            .pending_drain
            .as_ref()
            .and_then(|drain| drain.aggregator.as_ref())
            .map(count)
            .or_else(|| pending_arc.as_ref().map(|arc| count(arc)));
        assert_eq!(
            pending.is_some(),
            flag(state, "hasPending"),
            "{label}: pending ownership"
        );
        assert_eq!(
            pending.unwrap_or(0),
            number(state, "pending"),
            "{label}: pending count"
        );
        assert_eq!(
            i64::try_from(
                self.held
                    .iter()
                    .filter(|(generation, _)| {
                        flag(state, "hasPending") && *generation == number(state, "pendingGen")
                    })
                    .count()
            )
            .unwrap(),
            number(state, "pendingWriters"),
            "{label}: pending writers"
        );
        drop(pending_arc);

        for (index, drain) in self.drains.iter().enumerate() {
            let prefix = if index == 0 { "drain1" } else { "drain2" };
            assert_eq!(
                drain.is_some(),
                flag(state, &format!("{prefix}Open")),
                "{label}: {prefix} ownership"
            );
            assert_eq!(
                drain
                    .as_ref()
                    .and_then(|d| d.aggregator.as_ref())
                    .map_or(0, count),
                number(state, prefix),
                "{label}: {prefix} count"
            );
        }
        assert_eq!(self.emitted, number(state, "emitted"), "{label}: emitted");
        assert_eq!(
            self.recorded,
            number(state, "recorded"),
            "{label}: recorded"
        );
    }

    fn finish_write(&mut self, generation: i64) {
        let index = self
            .held
            .iter()
            .position(|(id, _)| *id == generation)
            .expect("held writer generation");
        let (_, aggregator) = self.held.remove(index);
        record_count_add_in_aggregator(&aggregator, RylvStr::from_static(METRIC), 1, &mut []);
        self.recorded += 1;
    }

    fn open_drain(&mut self, index: usize) {
        assert!(self.drains[index].is_none());
        self.drains[index] = Some(self.pending_drain.take().unwrap_or_else(|| {
            self.collector
                .try_begin_drain()
                .expect("pending generation should be ready")
        }));
    }

    fn close_drain(&mut self, index: usize, expected: i64, label: &str) {
        let mut drain = self.drains[index].take().expect("open drain");
        let observed: i64 = drain
            .frames()
            .filter(|frame| frame.kind == MetricKind::Count && frame.metric == METRIC)
            .map(|frame| frame.value.as_int().unwrap())
            .sum();
        assert_eq!(observed, expected, "{label}: emitted frame");
        self.emitted += observed;
    }

    fn apply(&mut self, previous: &Value, next: &Value, trace: &Path, step: usize) {
        let label = format!("{} state {step}", trace.display());
        let action = next["mbt::actionTaken"]
            .as_str()
            .unwrap_or_else(|| panic!("missing Quint action in {label}"));
        match action {
            "startWrite" => self.held.push((
                number(previous, "currentGen"),
                self.collector.current_aggregator.load_full(),
            )),
            "finishCurrentWrite" => self.finish_write(number(previous, "currentGen")),
            "finishPendingWrite" => self.finish_write(number(previous, "pendingGen")),
            "swapGeneration" => {
                assert!(
                    self.pending_drain.is_none(),
                    "{label}: pending drain already owned"
                );
                self.pending_drain = self.collector.try_begin_drain();
                assert_eq!(
                    self.pending_drain.is_none(),
                    number(next, "pendingWriters") > 0,
                    "{label}: swap result"
                );
            }
            "retryBlockedDrain" => {
                assert!(
                    self.collector.try_begin_drain().is_none(),
                    "{label}: retry should block"
                );
            }
            "openDrain1" | "openDrain2" => self.open_drain(usize::from(action != "openDrain1")),
            "emitAndCloseDrain1" | "emitAndCloseDrain2" => self.close_drain(
                usize::from(action != "emitAndCloseDrain1"),
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

    let collector = SharedCollector::default();
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
        replay.apply(&pair[0], &pair[1], trace, step + 1);
    }
    actions
}

#[test]
fn quint_trace_conformance() {
    // Ordinary cargo test runs do not require Node.js or Quint. The Makefile
    // generates ITF traces, sets this path, and exercises this replay test.
    let Ok(directory) = std::env::var("RYLV_QUINT_TRACE_DIR") else {
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
    assert!(!traces.is_empty(), "no Quint traces in {directory}");
    let mut covered = HashSet::new();
    for trace in traces {
        covered.extend(replay(&trace));
    }
    for action in [
        "startWrite",
        "finishCurrentWrite",
        "finishPendingWrite",
        "swapGeneration",
        "retryBlockedDrain",
        "openDrain1",
        "openDrain2",
        "emitAndCloseDrain1",
        "emitAndCloseDrain2",
    ] {
        assert!(
            covered.contains(action),
            "Quint traces did not exercise {action}"
        );
    }
}
