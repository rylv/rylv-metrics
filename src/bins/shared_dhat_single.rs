// Target-specific exceptions to the shared library lint policy.
#![allow(clippy::unreadable_literal, clippy::unwrap_used, missing_docs)]

use rylv_metrics::{
    HistogramConfig, MetricCollectorTrait, RylvStr, RylvTag, SharedCollector, SigFig,
};

#[cfg(not(feature = "allocationcounter"))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let collector = SharedCollector::new(rylv_metrics::SharedCollectorOptions {
        default_histogram_config: HistogramConfig::new(SigFig::THREE, Vec::new()).unwrap(),
        ..Default::default()
    });

    let profiler = dhat::Profiler::new_heap();

    for _ in 0..50000000 {
        let _ = collector.histogram(
            RylvStr::from_static("some.metric"),
            1,
            [
                RylvTag::from_static("tag:value"),
                RylvTag::from_static("tag2:value2"),
            ],
        );
    }

    drop(collector);
    drop(profiler);
}
