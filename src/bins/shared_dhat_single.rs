use rylv_metrics::{HistogramConfig, MetricCollectorTrait, RylvStr, SharedCollector, SigFig};

#[cfg(not(feature = "allocationcounter"))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let collector = SharedCollector::new(rylv_metrics::SharedCollectorOptions {
        default_histogram_config: HistogramConfig::new(SigFig::new(3).unwrap(), Vec::new())
            .unwrap(),
        ..Default::default()
    });

    let profiler = dhat::Profiler::new_heap();

    for _ in 0..50000000 {
        collector.histogram(
            RylvStr::from_static("some.metric"),
            1,
            [
                RylvStr::from_static("tag:value"),
                RylvStr::from_static("tag2:value2"),
            ],
        );
    }

    drop(collector);
    drop(profiler);
}
