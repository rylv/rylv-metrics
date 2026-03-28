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
        collector.histogram(
            RylvStr::from_static("some.metric"),
            1,
            [
                RylvTag::from(RylvStr::from_static("tag:value")),
                RylvTag::from(RylvStr::from_static("tag2:value2")),
            ],
        );
    }

    drop(collector);
    drop(profiler);
}
