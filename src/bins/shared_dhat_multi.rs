use rylv_metrics::{HistogramConfig, MetricCollectorTrait, RylvStr, SharedCollector, SigFig};

#[cfg(not(feature = "allocationcounter"))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let collector = SharedCollector::new(rylv_metrics::SharedCollectorOptions {
        default_histogram_config: HistogramConfig::new(SigFig::THREE, Vec::new())
            .unwrap(),
        ..Default::default()
    });

    let n = 1024 * 1024;
    let mut vec_metrics = Vec::<&'static str>::with_capacity(n);
    let mut tags_metrics = Vec::<&'static str>::with_capacity(n);

    for i in 0..n {
        vec_metrics.push(format!("some.long.metric.by.some.criteria{i}").leak());
        tags_metrics.push(format!("sometag:somevaluefromcriteria{i}").leak());
    }

    let profiler = dhat::Profiler::new_heap();
    let mut i = 0;
    for _ in 0..50000 {
        collector.histogram(
            RylvStr::from_static(vec_metrics[i]),
            1,
            [
                RylvStr::from_static(tags_metrics[i]),
                RylvStr::from_static("tag:value"),
                RylvStr::from_static("tag2:value2"),
            ],
        );
        i = (i + 1) % n;
    }

    drop(collector);
    drop(profiler);
}
