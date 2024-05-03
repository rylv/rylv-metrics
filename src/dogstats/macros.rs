/// Macro for recording histogram values with variable number of tags.
///
/// # Performance
///
/// **This macro is less efficient than calling the trait methods directly.**
/// It uses `RylvStr::from()` for metric names and tags, which converts `&str` literals
/// into `RylvStr::Borrowed`. When the aggregator stores a new metric key, `Borrowed`
/// values require a heap allocation via `RylvStr::to_cow()` (`Cow::Owned`).
///
/// In contrast, calling the trait methods directly with `RylvStr::from_static()` produces
/// `RylvStr::Static`, which converts to `Cow::Borrowed` — **zero-copy, no allocation**.
///
/// For hot paths where metric names and tags are known at compile time, prefer the direct API:
///
/// ```ignore
/// // Preferred: zero-copy on aggregator storage
/// collector.histogram(RylvStr::from_static("latency"), 42, &mut [RylvStr::from_static("env:prod")]);
///
/// // Macro: convenient but allocates when storing new keys
/// histogram!(collector, "latency", 42, "env:prod");
/// ```
///
/// # Examples
///
/// ```
/// use rylv_metrics::{histogram, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType};
/// use std::time::Duration;
///
/// let options = MetricCollectorOptions {
///     max_udp_packet_size: 1500,
///     max_udp_batch_size: 100,
///     flush_interval: Duration::from_millis(100),
///     stats_prefix: String::new(),
///     writer_type: StatsWriterType::Simple,
///     histogram_configs: std::collections::HashMap::new(),
/// };
/// let collector = MetricCollector::new("0.0.0.0:0".parse().unwrap(), "127.0.0.1:8125".parse().unwrap(), options);
///
/// // With static string tags
/// histogram!(collector, "request.duration", 100, "endpoint:api", "method:get");
///
/// // With mixed static and owned string tags
/// histogram!(collector, "response.size", 1024, "service:web", format!("status:{}", 200));
///
/// // With no tags
/// histogram!(collector, "memory.usage", 512);
/// ```
#[macro_export]
macro_rules! histogram {
    // With tags
    ($collector:expr, $metric:expr, $value:expr $(, $tag:expr)+) => {
        {
            #[allow(unused_mut)]
            let mut tags = [$($crate::RylvStr::from($tag)),*];
            $collector.histogram($crate::RylvStr::from($metric), $value, &mut tags)
        }
    };
    // Without tags
    ($collector:expr, $metric:expr, $value:expr) => {
        {
            #[allow(unused_mut)]
            let mut tags: [$crate::RylvStr<'static>; 0] = [];
            $collector.histogram($crate::RylvStr::from($metric), $value, &mut tags)
        }
    };
}

/// Macro for incrementing a counter by one with variable number of tags.
///
/// # Performance
///
/// **This macro is less efficient than calling the trait methods directly.**
/// It uses `RylvStr::from()` for metric names and tags, which converts `&str` literals
/// into `RylvStr::Borrowed`. When the aggregator stores a new metric key, `Borrowed`
/// values require a heap allocation via `RylvStr::to_cow()` (`Cow::Owned`).
///
/// In contrast, calling the trait methods directly with `RylvStr::from_static()` produces
/// `RylvStr::Static`, which converts to `Cow::Borrowed` — **zero-copy, no allocation**.
///
/// For hot paths where metric names and tags are known at compile time, prefer the direct API:
///
/// ```ignore
/// // Preferred: zero-copy on aggregator storage
/// collector.count(RylvStr::from_static("requests"), &mut [RylvStr::from_static("env:prod")]);
///
/// // Macro: convenient but allocates when storing new keys
/// count!(collector, "requests", "env:prod");
/// ```
///
/// # Examples
///
/// ```
/// use rylv_metrics::{count, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType};
/// use std::time::Duration;
///
/// let options = MetricCollectorOptions {
///     max_udp_packet_size: 1500,
///     max_udp_batch_size: 100,
///     flush_interval: Duration::from_millis(100),
///     stats_prefix: String::new(),
///     writer_type: StatsWriterType::Simple,
///     histogram_configs: std::collections::HashMap::new(),
/// };
/// let collector = MetricCollector::new("0.0.0.0:0".parse().unwrap(), "127.0.0.1:8125".parse().unwrap(), options);
///
/// count!(collector, "requests.total", "endpoint:api", "method:get");
/// count!(collector, "errors.total");
/// ```
#[macro_export]
macro_rules! count {
    // With tags
    ($collector:expr, $metric:expr $(, $tag:expr)+) => {
        {
            #[allow(unused_mut)]
            let mut tags = [$($crate::RylvStr::from($tag)),*];
            $collector.count($crate::RylvStr::from($metric), &mut tags)
        }
    };
    // Without tags
    ($collector:expr, $metric:expr) => {
        {
            #[allow(unused_mut)]
            let mut tags: [$crate::RylvStr<'static>; 0] = [];
            $collector.count($crate::RylvStr::from($metric), &mut tags)
        }
    };
}

/// Macro for incrementing a counter by a value with variable number of tags.
///
/// # Performance
///
/// **This macro is less efficient than calling the trait methods directly.**
/// It uses `RylvStr::from()` for metric names and tags, which converts `&str` literals
/// into `RylvStr::Borrowed`. When the aggregator stores a new metric key, `Borrowed`
/// values require a heap allocation via `RylvStr::to_cow()` (`Cow::Owned`).
///
/// In contrast, calling the trait methods directly with `RylvStr::from_static()` produces
/// `RylvStr::Static`, which converts to `Cow::Borrowed` — **zero-copy, no allocation**.
///
/// For hot paths where metric names and tags are known at compile time, prefer the direct API:
///
/// ```ignore
/// // Preferred: zero-copy on aggregator storage
/// collector.count_add(RylvStr::from_static("bytes.sent"), 1024, &mut [RylvStr::from_static("env:prod")]);
///
/// // Macro: convenient but allocates when storing new keys
/// count_add!(collector, "bytes.sent", 1024, "env:prod");
/// ```
///
/// # Examples
///
/// ```
/// use rylv_metrics::{count_add, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType};
/// use std::time::Duration;
///
/// let options = MetricCollectorOptions {
///     max_udp_packet_size: 1500,
///     max_udp_batch_size: 100,
///     flush_interval: Duration::from_millis(100),
///     stats_prefix: String::new(),
///     writer_type: StatsWriterType::Simple,
///     histogram_configs: std::collections::HashMap::new(),
/// };
/// let collector = MetricCollector::new("0.0.0.0:0".parse().unwrap(), "127.0.0.1:8125".parse().unwrap(), options);
///
/// count_add!(collector, "bytes.sent", 1024, "endpoint:api");
/// count_add!(collector, "events.total", 5);
/// ```
#[macro_export]
macro_rules! count_add {
    // With tags
    ($collector:expr, $metric:expr, $value:expr $(, $tag:expr)+) => {
        {
            #[allow(unused_mut)]
            let mut tags = [$($crate::RylvStr::from($tag)),*];
            $collector.count_add($crate::RylvStr::from($metric), $value, &mut tags)
        }
    };
    // Without tags
    ($collector:expr, $metric:expr, $value:expr) => {
        {
            #[allow(unused_mut)]
            let mut tags: [$crate::RylvStr<'static>; 0] = [];
            $collector.count_add($crate::RylvStr::from($metric), $value, &mut tags)
        }
    };
}

/// Macro for recording a gauge value with variable number of tags.
///
/// # Performance
///
/// **This macro is less efficient than calling the trait methods directly.**
/// It uses `RylvStr::from()` for metric names and tags, which converts `&str` literals
/// into `RylvStr::Borrowed`. When the aggregator stores a new metric key, `Borrowed`
/// values require a heap allocation via `RylvStr::to_cow()` (`Cow::Owned`).
///
/// In contrast, calling the trait methods directly with `RylvStr::from_static()` produces
/// `RylvStr::Static`, which converts to `Cow::Borrowed` — **zero-copy, no allocation**.
///
/// For hot paths where metric names and tags are known at compile time, prefer the direct API:
///
/// ```ignore
/// // Preferred: zero-copy on aggregator storage
/// collector.gauge(RylvStr::from_static("connections"), 42, &mut [RylvStr::from_static("pool:main")]);
///
/// // Macro: convenient but allocates when storing new keys
/// gauge!(collector, "connections", 42, "pool:main");
/// ```
///
/// # Examples
///
/// ```
/// use rylv_metrics::{gauge, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType};
/// use std::time::Duration;
///
/// let options = MetricCollectorOptions {
///     max_udp_packet_size: 1500,
///     max_udp_batch_size: 100,
///     flush_interval: Duration::from_millis(100),
///     stats_prefix: String::new(),
///     writer_type: StatsWriterType::Simple,
///     histogram_configs: std::collections::HashMap::new(),
/// };
/// let collector = MetricCollector::new("0.0.0.0:0".parse().unwrap(), "127.0.0.1:8125".parse().unwrap(), options);
///
/// gauge!(collector, "connections.active", 42, "pool:main");
/// gauge!(collector, "memory.usage", 512);
/// ```
#[macro_export]
macro_rules! gauge {
    // With tags
    ($collector:expr, $metric:expr, $value:expr $(, $tag:expr)+) => {
        {
            #[allow(unused_mut)]
            let mut tags = [$($crate::RylvStr::from($tag)),*];
            $collector.gauge($crate::RylvStr::from($metric), $value, &mut tags)
        }
    };
    // Without tags
    ($collector:expr, $metric:expr, $value:expr) => {
        {
            #[allow(unused_mut)]
            let mut tags: [$crate::RylvStr<'static>; 0] = [];
            $collector.gauge($crate::RylvStr::from($metric), $value, &mut tags)
        }
    };
}
