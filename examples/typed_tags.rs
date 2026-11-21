use rylv_metrics::{MetricCollectorTrait, RylvStr, RylvTags, SharedCollector};

#[derive(RylvTags)]
struct RequestTags<'a> {
    env: RylvStr<'static>,
    method: RylvStr<'a>,
    #[rylv(rename = "http.route")]
    route: RylvStr<'a>,
    status: RylvStr<'a>,
}

fn main() {
    let collector = SharedCollector::default();
    let tags = RequestTags {
        env: RylvStr::from_static("prod"),
        method: RylvStr::from_static("GET"),
        route: RylvStr::from_static("/users"),
        status: RylvStr::from(String::from("200")),
    };
    collector.count_add_typed(RylvStr::from_static("requests.total"), 1, &tags);
}
