use rylv_metrics::{MetricCollectorTrait, RylvStr, RylvTags, SharedCollector};

#[derive(RylvTags)]
struct RequestTags<'a> {
    env: RylvStr<'static>,
    method: &'a str,
    #[rylv(rename = "http.route")]
    route: &'a str,
    status: String,
}

fn main() {
    let collector = SharedCollector::default();
    let tags = RequestTags {
        env: RylvStr::from_static("prod"),
        method: "GET",
        route: "/users",
        status: String::from("200"),
    };
    collector.count_add_typed(RylvStr::from_static("requests.total"), 1, &tags);
}
