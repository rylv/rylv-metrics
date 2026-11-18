use rylv_metrics::RylvTags;

#[derive(RylvTags)]
struct Bad {
    #[rylv(rename = "a:b")]
    value: &'static str,
}

fn main() {}
