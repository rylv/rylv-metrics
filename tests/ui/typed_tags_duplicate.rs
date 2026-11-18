use rylv_metrics::RylvTags;

#[derive(RylvTags)]
struct Bad {
    env: &'static str,
    #[rylv(rename = "env")]
    other: &'static str,
}

fn main() {}
