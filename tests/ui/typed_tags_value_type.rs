use rylv_metrics::RylvTags;

#[derive(RylvTags)]
struct Bad<'a> {
    env: &'a str,
}

fn main() {}
