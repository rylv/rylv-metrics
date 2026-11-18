use rylv_metrics::RylvTags;

#[derive(RylvTags)]
struct Bad {
    #[rylv(unknown)]
    value: &'static str,
}

fn main() {}
