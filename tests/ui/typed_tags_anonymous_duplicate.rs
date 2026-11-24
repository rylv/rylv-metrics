fn main() {
    let _ = rylv_metrics::rylv_tags! {
        env: rylv_metrics::RylvStr::from_static("prod"),
        #[rylv(rename = "env")]
        other?: None,
    };
}
