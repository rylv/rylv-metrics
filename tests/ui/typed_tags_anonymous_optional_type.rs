fn main() {
    let _ = rylv_metrics::rylv_tags! { env?: Some(String::from("prod")) };
}
