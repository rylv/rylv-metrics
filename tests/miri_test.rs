use rylv_metrics::{MetricResult, StatsWriterTrait, StatsWriterType};

#[derive(Default)]
struct MiriCustomWriter {
    chunks: Vec<String>,
    current: String,
}

impl MiriCustomWriter {
    fn all_text(&self) -> String {
        self.chunks.concat()
    }
}

impl StatsWriterTrait for MiriCustomWriter {
    fn metric_copied(&self) -> bool {
        true
    }

    fn write(&mut self, metrics: &[&str], tags: &str, value: &str, metric_type: &str) -> MetricResult<()> {
        for metric in metrics {
            self.current.push_str(metric);
        }
        self.current.push(':');
        self.current.push_str(value);
        self.current.push('|');
        self.current.push_str(metric_type);
        if !tags.is_empty() {
            self.current.push_str("|#");
            self.current.push_str(tags);
        }
        self.current.push('\n');
        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        if self.current.is_empty() {
            return Ok(0);
        }

        let size = self.current.len();
        self.chunks.push(std::mem::take(&mut self.current));
        Ok(size)
    }

    fn reset(&mut self) {
        self.current.clear();
    }
}

#[test]
fn miri_custom_writer_formats_and_flushes() {
    let mut writer = MiriCustomWriter::default();

    writer
        .write(&["custom.metric"], "env:test", "42", "c")
        .expect("write should succeed");
    writer
        .write(&["another.metric"], "", "1", "g")
        .expect("write should succeed");

    let flushed = writer.flush().expect("flush should succeed");
    assert!(flushed > 0);

    let text = writer.all_text();
    assert!(text.contains("custom.metric:42|c|#env:test\n"));
    assert!(text.contains("another.metric:1|g\n"));
}

#[test]
fn miri_custom_writer_reset_clears_pending_buffer() {
    let mut writer = MiriCustomWriter::default();

    writer
        .write(&["pending.metric"], "scope:miri", "7", "g")
        .expect("write should succeed");
    writer.reset();

    let flushed = writer.flush().expect("flush should succeed");
    assert_eq!(flushed, 0);
    assert!(writer.all_text().is_empty());
}

#[test]
fn miri_custom_writer_can_be_wrapped_in_stats_writer_type() {
    let custom: Box<dyn StatsWriterTrait + Send + Sync> = Box::new(MiriCustomWriter::default());
    let writer_type = StatsWriterType::Custom(custom);

    assert!(matches!(writer_type, StatsWriterType::Custom(_)));
}
