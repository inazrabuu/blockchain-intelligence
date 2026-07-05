use metrics::{counter, histogram};
use std::time::Instant;

pub fn record_transaction_processed() {
    counter!("blockchain_transaction_processed_total").increment(1);
}

pub struct HistogramTimer {
    start: Instant,
    metric_name: &'static str
}

impl HistogramTimer {
    pub fn start(metric_name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metric_name: metric_name
        }
    }

    pub fn observe(self) {
        let duration = self.start.elapsed().as_secs_f64();
        histogram!(self.metric_name).record(duration);
    }
}