use metrics::{counter, histogram};

pub fn record_http_request() {
    counter!("blockchain_http_requests_total").increment(1);
}

pub fn record_http_duration(duration: f64) {
    histogram!("blockchain_http_requests_duration_seconds")
        .record(duration);
}