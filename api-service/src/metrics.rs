use metrics::counter;

pub fn http_request() {
    counter!("blockchain_http_requests_total").increment(1);
}