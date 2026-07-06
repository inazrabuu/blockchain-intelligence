use metrics::{counter, histogram, gauge};

pub fn record_http_request() {
    counter!("blockchain_http_requests_total").increment(1);
}

pub fn record_http_duration(duration: f64) {
    histogram!("blockchain_http_requests_duration_seconds")
        .record(duration);
}

pub fn record_ws_message_sent() {
    counter!("blockchain_websocket_messages_sent_total").increment(1);
}

pub fn record_ws_client_connected() {
    gauge!("blockchain_websocket_clients").increment(1.0);
}

pub fn record_ws_client_disconnected() {
    gauge!("blockchain_websocket_clients").decrement(1.0);
}