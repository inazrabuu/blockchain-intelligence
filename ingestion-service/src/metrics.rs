use metrics::counter;

pub fn record_transaction_processed() {
    counter!("blockchain_transaction_processed_total").increment(1);
}