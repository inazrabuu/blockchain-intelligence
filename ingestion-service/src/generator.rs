use crate::transaction::Transaction;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Generator {
    counter: u64,
}

impl Generator {
    pub fn new() -> Self {
        Self { counter: 0 }
    }

    pub fn generate(&mut self) -> Transaction {
        self.counter += 1;
        let timestamp = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as i64;

        Transaction::new(
            format!("tx_{:03}", self.counter),
            String::from("wallet_a"),
            String::from("wallet_b"),
            self.counter as f64,
            timestamp
        )
    }
}
