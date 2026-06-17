use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transacion {
    pub hash: String,
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub timestamp: i64
}

impl Transaction {
    pub fn new(hash: String, from: String, to: String, amount: f64, timestamp: i64) -> Self {
        Self {
            hash,
            from,
            to,
            amount,
            timestamp
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "Transaction {}: {} sent {} ETH to {} on {}",
            self.hash, self.from, self.amount, self.to, self.timestamp
        )
    }
}