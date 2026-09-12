use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub hash: String,
    pub from: String,
    pub to: Option<String>,
    pub amount_wei: String,
    pub timestamp: i64
}

impl Transaction {
    pub fn new(hash: String, from: String, to: Option<String>, amount_wei: String, timestamp: i64) -> Self {
        Self {
            hash,
            from,
            to,
            amount_wei,
            timestamp
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "Transaction {}: {} sent {} ETH to {:?} on {}",
            self.hash, self.from, self.amount_wei, self.to, self.timestamp
        )
    }
}