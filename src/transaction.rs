pub struct Transaction {
    pub hash: String,
    pub from: String,
    pub to: String,
    pub amount: f64,
}

impl Transaction {
    pub fn new(hash: String, from: String, to: String, amount: f64) -> Self {
        Self {
            hash,
            from,
            to,
            amount,
        }
    }

    pub fn summary(&self) {
        println!(
            "Transaction {}: {} sent {} ETH to {}",
            self.hash, self.from, self.amount, self.to
        )
    }
}
