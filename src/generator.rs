use crate::transaction::Transaction;

pub struct Generator {
    counter: u64,
}

impl Generator {
    pub fn new() -> Self {
        Self { counter: 0 }
    }

    pub fn generate(&mut self) -> Transaction {
        self.counter += 1;

        Transaction::new(
            format!("tx_{:03}", self.counter),
            String::from("wallet_a"),
            String::from("wallet_b"),
            self.counter as f64,
        )
    }
}
