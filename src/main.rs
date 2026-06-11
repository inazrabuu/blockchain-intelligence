struct Transaction {
    hash: String,
    from: String,
    to: String,
    amount: f64
}

impl Transaction {
    fn new(
        hash: String,
        from: String,
        to: String,
        amount: f64
    ) -> Self{
        Self { hash, from, to, amount }
    }

    fn summary(&self) {
        println!(
            "Transaction {}: {} sent {} ETH to {}",
            self.hash,
            self.from,
            self.amount,
            self.to
        )
    }
}
fn main() {
    let tx = Transaction::new(
        String::from("tx_001"),
        String::from("wallet_a"),
        String::from("wallet_b"),
        12.8
    );

    tx.summary();
    println!("Blockchain Intelligence Platform");
}
