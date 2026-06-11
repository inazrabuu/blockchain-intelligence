mod transaction;

use transaction::Transaction;
fn main() {
    println!("Blockchain Intelligence Platform");
    
    let tx = Transaction::new(
        String::from("tx_001"),
        String::from("wallet_a"),
        String::from("wallet_b"),
        12.8
    );

    tx.summary();
}
