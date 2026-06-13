mod generator;
mod transaction;

use generator::Generator;
use transaction::Transaction;
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::mpsc;
#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<Transaction>(100);

    let producer = tokio::spawn(async move {
        // producer
        let mut generator = Generator::new();

        loop {
            let trx = generator.generate();
            println!("Producing {}", trx.hash);            
            tx.send(trx).await.unwrap();
        }
    });

    let consumer = tokio::spawn(async move {
        // consumer
        while let Some(transaction) = rx.recv().await {
            println!("Consuming {}", transaction.hash);
            transaction.summary();
            sleep(Duration::from_secs(1)).await;
        }
    });

    let _ = tokio::join!(producer, consumer);
}