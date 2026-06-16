mod generator;
mod transaction;
mod database;

use generator::Generator;
use transaction::Transaction;
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::mpsc;
use dotenvy::dotenv;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let database_url = 
        std::env::var("DATABASE_URL")
        .expect("DATABASE_URL is not found");

    let pool = 
        database::connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");
    println!("Database Postgre connected.");

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

            if let Err(err) = database::insert_transaction(&pool, &transaction).await {
                eprintln!("Insert failed: {}", err)
            }

            sleep(Duration::from_secs(1)).await;
        }
    });

    let _ = tokio::join!(producer, consumer);
}