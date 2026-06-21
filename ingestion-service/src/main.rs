mod generator;
mod database;
mod redis_pub;

use shared::transaction::Transaction;
use shared::stream::StreamHub;
use generator::Generator;
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::mpsc;
use dotenvy::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    dotenv().ok();

    let database_url = 
        std::env::var("DATABASE_URL")
        .expect("DATABASE_URL is not found");

    let pool = 
        database::connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");
    println!("Database Postgre connected.");

    let redis_url = 
        std::env::var("REDIS_URL")
        .expect("REDIS_URL is not found");

    let redis_client = 
        redis_pub::connect(&redis_url)
        .await?;
    println!("{}", "Redis connected");

    let (tx, mut rx) = 
        mpsc::channel::<Transaction>(100);

    let hub = StreamHub::new(1000);
    let consumer_hub = hub.clone();

    let producer = tokio::spawn(async move {
        // producer
        let mut generator = Generator::new();

        loop {
            let trx = generator.generate();
            println!("Producing {}", trx.hash);            
            tx.send(trx).await.unwrap();
        }
    });

    let consumer_redis = redis_client.clone();
    let consumer = tokio::spawn(async move {
        // consumer
        while let Some(transaction) = rx.recv().await {
            println!("Consuming {}", transaction.hash);
            println!("{}", transaction.summary());

            if let Err(err) = database::insert_transaction(&pool, &transaction).await {
                eprintln!("Insert failed: {}", err);
                continue;
            }

            let payload = serde_json::to_string(&transaction)
                .expect("serialize transaction");

            if let Err(err) = redis_pub::publish_transaction(&consumer_redis, &payload).await {
                eprintln!("Redis publish failed {}", err);
            }
            println!("published {} to Redis", transaction.hash);

            consumer_hub.publish(transaction.clone());
            println!("Published to stream: {}", transaction.hash);

            sleep(Duration::from_secs(1)).await;
        }
    });

    let _ = tokio::join!(producer, consumer);

    Ok(())
}