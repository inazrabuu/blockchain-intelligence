use redis::{AsyncCommands, RedisResult};
use futures_util::StreamExt;
use shared::transaction::{self, Transaction};
use tokio::sync::broadcast;

pub async fn connect(
    url: &str
) -> redis::RedisResult<redis::Client> {
    redis::Client::open(url)
}

pub async fn subscribe_transactions(
    client: &redis::Client,
    broadcaster: broadcast::Sender<Transaction>
) -> redis::RedisResult<()> {
    let mut pubsub = client
        .get_async_pubsub()
        .await?;

    pubsub
        .subscribe("transaction_events")
        .await?;
    println!("{}", "Subscribed to tranasction_events");

    let mut stream = pubsub.on_message();

    while let Some(message) = stream.next().await {
        let payload: String = message.get_payload()?;

        let transaction: Transaction = 
            serde_json::from_str(&payload)
            .expect("Failed to deserialize transaction");

        println!("Broadcasting transaction {}", transaction.hash);
        let _ = broadcaster.send(transaction);
    }

    Ok(())
}