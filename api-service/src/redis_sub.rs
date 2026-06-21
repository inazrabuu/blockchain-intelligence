use redis::{AsyncCommands, RedisResult};
use futures_util::StreamExt;

pub async fn connect(
    url: &str
) -> redis::RedisResult<redis::Client> {
    redis::Client::open(url)
}

pub async fn subscribe_transactions(
    client: &redis::Client,
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

        println!("Received: {}", payload);
    }

    Ok(())
}