use redis::AsyncCommands;
use tracing::instrument;

pub async fn connect(
  url: &str
) -> redis::RedisResult<redis::Client> {
    redis::Client::open(url)
}

#[instrument(
    skip(client, payload)
)]
pub async fn publish_transaction(
  client: &redis::Client,
  payload: &str
) -> redis::RedisResult<()> {
    let mut conn = client.get_multiplexed_async_connection().await?;

    let _: () = conn
        .publish("transaction_events", payload)
        .await?;

    Ok(())
}