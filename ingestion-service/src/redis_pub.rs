use redis::AsyncCommands;
use tracing::{info, instrument, error};
use crate::metrics::{
    record_redis_publish, 
    record_redis_publish_failure,
    HistogramTimer
};

pub async fn connect(
  url: &str
) -> redis::RedisResult<redis::Client> {
    redis::Client::open(url)
}

#[instrument(
    skip(client, payload),
    fields(
        payload = %payload
    )
)]
pub async fn publish_transaction(
  client: &redis::Client,
  payload: &str
) -> redis::RedisResult<()> {
    let timer = HistogramTimer::start("blockchain_redis_publish_duration_seconds");
    let mut conn = client.get_multiplexed_async_connection().await?;

    match conn
        .publish::<_, _, i64>("transaction_events", payload)
        .await
    {
        Ok(subscribers) => {
            info!(subscribers, "Transaction published");
            record_redis_publish();
            timer.observe();
            Ok(())
        }

        Err(err) => {
            error!(error = %err, "Failed to publish transaction");
            record_redis_publish_failure();
            Err(err)
        }
    }
}