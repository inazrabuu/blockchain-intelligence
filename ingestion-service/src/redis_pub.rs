use redis::AsyncCommands;

pub async fn connect(
  url: &str
) -> redis::RedisResult<redis::Client> {
    redis::Client::open(url)
}