use shared::transaction::Transaction;
use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
      .max_connections(5)
      .connect(database_url)
      .await
}

pub async fn insert_transaction(
  pool: &PgPool,
  transaction: &Transaction,
) -> Result<(), sqlx::Error> {
  sqlx::query(
    r#"
    INSERT INTO transactions (
      hash,
      "from",
      "to",
      amount,
      timestamp
    ) VALUES ($1, $2, $3, $4, $5)
    "#
  )
  .bind(&transaction.hash)
  .bind(&transaction.from)
  .bind(&transaction.to)
  .bind(&transaction.amount)
  .bind(&transaction.timestamp)
  .execute(pool)
  .await?;

  Ok(())
}