use shared::transaction::Transaction;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::{info,instrument};
use crate::metrics::HistogramTimer;

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
      .max_connections(5)
      .connect(database_url)
      .await
}

#[instrument(
    skip(pool, transaction),
    fields(
      hash = %transaction.hash,
      amount = %transaction.amount
    )
)]
pub async fn insert_transaction(
  pool: &PgPool,
  transaction: &Transaction,
) -> Result<(), sqlx::Error> {
  let timer = HistogramTimer::start("blockchain_db_insert_duration_seconds");

  let result = sqlx::query(
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
  .await;

  if result.is_ok() {
      timer.observe();
  }
  info!("Transaction persisted!");

  Ok(())
}