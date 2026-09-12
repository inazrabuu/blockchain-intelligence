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

pub async fn migrate(
    pool: &PgPool
) -> Result<(), sqlx::Error> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await?;

    Ok(())
}

#[instrument(
    skip(pool, transaction),
    fields(
      hash = %transaction.hash,
      amount = %transaction.amount_wei
    )
)]
pub async fn insert_transaction(
  pool: &PgPool,
  transaction: &Transaction,
) -> Result<(), sqlx::Error> {
  let timer = HistogramTimer::start("blockchain_db_insert_duration_seconds");

  sqlx::query(
    r#"
    INSERT INTO transactions (
      hash,
      from_address,
      to_address,
      amount_wei,
      timestamp
    ) VALUES ($1, $2, $3, $4::NUMERIC, $5)
    "#
  )
  .bind(&transaction.hash)
  .bind(&transaction.from)
  .bind(&transaction.to)
  .bind(&transaction.amount_wei)
  .bind(&transaction.timestamp)
  .execute(pool)
  .await?;

  // if result.is_ok() {
  //     timer.observe();
  // }
  timer.observe();
  info!("Transaction persisted!");

  Ok(())
}