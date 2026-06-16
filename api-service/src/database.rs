use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use crate::transaction::Transaction;

pub async fn connect(
  database_url: &str
) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
      .max_connections(5)
      .connect(database_url)
      .await
}

pub async fn get_transactions(
  pool: &PgPool,
  limit: i64,
  offset: i64,
) -> Result<Vec<Transaction>, sqlx::Error> {
    let rows = sqlx::query(
      r#"
        SELECT hash, "from", "to", amount, timestamp 
        FROM transactions
        ORDER BY timestamp DESC 
        LIMIT $1 OFFSET $2
      "#
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let transactions = rows
        .into_iter()
        .map(|row| Transaction {
          hash: row.get("hash"),
          from: row.get("from"),
          to: row.get("to"),
          amount: row.get("amount"),
          timestamp: row.get("timestamp")
        })
        .collect();

    Ok(transactions)
}

pub async fn get_transaction_by_hash(
    pool: &PgPool,
    hash: &str,
) -> Result<Option<Transaction>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT
            hash,
            "from",
            "to",
            amount,
            timestamp
        FROM transactions
        WHERE hash = $1
        "#
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => Ok(Some(Transaction {
            hash: row.get("hash"),
            from: row.get("from"),
            to: row.get("to"),
            amount: row.get("amount"),
            timestamp: row.get("timestamp"),
        })),
        None => Ok(None),
    }
}