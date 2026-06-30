use shared::transaction::Transaction;

use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use std::collections::VecDeque;

const ROLLING_WINDOW_SECONDS: i64 = 60;

#[derive(Debug,Clone,serde::Serialize)]
pub struct AnalyticsSnapshot {
    pub total_transaction: u64,
    pub total_volume: f64,
    pub largest_transaction: Option<Transaction>,
    pub whale_transaction: u64,
    pub rolling_transaction_count: usize,
    pub rolling_volume: f64
}

#[derive(Debug,Clone)]
struct RollingTransaction {
    pub timestamp: i64,
    pub amount: f64
}

#[derive(Debug,Default,Clone)]
pub struct AnalyticsState {
    pub total_transaction: u64,
    pub total_volume: f64,
    pub largest_transaction: Option<Transaction>,
    pub whale_transaction: u64,
    pub recent_transactions: VecDeque<RollingTransaction>,
    pub rolling_transaction_count: usize,
    pub rolling_volume: f64
}

impl AnalyticsState {
    pub fn process_transaction(
      &mut self,
      tx: &Transaction
    ) {
        self.total_transaction += 1;
        self.total_volume += tx.amount;

        if tx.amount > 10_000.0 {
            self.whale_transaction += 1;
        }

        match &self.largest_transaction {
            Some(current) if current.amount >= tx.amount => {}
            _ => {
                self.largest_transaction = Some(tx.clone());
            }
        }

        self.recent_transactions.push_back(
            RollingTransaction { 
                timestamp: tx.timestamp, 
                amount: tx.amount
            }
        );

        self.rolling_transaction_count += 1;
        self.rolling_volume += tx.amount;

        self.evict_old_transaction(tx.timestamp);


    }

    pub fn snapshot(&self) -> AnalyticsSnapshot {
        AnalyticsSnapshot { 
            total_transaction: self.total_transaction, 
            total_volume: self.total_volume, 
            largest_transaction: self.largest_transaction.clone(), 
            whale_transaction: self.whale_transaction ,
            rolling_transaction_count: self.rolling_transaction_count,
            rolling_volume: self.rolling_volume
        }
    }

    pub fn evict_old_transaction(&mut self, current_timestamp: i64) {
        while let Some(tx) = self.recent_transactions.front() {
            let age = current_timestamp - tx.timestamp;
            if age <= ROLLING_WINDOW_SECONDS {
                break;
            }

            if let Some(expired) = self.recent_transactions.pop_front() {
                self.rolling_transaction_count -= 1;
                self.rolling_volume -= expired.amount;
            }
        }
    }
}

pub async fn analytics_worker(
    mut receiver: broadcast::Receiver<Transaction>,
    analytics: Arc<RwLock<AnalyticsState>>
) {
    println!("Analytics worker started");

    loop {
        match receiver.recv().await {
            Ok(tx) => {
                let mut state = analytics.write().await;

                state.process_transaction(&tx);

                println!("
                    [Analytics] processed #{} | volume {:.2}", 
                    state.total_transaction,
                    state.total_volume
                );
            }

            Err(broadcast::error::RecvError::Closed) => {
                println!("Analytics worker stopped.");
                break;
            }

            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                eprintln!(
                    "Analytics worker lagged behind. Skipped {} messages.",
                    skipped
                );
            }
        }
    }
}