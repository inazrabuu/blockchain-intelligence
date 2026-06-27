use shared::transaction::Transaction;

use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

#[derive(Debug,Default)]
pub struct AnalyticsState {
    pub total_transaction: u64,
    pub total_volume: f64,
    pub largest_transaction: Option<Transaction>,
    pub whale_transaction: u64
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