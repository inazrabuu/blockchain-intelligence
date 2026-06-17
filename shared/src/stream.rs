use tokio::sync::broadcast;
use crate::transaction::Transaction;

#[derive(Clone)]
pub struct StreamHub {
    sender: broadcast::Sender<Transaction>,
}

impl StreamHub {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);

        Self { sender }
    }

    pub fn publish(&self, tx: Transaction) {
        let _ = self.sender.send(tx);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Transaction> {
        self.sender.subscribe()
    }
}