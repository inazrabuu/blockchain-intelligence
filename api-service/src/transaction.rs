use serde::Serialize;

#[derive(Serialize)]
pub struct Transaction {
    pub hash: String,
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub timestamp: i64,
}