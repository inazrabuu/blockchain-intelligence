mod database;
mod transaction;

use dotenvy::dotenv;
use transaction::Transaction;
use axum::{
    routing::get,
    Json,
    Router
};
use serde::Serialize;
use std::sync::Arc;
use axum::extract::State;

#[derive(Clone)]
struct AppState {
    pool: sqlx::PgPool,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String
}

async fn health() -> Json<HealthResponse> {
    Json(
        HealthResponse {
            status: String::from("Ok"),
        }
    )
}

async fn get_transactions(
    State(state): State<Arc<AppState>>
) -> Json<Vec<Transaction>> {
    let transactions = 
        database::get_transactions(&state.pool)
        .await
        .unwrap();

    Json(transactions)
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let database_url = std::env::var("DATABASE_URL")
                        .expect("DATABASE_URL not found");
    let pool = database::connect(&database_url)
                .await
                .expect("Failed to connect to PostgreSQL");
    println!("Connected to PostgreSQL!");

    let state = Arc::new(AppState { pool });

    let app = Router::new()
        .route("/health", get(health))
        .route("/transactions", get(get_transactions))
        .with_state(state);

    let listener = 
        tokio::net::TcpListener::bind("0.0.0.0:3000")
            .await
            .unwrap();

    println!("API listening on port 3000");

    axum::serve(listener, app)
        .await
        .unwrap();
}
