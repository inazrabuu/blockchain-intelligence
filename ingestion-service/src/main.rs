mod blockchain;
mod database;
mod redis_pub;
mod metrics;

use shared::transaction::Transaction;
use shared::stream::StreamHub;
// use std::time::Duration;
// use tokio::time::sleep;
use tokio::sync::mpsc;
use tokio::net::TcpListener;
use dotenvy::dotenv;

use tracing::{info,error};
use tracing_subscriber::{fmt, EnvFilter};
use metrics_exporter_prometheus::{PrometheusBuilder,PrometheusHandle};
use crate::metrics::{record_transaction_processed};

#[derive(Clone)]
struct AppState {
    prometheus: PrometheusHandle
}

use axum::{
    extract::State, response::IntoResponse, routing::get, Router
};

async fn metrics_handler(
    State(state): State<AppState>
) -> impl IntoResponse {
    state.prometheus.render()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();
    info!("Starting Ingestion Service");

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()?;

    dotenv().ok();

    let database_url = 
        std::env::var("DATABASE_URL")
        .expect("DATABASE_URL is not found");

    let pool = 
        database::connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");
    info!("Database Postgre connected.");

    database::migrate(&pool)
        .await
        .expect("Failed to run database migration");
    info!("Database migration completed.");

    let redis_url = 
        std::env::var("REDIS_URL")
        .expect("REDIS_URL is not found");

    let redis_client = 
        redis_pub::connect(&redis_url)
        .await?;
    info!("Redis connected");

    let (tx, mut rx) = 
        mpsc::channel::<Transaction>(100);

    let hub = StreamHub::new(1000);
    let consumer_hub = hub.clone();

    
    let producer = tokio::spawn(async move {
        let ws_url = 
            std::env::var("ETH_WS_URL")
            .expect("ETH_WS_URL is not found");
        let http_url = 
            std::env::var("ETH_HTTP_URL")
            .expect("ETH_HTTP_URL is not found");

        if let Err(err) = 
            blockchain::subscribe_blocks(&ws_url, &http_url, tx).await 
        {
            error!(
                error = %err,
                "Blockchain subscription stopped"
            );
        }
    });

    let consumer_redis = redis_client.clone();
    let consumer = tokio::spawn(async move {
        // consumer
        while let Some(transaction) = rx.recv().await {
            info!(
                hash = %transaction.hash,
                "Consuming transaction"
            );
            info!(
                summary = %transaction.summary()
            );

            if let Err(err) = database::insert_transaction(&pool, &transaction).await {
                error!(
                    error = %err,
                    "Insert failed"
                );
                continue;
            }
            record_transaction_processed();


            let payload = serde_json::to_string(&transaction)
                .expect("serialize transaction");

            if let Err(err) = redis_pub::publish_transaction(&consumer_redis, &payload).await {
                error!(
                    error = %err,
                    "Redis publish failed"
                );
            }
            info!(
                hash = %transaction.hash,
                "published transaction to Redis"
            );

            consumer_hub.publish(transaction.clone());
            info!(
                hash = %transaction.hash,
                "Published to stream:"
            );

            // sleep(Duration::from_secs(1)).await;
        }
    });

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(AppState {
            prometheus: prometheus_handle.clone()
        });
    
    let metrics_server = tokio::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:8001")
            .await
            .unwrap();

        info!(
            address = "0.0.0.0:8001",
            "API metrics started"
        );

        axum::serve(listener, app)
            .await
            .unwrap();
    });

    let _ = tokio::join!(producer, consumer, metrics_server);

    Ok(())
}