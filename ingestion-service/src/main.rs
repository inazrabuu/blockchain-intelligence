mod generator;
mod database;
mod redis_pub;

use shared::transaction::Transaction;
use shared::stream::StreamHub;
use generator::Generator;
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::mpsc;
use tokio::net::TcpListener;
use dotenvy::dotenv;

use tracing::{info,error};
use tracing_subscriber::{fmt, EnvFilter};
use metrics_exporter_prometheus::{PrometheusBuilder,PrometheusHandle};

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
        // producer
        let mut generator = Generator::new();

        loop {
            let trx = generator.generate();
            info!(
                hash = %trx.hash,
                "Producing "
            );            
            tx.send(trx).await.unwrap();
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

            sleep(Duration::from_secs(1)).await;
        }
    });

    use metrics::counter;

    counter!("test_counter").increment(1);

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