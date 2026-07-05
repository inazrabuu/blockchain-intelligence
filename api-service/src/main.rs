mod database;
mod redis_sub;
mod analytics;
mod middleware;
mod metrics;

use shared::transaction::Transaction;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use dotenvy::dotenv;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::{Response, IntoResponse},
    routing::get,
    Json,
    Router
};
use serde::Serialize;
use serde::Deserialize;
use axum::extract::{Path, State, Query};
use axum::http::StatusCode;

use crate::analytics::{
    AnalyticsState,
    AnalyticsSnapshot,
    analytics_worker
};

use tracing::{info, error};
use tracing_subscriber::{fmt, EnvFilter};
use metrics_exporter_prometheus::PrometheusBuilder;

#[derive(Clone)]
struct AppState {
    pool: sqlx::PgPool,
    broadcaster: broadcast::Sender<Transaction>,
    analytics: Arc<RwLock<AnalyticsState>>
}

#[derive(Serialize)]
struct HealthResponse {
    status: String
}

#[derive(Deserialize)]
struct PaginationParams {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn health() -> Json<HealthResponse> {
    Json(
        HealthResponse {
            status: String::from("Ok"),
        }
    )
}

async fn get_transactions(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PaginationParams>
) -> Json<Vec<Transaction>> {
    let limit = params.limit.unwrap_or(10);
    let offset = params.offset.unwrap_or(0);

    let transactions = 
        database::get_transactions(
            &state.pool,
            limit,
            offset
        )
        .await
        .unwrap();

    Json(transactions)
}

async fn get_transaction_by_hash(
    Path(hash): Path<String>,
    State(state): State<std::sync::Arc<AppState>>,
) -> Result<Json<Transaction>, StatusCode> {

    let transaction =
        database::get_transaction_by_hash(
            &state.pool,
            &hash,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match transaction {
        Some(tx) => Ok(Json(tx)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    info!("WebSocket client connected");
    let mut receiver = state.broadcaster.subscribe();

    loop {
        tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Ok(tx) => {
                        info!(
                            hash = %tx.hash,
                            "Sending transaction to websocket"
                        );
                        let json = serde_json::to_string(&tx).unwrap();

                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            result = socket.recv() => {
                match result {
                    Some(Ok(_)) => {},
                    _ => break,
                }
            }
        }
    }

    info!("WebSocket client disconnected");
}

async fn analytics_handler(
    State(state): State<Arc<AppState>>
) -> Json<AnalyticsSnapshot> {
    let snapshot = {
        let analytics = state.analytics.read().await;
        analytics.snapshot()
    };

    Json(snapshot)
}

async fn metrics_handler(
    State(prometheus): State<Arc<metrics_exporter_prometheus::PrometheusHandle>>
) -> Response {
    Response::builder()
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(prometheus.render().into())
        .unwrap()
}

#[tokio::main]
async fn main() {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();
    info!("Starting API Service");

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    dotenv().ok();

    let database_url = std::env::var("DATABASE_URL")
                        .expect("DATABASE_URL not found");
    let pool = database::connect(&database_url)
                .await
                .expect("Failed to connect to PostgreSQL");
    info!("Connected to PostgreSQL!");

    let (broadcaster, _) = 
        broadcast::channel::<Transaction>(1000);

    let analytics = Arc::new(
        RwLock::new(AnalyticsState::default())
    );
    let analytics_rx = broadcaster.subscribe();

    let state = Arc::new(AppState { 
        pool, 
        broadcaster,
        analytics: analytics.clone()
    });

    let redis_url =
         std::env::var("REDIS_URL")
        .expect("REDIS_URL is not found");

    let redis_client = 
        redis_sub::connect(&redis_url)
        .await
        .expect("Failed to connect to Redis");

    let redis_broadcaster = 
        state.broadcaster.clone();

    tokio::spawn(async move {
        if let Err(err) = 
            redis_sub::subscribe_transactions(
                &redis_client, 
                redis_broadcaster
            )
            .await
        {
            error!(
                error = %err,
                "Redis subscriber error"
            );
        }
    });

    let analytics_state = analytics.clone();
    tokio::spawn(async move {
        analytics_worker(
            analytics_rx,
            analytics_state
        ).await
    });

    let api_router = Router::new()
        .route("/health", get(health))
        .route("/transactions", get(get_transactions))
        .route("/transaction/{hash}", get(get_transaction_by_hash))
        .route("/ws", get(ws_handler))
        .route("/analytics", get(analytics_handler))
        .with_state(state)
        .layer(
            axum::middleware::from_fn(middleware::metrics_middleware)
        );

    let prometheus = Arc::new(prometheus_handle);
    let metrics_router = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(prometheus);

    let app = api_router.merge(metrics_router);

    let listener = 
        tokio::net::TcpListener::bind("0.0.0.0:3000")
            .await
            .unwrap();

    info!(
        address = "0.0.0.0:3000",
        "API server started"
    );

    axum::serve(listener, app)
        .await
        .unwrap();
}