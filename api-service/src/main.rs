use axum::{
    routing::get,
    Json,
    Router
};
use serde::Serialize;

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

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health));

    let listener = 
        tokio::net::TcpListener::bind("0.0.0.0:3000")
            .await
            .unwrap();

    println!("API listening on port 3000");

    axum::serve(listener, app)
        .await
        .unwrap();
}
