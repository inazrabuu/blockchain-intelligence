use axum::{
  extract::Request,
  middleware::Next,
  response::Response
};
use crate::metrics;
use std::time::Instant;

pub async fn metrics_middleware(
    req: Request,
    next: Next
) -> Response {
    let start = Instant::now();

    let response = next.run(req).await;

    //implement counter

    response
}