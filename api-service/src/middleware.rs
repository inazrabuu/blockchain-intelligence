use axum::{
  extract::Request,
  middleware::Next,
  response::Response
};
use crate::metrics::{record_http_request, record_http_duration};
use std::time::Instant;

pub async fn metrics_middleware(
    req: Request,
    next: Next
) -> Response {
    let start = Instant::now();

    let response = next.run(req).await;

    record_http_request();
    record_http_duration(start.elapsed().as_secs_f64());
    
    response
}