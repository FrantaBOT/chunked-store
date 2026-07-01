use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tokio_stream::StreamExt;

use crate::{
    app_state::AppState,
    segment::{Segment, SegmentGuard},
};

pub async fn handle_put(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
    body: Request,
) -> impl IntoResponse {
    println!("PUT: {}", path);

    let segment = Arc::new(Segment::new());
    let _segment_guard = SegmentGuard(Arc::clone(&segment));

    state.segments.insert(path.clone(), Arc::clone(&segment));
    state.segments_list.write().await.insert(path.clone());

    let mut body = body.into_body().into_data_stream();

    loop {
        match body.next().await {
            Some(Ok(chunk)) => segment.add_chunk(chunk),
            Some(Err(error)) => return (StatusCode::BAD_REQUEST, error.to_string()),
            None => return (StatusCode::OK, "ok".into()),
        }
    }
}

pub async fn handle_get(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("GET: {}", path);

    let segment = {
        match state.segments.get(&path) {
            Some(segment) => Arc::clone(&segment),
            None => return (StatusCode::NOT_FOUND, "not found".to_string()).into_response(),
        }
    };

    Body::from_stream(segment.stream()).into_response()
}

pub async fn handle_delete(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("DELETE: {}", path);

    match state.segments.remove(&path) {
        Some(_) => {
            state.segments_list.write().await.remove(&path);

            (StatusCode::OK, "ok".to_string())
        }
        None => (StatusCode::NOT_FOUND, "not found".into()),
    }
}

pub async fn handle_any(
    state: State<Arc<AppState>>,
    params: Query<ListParams>,
    req: Request<Body>,
) -> impl IntoResponse {
    println!("ANY");

    let method = req.method();

    match method.as_str() {
        "LIST" => handle_list(req, params, state).await.into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

#[derive(serde::Deserialize)]
pub struct ListParams {
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn handle_list(
    req: Request<Body>,
    Query(params): Query<ListParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("LIST {}", req.uri());

    let path = req
        .uri()
        .path()
        .trim_start_matches("/")
        .trim_end_matches('*');
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(10000).min(100000);

    let results: Vec<String> = state
        .segments_list
        .read()
        .await
        .range(path.to_string()..=format!("{}~", path))
        .skip(offset)
        .take(limit)
        .cloned()
        .collect();

    (StatusCode::OK, results.join("\n"))
}
