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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;

    async fn body_to_string(body: Body) -> String {
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .unwrap_or_default();
        String::from_utf8(bytes.to_vec()).unwrap_or_default()
    }

    #[tokio::test]
    async fn test_handle_put_success() {
        let state = Arc::new(AppState::default());
        let path = "test".to_string();

        let body = Body::from("chunk");
        let request = Request::builder().method("PUT").body(body).unwrap();

        let response = handle_put(Path(path.clone()), State(state.clone()), request)
            .await
            .into_response();

        let status = response.status();
        assert_eq!(status, StatusCode::OK);

        assert!(state.segments.contains_key(&path));

        let list = state.segments_list.read().await;
        assert!(list.contains(&path));
    }

    #[tokio::test]
    async fn test_handle_get_success() {
        let state = Arc::new(AppState::default());
        let path = "test".to_string();

        let segment = Arc::new(Segment::new());
        segment.add_chunk(Bytes::from("chunk"));
        state.segments.insert(path.clone(), segment);

        let response = handle_get(Path(path.clone()), State(state.clone()))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_handle_get_not_found() {
        let state = Arc::new(AppState::default());
        let path = "test".to_string();

        let response = handle_get(Path(path), State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_handle_delete_success() {
        let state = Arc::new(AppState::default());
        let path = "test".to_string();

        let segment = Arc::new(Segment::new());
        state.segments.insert(path.clone(), segment);
        state.segments_list.write().await.insert(path.clone());

        let response = handle_delete(Path(path.clone()), State(state.clone()))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        assert!(!state.segments.contains_key(&path));

        let list = state.segments_list.read().await;
        assert!(!list.contains(&path));
    }

    #[tokio::test]
    async fn test_handle_delete_not_found() {
        let state = Arc::new(AppState::default());
        let path = "test".to_string();

        let response = handle_delete(Path(path), State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_handle_list_default_params() {
        let state = Arc::new(AppState::default());

        let request = Request::builder()
            .method("LIST")
            .uri("/test/")
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: None,
            offset: None,
        });

        let response = handle_list(request, params, State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_handle_list_path_prefix() {
        let state = Arc::new(AppState::default());
        let path = "/test/?limit=10".to_string();

        for prefix in &["test/", "other/", "test/sub/"] {
            for i in 0..3 {
                let path = format!("{}path{}", prefix, i);
                let segment = Arc::new(Segment::new());
                state.segments.insert(path.clone(), segment);
                state.segments_list.write().await.insert(path);
            }
        }

        let request = Request::builder()
            .method("LIST")
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: Some(10),
            offset: None,
        });

        let response = handle_list(request, params, State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body_text = body_to_string(response.into_body()).await;
        let lines: Vec<&str> = body_text.lines().collect();

        assert!(lines.iter().all(|line| line.starts_with("test/")));
    }

    #[tokio::test]
    async fn test_handle_list_empty() {
        let state = Arc::new(AppState::default());
        let path = "/?limit=10&offset=0".to_string();

        let request = Request::builder()
            .method("LIST")
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: Some(10),
            offset: Some(0),
        });

        let response = handle_list(request, params, State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        let body_text = body_to_string(response.into_body()).await;
        assert_eq!(body_text.to_string(), "");
    }

    #[tokio::test]
    async fn test_handle_list_with_pagination() {
        let state = Arc::new(AppState::default());
        let path = "/test/?limit=2&offset=0".to_string();

        for i in 0..5 {
            let path = format!("test/{}", i);
            let segment = Arc::new(Segment::new());
            state.segments.insert(path.clone(), segment);
            state.segments_list.write().await.insert(path);
        }

        let request = Request::builder()
            .method("LIST")
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: Some(2),
            offset: Some(0),
        });

        let response = handle_list(request, params, State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        let body_text = body_to_string(response.into_body()).await;
        let lines: Vec<&str> = body_text.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn test_handle_list_offset() {
        let state = Arc::new(AppState::default());
        let path = "/test/?limit=10&offset=2".to_string();

        for i in 0..10 {
            let path = format!("test/{}", i);
            let segment = Arc::new(Segment::new());
            state.segments.insert(path.clone(), segment);
            state.segments_list.write().await.insert(path);
        }

        let request = Request::builder()
            .method("LIST")
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: Some(10),
            offset: Some(2),
        });

        let response = handle_list(request, params, State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        let body_text = body_to_string(response.into_body()).await;
        let lines: Vec<&str> = body_text.lines().collect();

        assert_eq!(lines.len(), 8);

        assert_eq!(lines[0], "test/2");
        assert_eq!(lines[7], "test/9");
    }

    #[tokio::test]
    async fn test_handle_any_list() {
        let state = Arc::new(AppState::default());
        let path = "/?limit=10".to_string();

        let request = Request::builder()
            .method("LIST")
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: Some(10),
            offset: None,
        });

        let response = handle_any(State(state), params, request)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_handle_any_method_not_allowed() {
        let state = Arc::new(AppState::default());
        let path = "/";

        let request = Request::builder()
            .method("INVALID")
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let params = Query(ListParams {
            limit: None,
            offset: None,
        });

        let response = handle_any(State(state), params, request)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
