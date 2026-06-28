use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Path, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
};
use futures_util::stream;
use std::{collections::HashMap, env, sync::Arc, sync::RwLock};
use tokio::{
    net::TcpListener,
    sync::broadcast::{self, Receiver, Sender},
};
use tokio_stream::{
    StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};
use tower_http::cors::{Any, CorsLayer};

struct AppState {
    segments: RwLock<HashMap<String, Arc<RwLock<Segment>>>>,
}

struct Segment {
    tx: Option<Sender<Bytes>>,
    chunks: Vec<Bytes>,
}

impl Segment {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);

        Self {
            tx: Some(tx),
            chunks: Vec::new(),
        }
    }

    pub fn add_chunk(&mut self, chunk: Bytes) {
        if let Some(tx) = &self.tx {
            tx.send(chunk.clone()).ok();

            self.chunks.push(chunk);
        }
    }

    pub fn get_chunks(&self) -> (Vec<Bytes>, Option<Receiver<Bytes>>) {
        (self.chunks.clone(), self.tx.as_ref().map(|r| r.subscribe()))
    }

    pub fn close(&mut self) {
        self.tx.take();
    }
}

struct SegmentGuard {
    segment: Arc<RwLock<Segment>>,
}

impl SegmentGuard {
    pub fn new(segment: Arc<RwLock<Segment>>) -> Self {
        Self { segment }
    }
}

impl Drop for SegmentGuard {
    fn drop(&mut self) {
        if let Ok(mut segment) = self.segment.write() {
            segment.close();
        }
    }
}

async fn handle_put(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
    body: Request,
) -> impl IntoResponse {
    println!("PUT: {}", path);

    let segment = {
        let mut segments = match state.segments.write() {
            Ok(segments) => segments,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                );
            }
        };

        let segment = Arc::new(RwLock::new(Segment::new()));

        segments.insert(path.clone(), Arc::clone(&segment));

        segment
    };

    let _segment_guard = SegmentGuard::new(Arc::clone(&segment));

    let mut body = body.into_body().into_data_stream();
    while let Some(chunk) = body.next().await {
        let chunk: axum::body::Bytes = match chunk {
            Ok(chunk) => chunk,
            Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()),
        };

        match segment.write() {
            Ok(mut segment) => {
                segment.add_chunk(chunk);
            }
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                );
            }
        };
    }

    (StatusCode::OK, "ok".into())
}

async fn handle_get(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("GET: {}", path);

    let segment = {
        let segments = match state.segments.read() {
            Ok(segments) => segments,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
                    .into_response();
            }
        };

        match segments.get(&path) {
            Some(segment) => Arc::clone(segment),
            None => return (StatusCode::NOT_FOUND, "not found".to_string()).into_response(),
        }
    };

    let (chunks, rx) = {
        let segment = match segment.read() {
            Ok(segment) => segment,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
                    .into_response();
            }
        };

        segment.get_chunks()
    };

    let chunks = stream::iter(
        chunks
            .into_iter()
            .map(Ok::<Bytes, BroadcastStreamRecvError>),
    );

    let body_stream = match rx {
        Some(rx) => {
            let chunked_stream = BroadcastStream::new(rx);

            Body::from_stream(chunks.chain(chunked_stream))
        }
        None => Body::from_stream(chunks),
    };

    (StatusCode::OK, body_stream).into_response()
}

async fn handle_delete(Path(path): Path<String>, State(_state): State<Arc<AppState>>) -> String {
    println!("DELETE: {}", path);
    format!("DELETE: {}", path)
}

#[tokio::main]
async fn main() {
    let address = env::var("ADDRESS").unwrap_or("127.0.0.1:8080".into());

    let state = Arc::new(AppState {
        segments: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/{*path}", put(handle_put))
        .route("/{*path}", get(handle_get))
        .route("/{*path}", delete(handle_delete))
        .with_state(state)
        .layer(CorsLayer::new().allow_origin(Any));

    let listener = TcpListener::bind(address).await.unwrap();

    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
