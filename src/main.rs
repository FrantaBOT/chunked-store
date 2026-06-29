use arc_swap::ArcSwapOption;
use async_stream::try_stream;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Path, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
};
use dashmap::DashMap;
use futures_util::Stream;
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{net::TcpListener, sync::Notify};
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};

struct AppState {
    segments: DashMap<String, Arc<Segment>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            segments: DashMap::new(),
        }
    }
}

struct Chunk {
    data: Bytes,
    next: Arc<ArcSwapOption<Chunk>>,
}

struct Segment {
    start_chunk: Arc<ArcSwapOption<Chunk>>,
    current_chunk: Arc<ArcSwapOption<Chunk>>,
    notify: Arc<Notify>,
    closed: AtomicBool,
}

impl Segment {
    pub fn new() -> Self {
        Self {
            start_chunk: Arc::new(ArcSwapOption::new(None)),
            current_chunk: Arc::new(ArcSwapOption::new(None)),
            notify: Arc::new(Notify::new()),
            closed: AtomicBool::new(false),
        }
    }

    pub fn add_chunk(&self, data: Bytes) {
        let chunk = Arc::new(Chunk {
            data,
            next: Arc::new(ArcSwapOption::new(None)),
        });

        match self.current_chunk.load_full() {
            Some(last) => {
                last.next.store(Some(Arc::clone(&chunk)));
                self.current_chunk.store(Some(Arc::clone(&chunk)));
            }
            None => {
                self.start_chunk.store(Some(Arc::clone(&chunk)));
                self.current_chunk.store(Some(Arc::clone(&chunk)));
            }
        }

        self.notify.notify_waiters();
    }

    pub fn stream(self: Arc<Self>) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
        try_stream! {
            let mut current_chunk = Arc::clone(&self.start_chunk);

            loop {
                let closed = self.closed.load(Ordering::Acquire);

                if let Some(chunk) = current_chunk.load_full() {
                    current_chunk = Arc::clone(&chunk.next);

                    yield chunk.data.clone();
                    continue
                }

                if closed {
                    break
                }

                self.notify.notified().await;
            }
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

struct SegmentGuard(Arc<Segment>);

impl Drop for SegmentGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

async fn handle_put(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
    body: Request,
) -> impl IntoResponse {
    println!("PUT: {}", path);

    let segment = Arc::new(Segment::new());
    let _segment_guard = SegmentGuard(Arc::clone(&segment));

    state.segments.insert(path.clone(), Arc::clone(&segment));

    let mut body = body.into_body().into_data_stream();

    loop {
        match body.next().await {
            Some(Ok(chunk)) => segment.add_chunk(chunk),
            Some(Err(error)) => return (StatusCode::BAD_REQUEST, error.to_string()),
            None => return (StatusCode::OK, "ok".into()),
        }
    }
}

async fn handle_get(
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

async fn handle_delete(Path(path): Path<String>, State(_state): State<Arc<AppState>>) -> String {
    println!("DELETE: {}", path);
    format!("DELETE: {}", path)
}

#[tokio::main]
async fn main() {
    let address = env::var("ADDRESS").unwrap_or("127.0.0.1:8080".into());

    let state = Arc::new(AppState::default());

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
