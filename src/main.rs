mod app_state;
mod handlers;
mod segment;

use axum::{
    Router,
    routing::{any, delete, get, put},
};
use axum_metrics::MetricLayer;
use hyper::server::conn::http1;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

use app_state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    info!("application starting");

    let metrics_address = env::var("METRICS_ADDRESS")
        .ok()
        .and_then(|s| s.parse::<SocketAddr>().ok())
        .unwrap_or("127.0.0.1:9000".parse().unwrap());

    PrometheusBuilder::new()
        .with_http_listener(metrics_address)
        .install()
        .unwrap();

    info!("metrics listening on {}", metrics_address.to_string());

    let address = env::var("ADDRESS").unwrap_or("127.0.0.1:8080".into());

    let state = Arc::new(AppState::default());

    let app = Router::new()
        .route("/{*path}", put(handlers::handle_put))
        .route("/{*path}", get(handlers::handle_get))
        .route("/{*path}", delete(handlers::handle_delete))
        .route("/{*path}", any(handlers::handle_any))
        .with_state(state)
        .layer(CorsLayer::new().allow_origin(Any))
        .layer(MetricLayer::default());

    let listener = TcpListener::bind(address).await?;

    info!("app listening on {}", listener.local_addr().unwrap());

    loop {
        let (stream, _) = listener.accept().await?;
        let app = app.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);

            let mut builder = http1::Builder::new();
            builder.half_close(true);

            let service = TowerToHyperService::new(app);

            if let Err(err) = builder.serve_connection(io, service).await {
                debug!("failed to serve connection: {err:#}");
            }
        });
    }
}
