use axum::{extract::Extension, middleware, routing::get, Router};
use chrono::Local;
use clap::Parser;
use env_logger::{Builder, Target};
use log::LevelFilter;
use std::io::Write;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

mod error;
mod handlers;
mod https;
mod metrics;
mod state;

use crate::metrics::track_metrics;
use handlers::{handler_404, health, help, metrics, root};
use https::create_https_client;
use state::State;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Port to listen on
    #[arg(
        short,
        long,
        default_value_t = 8080,
        env = "ELASTIC_BILLING_EXPORTER_LISTEN_PORT"
    )]
    port: u16,

    /// Default global timeout
    #[arg(
        short,
        long,
        default_value_t = 60,
        env = "ELASTIC_BILLING_EXPORTER_TIMEOUT"
    )]
    timeout: u64,

    /// Elastic Reverse Proxy
    #[arg(short, long, env = "ELASTIC_BILLING_EXPORTER_REVERSE_PROXY")]
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // Initialize log Builder
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{{\"date\": \"{}\", \"level\": \"{}\", \"log\": {}}}",
                Local::now().format("%Y-%m-%dT%H:%M:%S:%f"),
                record.level(),
                record.args()
            )
        })
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    // Create state for axum
    let state = State::new(args.clone()).await?;

    // Create prometheus handle
    // let recorder_handle = setup_metrics_recorder();

    // These should be authenticated
    let base = Router::new().route("/", get(root));

    // These should NOT be authenticated
    let standard = Router::new()
        .route("/health", get(health))
        .route("/help", get(help))
        .route("/metrics", get(metrics));

    let app = Router::new()
        .merge(base)
        .merge(standard)
        .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()))
        .route_layer(middleware::from_fn(track_metrics))
        .fallback(handler_404)
        .layer(Extension(state));

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    let listener = TcpListener::bind(addr).await.unwrap();

    log::info!("Listening on {}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
