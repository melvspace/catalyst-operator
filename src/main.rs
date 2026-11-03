use std::{env, sync::Arc, time::Duration};

use anyhow::Context as _;
use axum::{Router, http::StatusCode, routing::get};
use catalyst_operator::{
    api::{Nest, Server, Template},
    catalyst::Catalyst,
    controller::{self, Context},
    runtime,
};
use kube::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "catalyst_operator=info,kube=warn".into()),
        )
        .json()
        .init();
    let client = Client::try_default().await?;
    let namespace = env::var("POD_NAMESPACE")
        .unwrap_or_else(|_| "catalyst-operator".into());
    let identity = env::var("POD_UID").unwrap_or_else(
        |_| {
            format!(
                "local-{}",
                std::process::id()
            )
        },
    );
    let leader = Arc::new(runtime::Leadership::default());
    let poll: u64 = env::var("POLL_SECONDS")
        .unwrap_or_else(|_| "30".into())
        .parse()
        .context("Invalid POLL_SECONDS")?;
    anyhow::ensure!(
        poll > 0,
        "POLL_SECONDS must be positive"
    );
    let context = Arc::new(
        Context {
            client: client.clone(),
            catalyst: Catalyst::new(
                &env::var("CATALYST_URL").context(
                    "Set CATALYST_URL to the API base, including /api",
                )?,
                env::var("CATALYST_API_KEY")
                    .context("Set CATALYST_API_KEY from a Kubernetes Secret")?,
            )?,
            poll: Duration::from_secs(poll),
            leader: leader.clone(),
            max_recovery: env::var("MAX_RECOVERY_ATTEMPTS")
                .unwrap_or_else(|_| "5".into())
                .parse()?,
            operation_timeout: env::var("OPERATION_TIMEOUT_SECONDS")
                .unwrap_or_else(|_| "900".into())
                .parse()?,
        },
    );
    let ready = leader.clone();
    let app = Router::new()
        .route(
            "/healthz",
            get(|| async { StatusCode::OK }),
        )
        .route(
            "/readyz",
            get(
                move || {
                    let ready = ready.clone();

                    async move {
                        if ready.is_leader() {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        }
                    }
                },
            ),
        );
    let listener = tokio::net::TcpListener::bind(
        env::var("HEALTH_BIND_ADDRESS")
            .unwrap_or_else(|_| "0.0.0.0:8080".into()),
    )
    .await?;
    let lease = tokio::spawn(
        runtime::leadership(
            client, namespace, identity, leader,
        ),
    );
    let health = tokio::spawn(
        async move {
            axum::serve(
                listener, app,
            )
            .await
        },
    );
    tracing::info!("Starting Catalyst Server, Template and Nest controllers");
    tokio::select! {
        _ = controller::run::<Server>(context.clone()) => {},
        _ = controller::run::<Template>(context.clone()) => {},
        _ = controller::run::<Nest>(context) => {},
        _ = tokio::signal::ctrl_c() => {},
    }
    lease.abort();
    health.abort();

    Ok(())
}
