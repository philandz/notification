use axum::{routing::get, Json, Router};
use notification::config::{NotificationServiceConfig, NotificationTuning};
use notification::handler::NotificationHandler;
use notification::manager::biz::NotificationBiz;
use notification::manager::email::{EmailDispatcher, EmailSettings, IdentityContacts};
use notification::manager::repository::NotificationRepository;
use notification::pb::service::notification::notification_service_server::NotificationServiceServer;
use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let rust_log = std::env::var("RUST_LOG").ok();
    philand_logging::init(
        "notification",
        rust_log.as_deref().or(Some("notification=debug")),
    );

    let app_info = philand_application::from_env_with_prefix("NOTIFICATION_APP");
    tracing::info!("starting {}", app_info.user_agent());

    let config = NotificationServiceConfig::from_env()
        .map_err(|e| anyhow::anyhow!("invalid configuration: {e}"))?;

    // Consul: register and ignore failures, like the other services.
    if let Err(e) = config.register_consul().await {
        tracing::warn!("Consul registration failed: {e}. Continuing without Consul.");
    }

    let repo = NotificationRepository::new(&config.database_url, "./migrations")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to init repository: {e}"))?;
    tracing::info!("Storage initialized");

    let email = EmailSettings::from_env(&config.internal_token);
    let email_repo = NotificationRepository::from_pool(repo.pool().clone());
    let biz = Arc::new(
        NotificationBiz::new(
            repo,
            config.internal_token.clone(),
            NotificationTuning::from_env(),
        )
        .with_email(email.clone()),
    );
    biz.spawn_cleanup();
    if email.active {
        let mailer = Arc::new(philand_notify::ResendMailer::new(
            philand_notify::ApiKeySource::Env(email.api_key.clone()),
        ));
        let contacts = Arc::new(IdentityContacts::connect_lazy(
            &email.identity_grpc_url,
            &config.internal_token,
        )?);
        Arc::new(EmailDispatcher::new(email_repo, email, mailer, contacts)).spawn();
        tracing::info!("email channel enabled");
    }
    let grpc_handler = NotificationHandler::new(biz);

    let grpc_addr: SocketAddr = format!("{}:{}", config.grpc_host, config.grpc_port).parse()?;
    let grpc_server = tonic::transport::Server::builder()
        .add_service(NotificationServiceServer::new(grpc_handler))
        .serve(grpc_addr);
    tracing::info!("gRPC server listening on {}", grpc_addr);

    let http_addr: SocketAddr = format!("{}:{}", config.http_host, config.http_port).parse()?;
    let http_app = Router::new().route("/health", get(health_check));
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!("HTTP server listening on {}", http_addr);

    tokio::select! {
        res = grpc_server => { if let Err(e) = res { tracing::error!("gRPC error: {}", e); } }
        res = axum::serve(http_listener, http_app) => { if let Err(e) = res { tracing::error!("HTTP error: {}", e); } }
    }
    Ok(())
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "service": "notification" }))
}
