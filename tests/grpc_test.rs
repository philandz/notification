//! End-to-end over a real gRPC connection: wiring of metadata, internal token and admin
//! guard. DB-backed: `DATABASE_URL=mysql://… cargo test --test grpc_test -- --include-ignored`
use std::sync::Arc;

use notification::config::NotificationTuning;
use notification::handler::NotificationHandler;
use notification::manager::biz::NotificationBiz;
use notification::manager::repository::NotificationRepository;
use notification::pb::service::notification::notification_service_client::NotificationServiceClient;
use notification::pb::service::notification::notification_service_server::NotificationServiceServer;
use notification::pb::service::notification::{
    AdminListBroadcastsRequest, GetUnreadCountRequest, ListInboxRequest, NotificationCategory,
    NotificationSeverity, PublishRequest,
};
use tonic::metadata::MetadataValue;
use tonic::{transport::Channel, Code, Request};

const TOKEN: &str = "internal-secret";

async fn start() -> NotificationServiceClient<Channel> {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let repo = NotificationRepository::new(&url, "./migrations")
        .await
        .unwrap();
    let biz = Arc::new(NotificationBiz::new(
        repo,
        TOKEN.into(),
        NotificationTuning::default(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(NotificationServiceServer::new(NotificationHandler::new(
                biz,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    NotificationServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

fn with<T>(msg: T, meta: &[(&'static str, &str)]) -> Request<T> {
    let mut req = Request::new(msg);
    for (k, v) in meta {
        req.metadata_mut()
            .insert(*k, MetadataValue::try_from(*v).unwrap());
    }
    req
}

fn publish_to(user: &str) -> PublishRequest {
    PublishRequest {
        event_id: uuid::Uuid::new_v4().to_string(),
        source: "budget".into(),
        r#type: "debt.payment_due".into(),
        category: NotificationCategory::Reminder as i32,
        severity: NotificationSeverity::Info as i32,
        recipient_user_ids: vec![user.into()],
        fallback_text: "Payment due".into(),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn publish_needs_the_internal_token() {
    let mut c = start().await;
    let user = uuid::Uuid::new_v4().to_string();

    let err = c.publish(with(publish_to(&user), &[])).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    let err = c
        .publish(with(publish_to(&user), &[("x-internal-token", "wrong")]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    // A user JWT's identity headers are not a substitute for the token.
    let err = c
        .publish(with(publish_to(&user), &[("x-user-id", &user)]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let ok = c
        .publish(with(publish_to(&user), &[("x-internal-token", TOKEN)]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ok.created, 1);

    let unread = c
        .get_unread_count(with(GetUnreadCountRequest {}, &[("x-user-id", &user)]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unread.personal, 1);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn inbox_requires_a_user_and_admin_requires_super_admin() {
    let mut c = start().await;
    let user = uuid::Uuid::new_v4().to_string();

    let err = c
        .list_inbox(with(ListInboxRequest::default(), &[]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    // Super admins have no personal inbox.
    let err = c
        .list_inbox(with(
            ListInboxRequest::default(),
            &[("x-user-id", &user), ("x-user-type", "super_admin")],
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    // Normal users cannot use admin RPCs.
    let err = c
        .admin_list_broadcasts(with(
            AdminListBroadcastsRequest::default(),
            &[("x-user-id", &user), ("x-user-type", "normal")],
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    let ok = c
        .admin_list_broadcasts(with(
            AdminListBroadcastsRequest::default(),
            &[("x-user-id", &user), ("x-user-type", "super_admin")],
        ))
        .await;
    assert!(ok.is_ok());
}
