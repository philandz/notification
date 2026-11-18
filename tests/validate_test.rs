//! Pure validation rules: no database needed.
use notification::manager::biz::{decode_cursor, encode_cursor};
use notification::manager::validate;
use notification::pb::service::notification::{
    BroadcastAudience, BroadcastInput, NotificationCategory, NotificationSeverity, PublishRequest,
};
use tonic::metadata::MetadataMap;
use tonic::Code;

fn publish_req() -> PublishRequest {
    PublishRequest {
        event_id: uuid::Uuid::new_v4().to_string(),
        source: "budget".into(),
        r#type: "debt.payment_due".into(),
        category: NotificationCategory::Reminder as i32,
        severity: NotificationSeverity::Info as i32,
        recipient_user_ids: vec!["u1".into(), "u2".into()],
        fallback_text: "Payment due".into(),
        params_json: r#"{"a":1}"#.into(),
        ..Default::default()
    }
}

fn broadcast_input() -> BroadcastInput {
    BroadcastInput {
        audience: BroadcastAudience::All as i32,
        severity: NotificationSeverity::Info as i32,
        title_vi: "Bảo trì".into(),
        body_vi: "Hệ thống bảo trì tối nay".into(),
        title_en: "Maintenance".into(),
        body_en: "Down for maintenance tonight".into(),
        ..Default::default()
    }
}

#[test]
fn links_must_be_relative_paths() {
    assert!(validate::link("").is_ok());
    assert!(validate::link("/budgets/1/debt").is_ok());
    for bad in [
        "https://evil.example",
        "//evil.example",
        "budgets/1",
        "/a\\b",
        "/a\nb",
    ] {
        assert_eq!(
            validate::link(bad).unwrap_err().code(),
            Code::InvalidArgument,
            "{bad}"
        );
    }
    assert!(validate::link(&format!("/{}", "a".repeat(512))).is_err());
}

#[test]
fn publish_accepts_a_good_request_and_normalises_recipients() {
    let mut req = publish_req();
    req.actor_user_id = "u2".into();
    req.recipient_user_ids = vec![
        "u1".into(),
        "u2".into(),
        "u1".into(),
        "".into(),
        "u3".into(),
    ];
    let v = validate::publish(&req).unwrap();
    assert_eq!(v.recipients, vec!["u1", "u3"]);
    assert_eq!(v.category, NotificationCategory::Reminder);
}

#[test]
fn publish_rejects_bad_requests() {
    type Mutation = Box<dyn Fn(&mut PublishRequest)>;
    let cases: Vec<(&str, Mutation)> = vec![
        ("empty event id", Box::new(|r| r.event_id.clear())),
        ("long event id", Box::new(|r| r.event_id = "x".repeat(37))),
        ("empty source", Box::new(|r| r.source.clear())),
        ("uppercase type", Box::new(|r| r.r#type = "Debt.Due".into())),
        ("empty type", Box::new(|r| r.r#type.clear())),
        ("unspecified category", Box::new(|r| r.category = 0)),
        (
            "system category",
            Box::new(|r| r.category = NotificationCategory::System as i32),
        ),
        ("no recipients", Box::new(|r| r.recipient_user_ids.clear())),
        (
            "only the actor",
            Box::new(|r| {
                r.actor_user_id = "u1".into();
                r.recipient_user_ids = vec!["u1".into()];
            }),
        ),
        (
            "too many recipients",
            Box::new(|r| r.recipient_user_ids = (0..501).map(|i| format!("u{i}")).collect()),
        ),
        (
            "blank fallback",
            Box::new(|r| r.fallback_text = "  ".into()),
        ),
        (
            "params not an object",
            Box::new(|r| r.params_json = "[1]".into()),
        ),
        ("params not json", Box::new(|r| r.params_json = "{".into())),
        (
            "params too large",
            Box::new(|r| r.params_json = format!("{{\"a\":\"{}\"}}", "x".repeat(9000))),
        ),
        ("absolute link", Box::new(|r| r.link = "https://x.y".into())),
        (
            "long dedup key",
            Box::new(|r| r.dedup_key = "k".repeat(192)),
        ),
    ];
    for (name, mutate) in cases {
        let mut req = publish_req();
        mutate(&mut req);
        let err = validate::publish(&req).expect_err(name);
        assert_eq!(err.code(), Code::InvalidArgument, "{name}");
    }
}

#[test]
fn publish_defaults_unspecified_severity_to_info() {
    let mut req = publish_req();
    req.severity = 0;
    assert_eq!(
        validate::publish(&req).unwrap().severity,
        NotificationSeverity::Info
    );
}

#[test]
fn broadcast_input_rules() {
    let now = 1_000;
    assert!(validate::broadcast_input(&broadcast_input(), now).is_ok());

    let mut users = broadcast_input();
    users.audience = BroadcastAudience::Users as i32;
    assert!(
        validate::broadcast_input(&users, now).is_err(),
        "USERS needs ids"
    );
    users.audience_user_ids = vec!["a".into(), "b".into(), "a".into()];
    assert_eq!(
        validate::broadcast_input(&users, now).unwrap().user_ids,
        vec!["a", "b"]
    );

    let mut orgs = broadcast_input();
    orgs.audience = BroadcastAudience::Orgs as i32;
    assert!(
        validate::broadcast_input(&orgs, now).is_err(),
        "ORGS not available yet"
    );

    let mut unspecified = broadcast_input();
    unspecified.audience = 0;
    assert!(validate::broadcast_input(&unspecified, now).is_err());

    for mutate in [
        (|i: &mut BroadcastInput| i.title_vi = " ".into()) as fn(&mut BroadcastInput),
        |i| i.body_en.clear(),
        |i| i.title_en = "x".repeat(201),
        |i| i.link = "http://x".into(),
        |i| i.expires_at = 500, // before "now"
        |i| i.publish_at = -1,
    ] {
        let mut input = broadcast_input();
        mutate(&mut input);
        assert!(validate::broadcast_input(&input, now).is_err());
    }

    let mut scheduled = broadcast_input();
    scheduled.publish_at = 2_000;
    scheduled.expires_at = 2_000;
    assert!(
        validate::broadcast_input(&scheduled, now).is_err(),
        "expiry must follow publish"
    );
    scheduled.expires_at = 2_001;
    assert!(validate::broadcast_input(&scheduled, now).is_ok());
}

#[test]
fn caller_identity_from_metadata() {
    let mut meta = MetadataMap::new();
    assert_eq!(
        validate::normal_user(&meta).unwrap_err().code(),
        Code::Unauthenticated
    );
    meta.insert("x-user-id", "u1".parse().unwrap());
    assert_eq!(validate::normal_user(&meta).unwrap(), "u1");
    assert_eq!(
        validate::super_admin(&meta).unwrap_err().code(),
        Code::PermissionDenied
    );

    meta.insert("x-user-type", "super_admin".parse().unwrap());
    assert_eq!(validate::super_admin(&meta).unwrap(), "u1");
    assert_eq!(
        validate::normal_user(&meta).unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[test]
fn internal_token_is_required_and_exact() {
    let mut meta = MetadataMap::new();
    assert!(validate::internal_token(&meta, "secret").is_err());
    meta.insert("x-internal-token", "wrong".parse().unwrap());
    assert!(validate::internal_token(&meta, "secret").is_err());
    meta.insert("x-internal-token", "secret".parse().unwrap());
    assert!(validate::internal_token(&meta, "secret").is_ok());
    // An empty server-side secret never matches, even an empty header.
    assert!(validate::internal_token(&meta, "").is_err());
}

#[test]
fn cursors_round_trip_and_reject_garbage() {
    let c = encode_cursor(1234, "abc-def");
    assert_eq!(
        decode_cursor(&c).unwrap(),
        Some((1234, "abc-def".to_string()))
    );
    assert_eq!(decode_cursor("").unwrap(), None);
    for bad in ["nope", "x:id", "12:", &format!("1:{}", "a".repeat(37))] {
        assert!(decode_cursor(bad).is_err(), "{bad}");
    }
}

#[test]
fn page_limit_defaults_and_caps() {
    assert_eq!(validate::page_limit(0), 20);
    assert_eq!(validate::page_limit(-5), 20);
    assert_eq!(validate::page_limit(7), 7);
    assert_eq!(validate::page_limit(10_000), 100);
}
