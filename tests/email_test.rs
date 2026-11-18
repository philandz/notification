//! Email channel against a real MySQL (provider and identity are faked).
//! `DATABASE_URL=mysql://… cargo test --test email_test -- --include-ignored`

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use notification::config::NotificationTuning;
use notification::manager::biz::NotificationBiz;
use notification::manager::email::{token, Contact, ContactSource, EmailDispatcher, EmailSettings};
use notification::manager::repository::NotificationRepository;
use notification::pb::service::notification::{
    NotificationCategory, NotificationChannel, NotificationSeverity, PreferenceCell,
    PublishRequest, UpdatePreferencesRequest,
};
use philand_notify::{MailMessage, MailReceipt, Mailer, MailerError};

const SECRET: &str = "unsub-secret";
const NOW: i64 = 1_900_000_000;

fn uid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn settings(cap: i64) -> EmailSettings {
    let mut s = EmailSettings::disabled();
    s.active = true;
    s.api_key = "re_test".into();
    s.from = "Philandz <noreply@example.com>".into();
    s.app_base_url = "https://app.example".into();
    s.api_base_url = "https://app.example".into();
    s.support_email = "support@example.com".into();
    s.daily_cap = cap;
    s.batch = 1000;
    s.unsubscribe_secret = SECRET.into();
    s
}

async fn repo() -> NotificationRepository {
    // Migrations are not safe to run from several tests at once.
    static INIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT.lock().await;
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    NotificationRepository::new(&url, "./migrations")
        .await
        .expect("repo")
}

async fn biz(cap: i64) -> NotificationBiz {
    NotificationBiz::new(repo().await, "tok".into(), NotificationTuning::default())
        .with_email(settings(cap))
}

fn event(user: &str, category: NotificationCategory) -> PublishRequest {
    PublishRequest {
        event_id: uid(),
        source: "budget".into(),
        r#type: "debt.payment_due".into(),
        category: category as i32,
        severity: NotificationSeverity::Info as i32,
        recipient_user_ids: vec![user.to_string()],
        fallback_text: "Payment due".into(),
        params_json: r#"{"debt_name":"Loan","amount":1500000,"days":3}"#.into(),
        link: "/budgets/b1".into(),
        ..Default::default()
    }
}

fn email_cell(category: NotificationCategory, enabled: bool) -> UpdatePreferencesRequest {
    UpdatePreferencesRequest {
        cells: vec![PreferenceCell {
            category: category as i32,
            channel: NotificationChannel::Email as i32,
            enabled,
            locked: false,
        }],
    }
}

async fn queued(user: &str) -> i64 {
    sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM notification_deliveries WHERE user_id = ?")
        .bind(user)
        .fetch_one(repo().await.pool())
        .await
        .unwrap()
        .0
}

#[derive(Default)]
struct FakeMailer {
    sent: Mutex<Vec<MailMessage>>,
}

#[async_trait]
impl Mailer for FakeMailer {
    fn provider_name(&self) -> &'static str {
        "fake"
    }
    async fn send(&self, msg: MailMessage) -> Result<MailReceipt, MailerError> {
        self.sent.lock().unwrap().push(msg);
        Ok(MailReceipt {
            provider: "fake".into(),
            message_id: "msg-1".into(),
        })
    }
}

struct FakeContacts(Vec<(String, Contact)>);

#[async_trait]
impl ContactSource for FakeContacts {
    async fn contact(&self, user_id: &str) -> Result<Option<Contact>, String> {
        Ok(self
            .0
            .iter()
            .find(|(u, _)| u == user_id)
            .map(|(_, c)| c.clone()))
    }
}

fn contact(email: &str, locale: &str) -> Contact {
    Contact {
        email: email.into(),
        display_name: "Lan".into(),
        locale: locale.into(),
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn email_is_opt_in_except_security() {
    let biz = biz(3).await;
    let user = uid();

    // Reminder: off by default, nothing queued.
    biz.publish_at(event(&user, NotificationCategory::Reminder), NOW)
        .await
        .unwrap();
    assert_eq!(queued(&user).await, 0);

    // Opt in: queued once, and re-publishing the same event does not queue it twice.
    biz.update_preferences_at(&user, email_cell(NotificationCategory::Reminder, true), NOW)
        .await
        .unwrap();
    let ev = event(&user, NotificationCategory::Reminder);
    biz.publish_at(ev.clone(), NOW).await.unwrap();
    biz.publish_at(ev, NOW).await.unwrap();
    assert_eq!(queued(&user).await, 1);

    // Activity stays off, security is on without any setting.
    biz.publish_at(event(&user, NotificationCategory::Activity), NOW)
        .await
        .unwrap();
    assert_eq!(queued(&user).await, 1);
    let other = uid();
    biz.publish_at(event(&other, NotificationCategory::Security), NOW)
        .await
        .unwrap();
    assert_eq!(queued(&other).await, 1);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn email_does_not_depend_on_the_inbox_setting() {
    let biz = biz(3).await;
    let user = uid();
    biz.update_preferences_at(&user, email_cell(NotificationCategory::Reminder, true), NOW)
        .await
        .unwrap();
    let mut off = email_cell(NotificationCategory::Reminder, true);
    off.cells[0].channel = NotificationChannel::InApp as i32;
    off.cells[0].enabled = false;
    biz.update_preferences_at(&user, off, NOW).await.unwrap();

    biz.publish_at(event(&user, NotificationCategory::Reminder), NOW)
        .await
        .unwrap();
    assert_eq!(queued(&user).await, 1);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn dispatcher_sends_localised_mail_and_enforces_the_daily_cap() {
    let biz = biz(1).await;
    let user = uid();
    biz.update_preferences_at(&user, email_cell(NotificationCategory::Reminder, true), NOW)
        .await
        .unwrap();
    biz.publish_at(event(&user, NotificationCategory::Reminder), NOW)
        .await
        .unwrap();
    biz.publish_at(event(&user, NotificationCategory::Reminder), NOW)
        .await
        .unwrap();
    assert_eq!(queued(&user).await, 2);

    let mailer = Arc::new(FakeMailer::default());
    let to = format!("{user}@example.com");
    let contacts = Arc::new(FakeContacts(vec![(user.clone(), contact(&to, "vi"))]));
    let d = EmailDispatcher::new(repo().await, settings(1), mailer.clone(), contacts);
    d.run_once(NOW).await.unwrap();

    let mine: Vec<MailMessage> = mailer
        .sent
        .lock()
        .unwrap()
        .iter()
        .filter(|m| m.to == to)
        .cloned()
        .collect();
    assert_eq!(mine.len(), 1, "cap of 1 per day");
    assert_eq!(mine[0].subject, "Khoản nợ đến hạn sau 3 ngày");
    assert!(mine[0].html.contains("1.500.000 ₫"));
    assert!(mine[0].html.contains("https://app.example/vi/budgets/b1"));
    assert!(mine[0]
        .text
        .contains("/api/notification/unsubscribe?token="));

    let statuses: Vec<(String,)> = sqlx::query_as(
        "SELECT status FROM notification_deliveries WHERE user_id = ? ORDER BY status",
    )
    .bind(&user)
    .fetch_all(repo().await.pool())
    .await
    .unwrap();
    assert_eq!(
        statuses.into_iter().map(|s| s.0).collect::<Vec<_>>(),
        vec!["sent", "skipped"]
    );

    // A second pass has nothing left to do for this user.
    d.run_once(NOW + 1).await.unwrap();
    assert_eq!(
        mailer
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.to == to)
            .count(),
        1
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn unsubscribe_link_turns_the_category_off() {
    let biz = biz(3).await;
    let user = uid();
    biz.update_preferences_at(&user, email_cell(NotificationCategory::Reminder, true), NOW)
        .await
        .unwrap();

    let t = token::sign(SECRET, &user, "reminder", NOW);
    assert_eq!(
        biz.unsubscribe_email_at(&t, NOW + 10).await.unwrap(),
        "reminder"
    );
    biz.publish_at(event(&user, NotificationCategory::Reminder), NOW)
        .await
        .unwrap();
    assert_eq!(queued(&user).await, 0);

    // Bad token / wrong secret / unknown category are all rejected.
    assert!(biz.unsubscribe_email_at("nope", NOW).await.is_err());
    assert!(biz
        .unsubscribe_email_at(&token::sign("other", &user, "reminder", NOW), NOW)
        .await
        .is_err());
    assert!(biz
        .unsubscribe_email_at(&token::sign(SECRET, &user, "bogus", NOW), NOW)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn email_cells_only_appear_when_the_channel_is_active() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let off = NotificationBiz::new(
        NotificationRepository::new(&url, "./migrations")
            .await
            .unwrap(),
        "tok".into(),
        NotificationTuning::default(),
    );
    let p = off.preferences(&uid()).await.unwrap();
    assert!(p
        .cells
        .iter()
        .all(|c| c.channel == NotificationChannel::InApp as i32));
    assert!(off
        .update_preferences_at(
            &uid(),
            email_cell(NotificationCategory::Reminder, true),
            NOW
        )
        .await
        .is_err());

    let on = biz(3).await;
    let p = on.preferences(&uid()).await.unwrap();
    let email: Vec<_> = p
        .cells
        .iter()
        .filter(|c| c.channel == NotificationChannel::Email as i32)
        .collect();
    assert_eq!(email.len(), 5);
    // Only security is on by default.
    assert_eq!(email.iter().filter(|c| c.enabled).count(), 1);
}
