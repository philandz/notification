//! Email channel (Phase 4). Email is opt-in per category (see `default_enabled`) and costs money
//! per message, so there is a master switch, a per-user daily cap and a bounded retry budget.

pub mod templates;
pub mod token;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use philand_notify::{MailMessage, Mailer, MailerError};
use tonic::transport::Channel;

use crate::converters::DbDelivery;
use crate::manager::repository::NotificationRepository;
use crate::pb::service::identity::identity_service_client::IdentityServiceClient;
use crate::pb::service::identity::GetUserContactsRequest;

/// Security email is time-critical and exempt from the daily cap.
const CAP_EXEMPT_CATEGORY: &str = "security";
/// How long a claimed delivery is invisible to other workers/replicas.
const LEASE_SECS: i64 = 120;

#[derive(Debug, Clone)]
pub struct EmailSettings {
    /// `NOTIFY_EMAIL_ENABLED` and an API key are both present.
    pub active: bool,
    pub api_key: String,
    pub from: String,
    pub app_base_url: String,
    pub api_base_url: String,
    pub support_email: String,
    pub daily_cap: i64,
    pub max_attempts: i32,
    pub interval: Duration,
    pub batch: i64,
    pub unsubscribe_secret: String,
    pub identity_grpc_url: String,
    pub internal_token: String,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_num(name: &str, default: i64) -> i64 {
    env(name)
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

impl EmailSettings {
    /// Reads the environment. Email stays off unless `NOTIFY_EMAIL_ENABLED=true` **and**
    /// `RESEND_API_KEY` is set, so a missing key never grows a queue nobody drains.
    pub fn from_env(internal_token: &str) -> Self {
        let enabled = env("NOTIFY_EMAIL_ENABLED")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let api_key = env("RESEND_API_KEY").unwrap_or_default();
        if enabled && api_key.is_empty() {
            tracing::warn!(
                "NOTIFY_EMAIL_ENABLED is set but RESEND_API_KEY is missing: email stays off"
            );
        }
        let app_base_url = env("APP_PUBLIC_BASE_URL")
            .unwrap_or_else(|| "http://localhost:3000".into())
            .trim_end_matches('/')
            .to_string();
        Self {
            active: enabled && !api_key.is_empty(),
            api_key,
            from: env("NOTIFY_MAIL_FROM_ADDRESS")
                .or_else(|| env("MAIL_FROM_ADDRESS"))
                .unwrap_or_else(|| "Philandz <noreply@philandz.com>".into()),
            api_base_url: env("NOTIFY_PUBLIC_API_BASE_URL")
                .unwrap_or_else(|| app_base_url.clone())
                .trim_end_matches('/')
                .to_string(),
            app_base_url,
            support_email: env("SUPPORT_EMAIL").unwrap_or_else(|| "support@philandz.com".into()),
            daily_cap: env_num("NOTIFY_EMAIL_DAILY_CAP", 3),
            max_attempts: env_num("NOTIFY_EMAIL_MAX_ATTEMPTS", 6) as i32,
            interval: Duration::from_secs(env_num("NOTIFY_EMAIL_INTERVAL_SECS", 10) as u64),
            batch: env_num("NOTIFY_EMAIL_BATCH_SIZE", 20),
            unsubscribe_secret: env("NOTIFY_UNSUBSCRIBE_SECRET")
                .unwrap_or_else(|| internal_token.to_string()),
            identity_grpc_url: env("IDENTITY_GRPC_URL")
                .unwrap_or_else(|| "http://127.0.0.1:50051".into()),
            internal_token: internal_token.to_string(),
        }
    }

    /// Settings with email switched off (used by tests and when unconfigured).
    pub fn disabled() -> Self {
        Self {
            active: false,
            api_key: String::new(),
            from: String::new(),
            app_base_url: "http://localhost:3000".into(),
            api_base_url: "http://localhost:3000".into(),
            support_email: String::new(),
            daily_cap: 3,
            max_attempts: 6,
            interval: Duration::from_secs(10),
            batch: 20,
            unsubscribe_secret: String::new(),
            identity_grpc_url: String::new(),
            internal_token: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contact {
    pub email: String,
    pub display_name: String,
    pub locale: String,
}

#[async_trait]
pub trait ContactSource: Send + Sync {
    /// `Ok(None)`: the user does not exist (any more). `Err`: try again later.
    async fn contact(&self, user_id: &str) -> Result<Option<Contact>, String>;
}

/// Looks contacts up through identity's internal `GetUserContacts` RPC.
pub struct IdentityContacts {
    client: IdentityServiceClient<Channel>,
    token: String,
}

impl IdentityContacts {
    pub fn connect_lazy(url: &str, token: &str) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(url.to_string())?.connect_lazy();
        Ok(Self {
            client: IdentityServiceClient::new(channel),
            token: token.to_string(),
        })
    }
}

#[async_trait]
impl ContactSource for IdentityContacts {
    async fn contact(&self, user_id: &str) -> Result<Option<Contact>, String> {
        let mut req = tonic::Request::new(GetUserContactsRequest {
            user_ids: vec![user_id.to_string()],
        });
        req.metadata_mut().insert(
            "x-internal-token",
            self.token
                .parse()
                .map_err(|_| "invalid internal token".to_string())?,
        );
        let resp = self
            .client
            .clone()
            .get_user_contacts(req)
            .await
            .map_err(|e| format!("identity: {}", e.message()))?;
        Ok(resp
            .into_inner()
            .contacts
            .into_iter()
            .find(|c| c.user_id == user_id)
            .map(|c| Contact {
                email: c.email,
                display_name: c.display_name,
                locale: c.locale,
            }))
    }
}

pub struct EmailDispatcher {
    repo: NotificationRepository,
    settings: EmailSettings,
    mailer: Arc<dyn Mailer>,
    contacts: Arc<dyn ContactSource>,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Sent(String),
    Skipped(String),
    Retry(String),
    Failed(String),
}

fn backoff_secs(attempts: i32) -> i64 {
    (60i64 << (2 * (attempts.clamp(1, 6) - 1))).min(6 * 3600)
}

impl EmailDispatcher {
    pub fn new(
        repo: NotificationRepository,
        settings: EmailSettings,
        mailer: Arc<dyn Mailer>,
        contacts: Arc<dyn ContactSource>,
    ) -> Self {
        Self {
            repo,
            settings,
            mailer,
            contacts,
        }
    }

    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(self.settings.interval);
            loop {
                tick.tick().await;
                let now = chrono::Utc::now().timestamp();
                if let Err(e) = self.run_once(now).await {
                    tracing::warn!("email worker pass failed: {e}");
                }
            }
        });
    }

    /// One pass over the due deliveries. Returns how many were handled.
    pub async fn run_once(&self, now: i64) -> anyhow::Result<usize> {
        let due = self.repo.due_deliveries(now, self.settings.batch).await?;
        let mut handled = 0;
        for d in due {
            // Another replica may have claimed it first.
            if !self.repo.claim_delivery(&d.id, now, LEASE_SECS).await? {
                continue;
            }
            let attempts = d.attempts + 1;
            let outcome = self.deliver(&d, now).await;
            match outcome {
                Outcome::Sent(id) => self.repo.mark_delivery_sent(&d.id, &id, now).await?,
                Outcome::Skipped(why) => {
                    self.repo
                        .finish_delivery(&d.id, "skipped", &why, now)
                        .await?
                }
                Outcome::Failed(why) => {
                    tracing::warn!("email {} failed permanently: {why}", d.event_type);
                    self.repo
                        .finish_delivery(&d.id, "failed", &why, now)
                        .await?
                }
                Outcome::Retry(why) if attempts >= self.settings.max_attempts => {
                    tracing::warn!(
                        "email {} gave up after {attempts} attempts: {why}",
                        d.event_type
                    );
                    self.repo
                        .finish_delivery(&d.id, "failed", &why, now)
                        .await?
                }
                Outcome::Retry(why) => {
                    self.repo
                        .retry_delivery(&d.id, &why, now + backoff_secs(attempts), now)
                        .await?
                }
            }
            handled += 1;
        }
        Ok(handled)
    }

    async fn deliver(&self, d: &DbDelivery, now: i64) -> Outcome {
        if d.category != CAP_EXEMPT_CATEGORY {
            match self
                .repo
                .sent_count_since(&d.user_id, now - 24 * 3600)
                .await
            {
                Ok(n) if n >= self.settings.daily_cap => {
                    return Outcome::Skipped("daily email cap reached".into())
                }
                Ok(_) => {}
                Err(e) => return Outcome::Retry(format!("cap check: {e}")),
            }
        }
        let contact = match self.contacts.contact(&d.user_id).await {
            Ok(Some(c)) => c,
            Ok(None) => return Outcome::Skipped("user not found".into()),
            Err(e) => return Outcome::Retry(e),
        };
        let locale = if contact.locale == "vi" { "vi" } else { "en" };
        let params: serde_json::Value =
            serde_json::from_str(&d.params_json).unwrap_or_else(|_| serde_json::json!({}));
        let link_url = d
            .link
            .as_deref()
            .filter(|l| l.starts_with('/'))
            .map(|l| format!("{}/{locale}{l}", self.settings.app_base_url));
        let settings_url = format!(
            "{}/{locale}/settings/notifications",
            self.settings.app_base_url
        );
        let unsubscribe_url = format!(
            "{}/api/notification/unsubscribe?token={}",
            self.settings.api_base_url,
            token::sign(
                &self.settings.unsubscribe_secret,
                &d.user_id,
                &d.category,
                now
            )
        );
        let mail = templates::render(&templates::Context {
            locale,
            display_name: &contact.display_name,
            event_type: &d.event_type,
            params: &params,
            fallback_text: &d.fallback_text,
            link_url: link_url.as_deref(),
            settings_url: &settings_url,
            unsubscribe_url: &unsubscribe_url,
            support_email: &self.settings.support_email,
        });
        let msg = MailMessage {
            to: contact.email,
            from: self.settings.from.clone(),
            subject: mail.subject,
            html: mail.html,
            text: mail.text,
            reply_to: None,
        };
        match self.mailer.send(msg).await {
            Ok(receipt) => Outcome::Sent(receipt.message_id),
            Err(MailerError::Rejected { status, body })
                if (400..500).contains(&status) && status != 429 =>
            {
                Outcome::Failed(format!("provider rejected ({status}): {body}"))
            }
            Err(e) => Outcome::Retry(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(backoff_secs(1), 60);
        assert_eq!(backoff_secs(2), 240);
        assert_eq!(backoff_secs(3), 960);
        assert_eq!(backoff_secs(6), 6 * 3600);
        assert_eq!(backoff_secs(50), 6 * 3600);
    }

    #[test]
    fn email_stays_off_without_key() {
        let s = EmailSettings::disabled();
        assert!(!s.active);
    }
}
