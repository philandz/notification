//! DB row ↔ proto conversions and the single source of truth for enum ↔ VARCHAR mapping
//! (see `infra/.ai/skills/02-backend-rust/proto-enum-types.md`).

use crate::pb::service::notification::{
    Banner, Broadcast, BroadcastAudience, BroadcastStatus, InboxItem, NotificationCategory,
    NotificationChannel, NotificationKind, NotificationSeverity,
};

// ---------------------------------------------------------------------------
// Enum ↔ DB string
// ---------------------------------------------------------------------------

pub fn category_to_db(c: NotificationCategory) -> &'static str {
    match c {
        NotificationCategory::Activity => "activity",
        NotificationCategory::Membership => "membership",
        NotificationCategory::Reminder => "reminder",
        NotificationCategory::Alert => "alert",
        NotificationCategory::Security => "security",
        NotificationCategory::System => "system",
        NotificationCategory::Unspecified => "activity",
    }
}

pub fn category_from_db(s: &str) -> NotificationCategory {
    match s {
        "activity" => NotificationCategory::Activity,
        "membership" => NotificationCategory::Membership,
        "reminder" => NotificationCategory::Reminder,
        "alert" => NotificationCategory::Alert,
        "security" => NotificationCategory::Security,
        "system" => NotificationCategory::System,
        _ => NotificationCategory::Unspecified,
    }
}

pub fn severity_to_db(s: NotificationSeverity) -> &'static str {
    match s {
        NotificationSeverity::Warning => "warning",
        NotificationSeverity::Critical => "critical",
        NotificationSeverity::Info | NotificationSeverity::Unspecified => "info",
    }
}

pub fn severity_from_db(s: &str) -> NotificationSeverity {
    match s {
        "warning" => NotificationSeverity::Warning,
        "critical" => NotificationSeverity::Critical,
        _ => NotificationSeverity::Info,
    }
}

pub fn channel_to_db(c: NotificationChannel) -> &'static str {
    match c {
        NotificationChannel::InApp | NotificationChannel::Unspecified => "in_app",
        NotificationChannel::Email => "email",
        NotificationChannel::Telegram => "telegram",
    }
}

pub fn channel_from_db(s: &str) -> NotificationChannel {
    match s {
        "in_app" => NotificationChannel::InApp,
        "email" => NotificationChannel::Email,
        "telegram" => NotificationChannel::Telegram,
        _ => NotificationChannel::Unspecified,
    }
}

pub fn audience_to_db(a: BroadcastAudience) -> &'static str {
    match a {
        BroadcastAudience::All | BroadcastAudience::Unspecified => "all",
        BroadcastAudience::Users => "users",
        BroadcastAudience::Orgs => "orgs",
    }
}

pub fn audience_from_db(s: &str) -> BroadcastAudience {
    match s {
        "all" => BroadcastAudience::All,
        "users" => BroadcastAudience::Users,
        "orgs" => BroadcastAudience::Orgs,
        _ => BroadcastAudience::Unspecified,
    }
}

pub fn status_to_db(s: BroadcastStatus) -> &'static str {
    match s {
        BroadcastStatus::Draft | BroadcastStatus::Unspecified => "draft",
        BroadcastStatus::Scheduled => "scheduled",
        BroadcastStatus::Published => "published",
        BroadcastStatus::Archived => "archived",
    }
}

pub fn status_from_db(s: &str) -> BroadcastStatus {
    match s {
        "draft" => BroadcastStatus::Draft,
        "scheduled" => BroadcastStatus::Scheduled,
        "published" => BroadcastStatus::Published,
        "archived" => BroadcastStatus::Archived,
        _ => BroadcastStatus::Unspecified,
    }
}

// ---------------------------------------------------------------------------
// Locale
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locale {
    Vi,
    En,
}

impl Locale {
    /// "en" (any case, with or without a region) → English; everything else → Vietnamese.
    pub fn parse(s: &str) -> Self {
        if s.trim().to_ascii_lowercase().starts_with("en") {
            Self::En
        } else {
            Self::Vi
        }
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbItem {
    pub id: String,
    pub recipient_user_id: String,
    pub org_id: Option<String>,
    pub budget_id: Option<String>,
    pub category: String,
    pub event_type: String,
    pub severity: String,
    pub actor_user_id: Option<String>,
    pub params_json: String,
    pub link: Option<String>,
    pub fallback_text: String,
    pub source_event_id: String,
    pub dedup_key: Option<String>,
    pub group_key: Option<String>,
    pub group_count: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub read_at: Option<i64>,
    pub archived_at: Option<i64>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbBroadcast {
    pub id: String,
    pub audience_kind: String,
    pub audience_orgs_json: Option<String>,
    pub severity: String,
    pub title_vi: String,
    pub body_vi: String,
    pub title_en: String,
    pub body_en: String,
    pub link: Option<String>,
    pub status: String,
    pub publish_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub show_banner: bool,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A broadcast as one user sees it (joined with that user's state row).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbBroadcastView {
    pub id: String,
    pub severity: String,
    pub title_vi: String,
    pub body_vi: String,
    pub title_en: String,
    pub body_en: String,
    pub link: Option<String>,
    pub show_banner: bool,
    pub publish_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub read_at: Option<i64>,
}

fn opt(s: &Option<String>) -> String {
    s.clone().unwrap_or_default()
}

impl DbItem {
    pub fn into_proto(self) -> InboxItem {
        InboxItem {
            id: self.id,
            kind: NotificationKind::Personal as i32,
            category: category_from_db(&self.category) as i32,
            severity: severity_from_db(&self.severity) as i32,
            r#type: self.event_type,
            params_json: self.params_json,
            title: String::new(),
            body: String::new(),
            fallback_text: self.fallback_text,
            link: opt(&self.link),
            actor_user_id: opt(&self.actor_user_id),
            org_id: opt(&self.org_id),
            budget_id: opt(&self.budget_id),
            group_count: self.group_count,
            created_at: self.created_at,
            updated_at: self.updated_at,
            read_at: self.read_at.unwrap_or(0),
        }
    }
}

impl DbBroadcastView {
    fn title(&self, locale: Locale) -> String {
        match locale {
            Locale::Vi => self.title_vi.clone(),
            Locale::En => self.title_en.clone(),
        }
    }

    fn body(&self, locale: Locale) -> String {
        match locale {
            Locale::Vi => self.body_vi.clone(),
            Locale::En => self.body_en.clone(),
        }
    }

    pub fn into_inbox_item(self, locale: Locale) -> InboxItem {
        let title = self.title(locale);
        let body = self.body(locale);
        let ts = self.publish_at.unwrap_or(0);
        InboxItem {
            id: self.id,
            kind: NotificationKind::System as i32,
            category: NotificationCategory::System as i32,
            severity: severity_from_db(&self.severity) as i32,
            r#type: "system.broadcast".to_string(),
            params_json: "{}".to_string(),
            fallback_text: title.clone(),
            title,
            body,
            link: opt(&self.link),
            actor_user_id: String::new(),
            org_id: String::new(),
            budget_id: String::new(),
            group_count: 1,
            created_at: ts,
            updated_at: ts,
            read_at: self.read_at.unwrap_or(0),
        }
    }

    pub fn into_banner(self, locale: Locale) -> Banner {
        let severity = severity_from_db(&self.severity);
        Banner {
            title: self.title(locale),
            body: self.body(locale),
            broadcast_id: self.id,
            severity: severity as i32,
            link: opt(&self.link),
            dismissible: severity != NotificationSeverity::Critical,
            expires_at: self.expires_at.unwrap_or(0),
        }
    }
}

impl DbBroadcast {
    pub fn into_proto(
        self,
        audience_user_ids: Vec<String>,
        read_count: i32,
    ) -> Result<Broadcast, serde_json::Error> {
        let audience_org_ids: Vec<String> = match &self.audience_orgs_json {
            Some(j) if !j.is_empty() => serde_json::from_str(j)?,
            _ => Vec::new(),
        };
        Ok(Broadcast {
            id: self.id,
            audience: audience_from_db(&self.audience_kind) as i32,
            audience_user_ids,
            audience_org_ids,
            severity: severity_from_db(&self.severity) as i32,
            title_vi: self.title_vi,
            body_vi: self.body_vi,
            title_en: self.title_en,
            body_en: self.body_en,
            link: opt(&self.link),
            status: status_from_db(&self.status) as i32,
            publish_at: self.publish_at.unwrap_or(0),
            expires_at: self.expires_at.unwrap_or(0),
            show_banner: self.show_banner,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
            read_count,
        })
    }
}
