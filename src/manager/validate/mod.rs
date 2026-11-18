use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::pb::service::notification::{
    BroadcastAudience, BroadcastInput, NotificationCategory, NotificationSeverity, PublishRequest,
};

pub const MAX_RECIPIENTS: usize = 500;
pub const MAX_AUDIENCE_USERS: usize = 5000;
pub const MAX_PARAMS_BYTES: usize = 8 * 1024;
pub const MAX_PAGE: i32 = 100;
pub const DEFAULT_PAGE: i32 = 20;
pub const MAX_IDS_PER_CALL: usize = 200;

pub fn user_id_from_metadata(meta: &MetadataMap) -> Result<String, Status> {
    meta.get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::unauthenticated("missing x-user-id metadata"))
}

pub fn user_type_from_metadata(meta: &MetadataMap) -> Option<String> {
    meta.get("x-user-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Personal inbox calls: a signed-in normal user. Super admins have no personal inbox.
pub fn normal_user(meta: &MetadataMap) -> Result<String, Status> {
    let user_id = user_id_from_metadata(meta)?;
    if user_type_from_metadata(meta).as_deref() == Some("super_admin") {
        return Err(Status::permission_denied(
            "super admins do not have a personal inbox",
        ));
    }
    Ok(user_id)
}

/// Admin calls: returns the admin's user id.
pub fn super_admin(meta: &MetadataMap) -> Result<String, Status> {
    let user_id = user_id_from_metadata(meta)?;
    if user_type_from_metadata(meta).as_deref() != Some("super_admin") {
        return Err(Status::permission_denied("super admin only"));
    }
    Ok(user_id)
}

/// Internal `Publish` calls must carry the shared secret. Compared in constant time.
pub fn internal_token(meta: &MetadataMap, expected: &str) -> Result<(), Status> {
    let given = meta
        .get("x-internal-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if expected.is_empty() || !constant_time_eq(given.as_bytes(), expected.as_bytes()) {
        return Err(Status::unauthenticated("invalid internal token"));
    }
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Relative web path only ("/budgets/…"): never an absolute URL, so a notification cannot
/// send a user to another site.
pub fn link(link: &str) -> Result<(), Status> {
    if link.is_empty() {
        return Ok(());
    }
    if link.len() > 512 {
        return Err(Status::invalid_argument(
            "link must be 512 characters or fewer",
        ));
    }
    if !link.starts_with('/') || link.starts_with("//") || link.contains('\\') {
        return Err(Status::invalid_argument(
            "link must be a relative path starting with '/'",
        ));
    }
    if link.chars().any(|c| c.is_control()) {
        return Err(Status::invalid_argument("link contains control characters"));
    }
    Ok(())
}

fn event_type(t: &str) -> Result<(), Status> {
    let ok = !t.is_empty()
        && t.len() <= 64
        && t.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_');
    if ok {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "type must be 1-64 chars of [a-z0-9._]",
        ))
    }
}

/// Validated, normalised `Publish` request.
#[derive(Debug, Clone)]
pub struct ValidPublish {
    pub category: NotificationCategory,
    pub severity: NotificationSeverity,
    /// De-duplicated, actor removed, blanks dropped.
    pub recipients: Vec<String>,
}

pub fn publish(req: &PublishRequest) -> Result<ValidPublish, Status> {
    if req.event_id.is_empty() || req.event_id.len() > 36 {
        return Err(Status::invalid_argument("event_id must be 1-36 characters"));
    }
    if req.source.is_empty() || req.source.len() > 32 {
        return Err(Status::invalid_argument("source must be 1-32 characters"));
    }
    event_type(&req.r#type)?;

    let category =
        NotificationCategory::try_from(req.category).unwrap_or(NotificationCategory::Unspecified);
    if matches!(
        category,
        NotificationCategory::Unspecified | NotificationCategory::System
    ) {
        return Err(Status::invalid_argument(
            "category must be activity, membership, reminder, alert or security",
        ));
    }
    let severity =
        NotificationSeverity::try_from(req.severity).unwrap_or(NotificationSeverity::Unspecified);
    let severity = if severity == NotificationSeverity::Unspecified {
        NotificationSeverity::Info
    } else {
        severity
    };

    if req.recipient_user_ids.len() > MAX_RECIPIENTS {
        return Err(Status::invalid_argument(format!(
            "at most {MAX_RECIPIENTS} recipients per event"
        )));
    }
    let mut seen = std::collections::HashSet::new();
    let recipients: Vec<String> = req
        .recipient_user_ids
        .iter()
        .filter(|id| !id.is_empty() && id.as_str() != req.actor_user_id && id.len() <= 36)
        .filter(|id| seen.insert(id.to_string()))
        .cloned()
        .collect();
    if recipients.is_empty() {
        return Err(Status::invalid_argument("no recipients"));
    }

    let fallback = req.fallback_text.trim();
    if fallback.is_empty() || fallback.len() > 512 {
        return Err(Status::invalid_argument(
            "fallback_text must be 1-512 characters",
        ));
    }
    if req.params_json.len() > MAX_PARAMS_BYTES {
        return Err(Status::invalid_argument("params_json is too large"));
    }
    if !req.params_json.is_empty() {
        match serde_json::from_str::<serde_json::Value>(&req.params_json) {
            Ok(serde_json::Value::Object(_)) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "params_json must be a JSON object",
                ))
            }
        }
    }
    link(&req.link)?;
    for (name, v) in [("dedup_key", &req.dedup_key), ("group_key", &req.group_key)] {
        if v.len() > 191 {
            return Err(Status::invalid_argument(format!(
                "{name} must be 191 characters or fewer"
            )));
        }
    }
    for (name, v) in [("org_id", &req.org_id), ("budget_id", &req.budget_id)] {
        if v.len() > 36 {
            return Err(Status::invalid_argument(format!(
                "{name} must be 36 characters or fewer"
            )));
        }
    }
    Ok(ValidPublish {
        category,
        severity,
        recipients,
    })
}

/// Validated broadcast input with the user ids de-duplicated.
#[derive(Debug, Clone)]
pub struct ValidBroadcast {
    pub audience: BroadcastAudience,
    pub user_ids: Vec<String>,
    pub severity: NotificationSeverity,
}

pub fn broadcast_input(input: &BroadcastInput, now: i64) -> Result<ValidBroadcast, Status> {
    let audience =
        BroadcastAudience::try_from(input.audience).unwrap_or(BroadcastAudience::Unspecified);
    let mut user_ids = Vec::new();
    match audience {
        BroadcastAudience::All => {}
        BroadcastAudience::Users => {
            let mut seen = std::collections::HashSet::new();
            for id in &input.audience_user_ids {
                let id = id.trim();
                if id.is_empty() || id.len() > 36 {
                    return Err(Status::invalid_argument("invalid user id in audience"));
                }
                if seen.insert(id.to_string()) {
                    user_ids.push(id.to_string());
                }
            }
            if user_ids.is_empty() {
                return Err(Status::invalid_argument(
                    "audience USERS needs at least one user id",
                ));
            }
            if user_ids.len() > MAX_AUDIENCE_USERS {
                return Err(Status::invalid_argument(format!(
                    "at most {MAX_AUDIENCE_USERS} users per broadcast"
                )));
            }
        }
        BroadcastAudience::Orgs => {
            return Err(Status::invalid_argument(
                "audience ORGS is not available yet; use ALL or USERS",
            ));
        }
        BroadcastAudience::Unspecified => {
            return Err(Status::invalid_argument("audience is required"));
        }
    }

    let severity =
        NotificationSeverity::try_from(input.severity).unwrap_or(NotificationSeverity::Unspecified);
    let severity = if severity == NotificationSeverity::Unspecified {
        NotificationSeverity::Info
    } else {
        severity
    };

    for (name, v, max) in [
        ("title_vi", &input.title_vi, 200),
        ("title_en", &input.title_en, 200),
        ("body_vi", &input.body_vi, 4000),
        ("body_en", &input.body_en, 4000),
    ] {
        let t = v.trim();
        if t.is_empty() {
            return Err(Status::invalid_argument(format!("{name} is required")));
        }
        if t.chars().count() > max {
            return Err(Status::invalid_argument(format!(
                "{name} must be {max} characters or fewer"
            )));
        }
    }
    link(&input.link)?;
    if input.publish_at < 0 || input.expires_at < 0 {
        return Err(Status::invalid_argument("timestamps must not be negative"));
    }
    if input.expires_at != 0 {
        let start = if input.publish_at != 0 {
            input.publish_at
        } else {
            now
        };
        if input.expires_at <= start {
            return Err(Status::invalid_argument(
                "expires_at must be after the publish time",
            ));
        }
    }
    Ok(ValidBroadcast {
        audience,
        user_ids,
        severity,
    })
}

pub fn page_limit(limit: i32) -> i64 {
    if limit <= 0 {
        DEFAULT_PAGE as i64
    } else {
        limit.min(MAX_PAGE) as i64
    }
}

pub fn ids(name: &str, ids: &[String]) -> Result<(), Status> {
    if ids.len() > MAX_IDS_PER_CALL {
        return Err(Status::invalid_argument(format!(
            "at most {MAX_IDS_PER_CALL} {name} per call"
        )));
    }
    if ids.iter().any(|i| i.is_empty() || i.len() > 36) {
        return Err(Status::invalid_argument(format!("invalid id in {name}")));
    }
    Ok(())
}

pub fn budget_id(id: &str) -> Result<(), Status> {
    if id.is_empty() || id.len() > 36 {
        return Err(Status::invalid_argument(
            "budget_id must be 1-36 characters",
        ));
    }
    Ok(())
}
