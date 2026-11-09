//! Typed configuration. The shared struct lives in `libs/configs`; the knobs below are
//! notification-specific and read from the environment with safe defaults.

pub use philand_configs::NotificationServiceConfig;

use std::time::Duration;

/// Tunables that do not need to live in the shared libs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationTuning {
    /// Personal items that were read are deleted after this many seconds (default 90 days).
    pub read_retention_secs: i64,
    /// Personal items nobody read are deleted after this many seconds (default 180 days).
    pub unread_retention_secs: i64,
    /// Time between cleanup passes (default 6 hours).
    pub cleanup_interval: Duration,
    /// An unread item with the same `group_key` newer than this absorbs new events
    /// instead of creating another row (default 15 minutes).
    pub group_window_secs: i64,
}

impl Default for NotificationTuning {
    fn default() -> Self {
        Self {
            read_retention_secs: 90 * 24 * 3600,
            unread_retention_secs: 180 * 24 * 3600,
            cleanup_interval: Duration::from_secs(6 * 3600),
            group_window_secs: 15 * 60,
        }
    }
}

impl NotificationTuning {
    /// `NOTIFICATION_READ_RETENTION_DAYS`, `NOTIFICATION_UNREAD_RETENTION_DAYS`,
    /// `NOTIFICATION_CLEANUP_INTERVAL_SECS`, `NOTIFICATION_GROUP_WINDOW_SECS`.
    /// Missing or non-positive values keep the default.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            read_retention_secs: positive("NOTIFICATION_READ_RETENTION_DAYS")
                .map(|n| n * 24 * 3600)
                .unwrap_or(d.read_retention_secs),
            unread_retention_secs: positive("NOTIFICATION_UNREAD_RETENTION_DAYS")
                .map(|n| n * 24 * 3600)
                .unwrap_or(d.unread_retention_secs),
            cleanup_interval: positive("NOTIFICATION_CLEANUP_INTERVAL_SECS")
                .map(|n| Duration::from_secs(n as u64))
                .unwrap_or(d.cleanup_interval),
            group_window_secs: positive("NOTIFICATION_GROUP_WINDOW_SECS")
                .unwrap_or(d.group_window_secs),
        }
    }
}

fn positive(name: &str) -> Option<i64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|n| *n > 0)
}
