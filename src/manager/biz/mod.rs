//! Business rules: who gets what, preference gating, broadcast lifecycle.

use tonic::Status;

use crate::config::NotificationTuning;
use crate::converters::{
    audience_to_db, category_from_db, category_to_db, channel_from_db, channel_to_db,
    severity_to_db, status_to_db, DbBroadcast, Locale,
};
use crate::manager::email::{token, EmailSettings};
use crate::manager::repository::{
    ApplyOutcome, BroadcastWrite, NewDelivery, NewItem, NotificationRepository,
};
use crate::manager::validate;
use crate::pb::service::notification::{
    AdminListBroadcastsRequest, AdminListBroadcastsResponse, ArchiveRequest, Banner, Broadcast,
    BroadcastInput, BroadcastStatus, InboxFilter, InboxItem, ListInboxRequest, ListInboxResponse,
    MarkAllReadRequest, MarkReadRequest, NotificationCategory, NotificationChannel,
    NotificationKind, NotificationSeverity, PreferenceCell, Preferences, PublishRequest,
    PublishResponse, UnreadCount, UpdatePreferencesRequest,
};

/// The categories a user can receive personally, in display order.
pub const PERSONAL_CATEGORIES: [NotificationCategory; 5] = [
    NotificationCategory::Activity,
    NotificationCategory::Membership,
    NotificationCategory::Reminder,
    NotificationCategory::Alert,
    NotificationCategory::Security,
];

/// Security notifications cannot be switched off in-app.
pub fn is_locked(category: NotificationCategory, channel: NotificationChannel) -> bool {
    category == NotificationCategory::Security && channel == NotificationChannel::InApp
}

/// In-app is on by default. Email costs per message, so it is opt-in (security excepted), and
/// Telegram is not available yet. Only changes from the default are stored.
pub fn default_enabled(category: NotificationCategory, channel: NotificationChannel) -> bool {
    match channel {
        NotificationChannel::InApp | NotificationChannel::Unspecified => true,
        NotificationChannel::Email => category == NotificationCategory::Security,
        NotificationChannel::Telegram => false,
    }
}

fn internal(context: &str, e: impl std::fmt::Display) -> Status {
    tracing::error!("{context}: {e}");
    Status::internal("internal error")
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

pub fn encode_cursor(ts: i64, id: &str) -> String {
    format!("{ts}:{id}")
}

pub fn decode_cursor(s: &str) -> Result<Option<(i64, String)>, Status> {
    if s.is_empty() {
        return Ok(None);
    }
    let (ts, id) = s
        .split_once(':')
        .ok_or_else(|| Status::invalid_argument("invalid cursor"))?;
    let ts: i64 = ts
        .parse()
        .map_err(|_| Status::invalid_argument("invalid cursor"))?;
    if id.is_empty() || id.len() > 36 {
        return Err(Status::invalid_argument("invalid cursor"));
    }
    Ok(Some((ts, id.to_string())))
}

pub struct NotificationBiz {
    repo: NotificationRepository,
    internal_token: String,
    tuning: NotificationTuning,
    email: EmailSettings,
}

impl NotificationBiz {
    pub fn new(
        repo: NotificationRepository,
        internal_token: String,
        tuning: NotificationTuning,
    ) -> Self {
        Self {
            repo,
            internal_token,
            tuning,
            email: EmailSettings::disabled(),
        }
    }

    /// Turn the email channel on (queueing + preference cells + unsubscribe).
    pub fn with_email(mut self, email: EmailSettings) -> Self {
        self.email = email;
        self
    }

    pub fn email_active(&self) -> bool {
        self.email.active
    }

    pub fn repo(&self) -> &NotificationRepository {
        &self.repo
    }

    pub fn check_internal_token(&self, meta: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        validate::internal_token(meta, &self.internal_token)
    }

    // -----------------------------------------------------------------------
    // Publish (internal)
    // -----------------------------------------------------------------------

    pub async fn publish(&self, req: PublishRequest) -> Result<PublishResponse, Status> {
        self.publish_at(req, now()).await
    }

    pub async fn publish_at(
        &self,
        req: PublishRequest,
        now: i64,
    ) -> Result<PublishResponse, Status> {
        let valid = validate::publish(&req)?;
        let category = category_to_db(valid.category);
        let severity = severity_to_db(valid.severity);

        // Security and critical notifications ignore preferences and mutes.
        let locked = valid.category == NotificationCategory::Security
            || valid.severity == NotificationSeverity::Critical;
        let budget_id = non_empty(&req.budget_id);
        let allowed = if locked {
            valid.recipients.clone()
        } else {
            self.repo
                .filter_recipients(&valid.recipients, category, budget_id.as_deref())
                .await
                .map_err(|e| internal("filter recipients", e))?
        };

        let mut resp = PublishResponse {
            created: 0,
            collapsed: 0,
            skipped: (valid.recipients.len() - allowed.len()) as i32,
        };
        let params_json = if req.params_json.is_empty() {
            "{}".to_string()
        } else {
            req.params_json.clone()
        };
        for recipient in allowed {
            let item = NewItem {
                event_id: req.event_id.clone(),
                recipient_user_id: recipient,
                org_id: non_empty(&req.org_id),
                budget_id: budget_id.clone(),
                category: category.to_string(),
                event_type: req.r#type.clone(),
                severity: severity.to_string(),
                actor_user_id: non_empty(&req.actor_user_id),
                params_json: params_json.clone(),
                link: non_empty(&req.link),
                fallback_text: req.fallback_text.trim().to_string(),
                dedup_key: non_empty(&req.dedup_key),
                group_key: non_empty(&req.group_key),
            };
            match self
                .repo
                .apply_event(&item, self.tuning.group_window_secs, now)
                .await
                .map_err(|e| internal("apply event", e))?
            {
                ApplyOutcome::Created => resp.created += 1,
                ApplyOutcome::Collapsed => resp.collapsed += 1,
                ApplyOutcome::Skipped => resp.skipped += 1,
            }
        }
        if self.email.active {
            self.queue_emails(&req, &valid.recipients, category, severity, locked, now)
                .await?;
        }
        Ok(resp)
    }

    /// Queue one email per recipient who opted in to this category. Independent of the in-app
    /// outcome (the user may have switched the inbox off) and idempotent per delivery key.
    async fn queue_emails(
        &self,
        req: &PublishRequest,
        recipients: &[String],
        category: &str,
        severity: &str,
        locked: bool,
        now: i64,
    ) -> Result<(), Status> {
        let default_on = default_enabled(category_from_db(category), NotificationChannel::Email);
        let budget_id = non_empty(&req.budget_id);
        let wanted = self
            .repo
            .email_recipients(
                recipients,
                category,
                default_on,
                budget_id.as_deref(),
                !locked,
            )
            .await
            .map_err(|e| internal("filter email recipients", e))?;
        let key = non_empty(&req.dedup_key).unwrap_or_else(|| req.event_id.clone());
        for user_id in wanted {
            let delivery = NewDelivery {
                user_id,
                delivery_key: key.clone(),
                source_event_id: req.event_id.clone(),
                category: category.to_string(),
                event_type: req.r#type.clone(),
                severity: severity.to_string(),
                params_json: if req.params_json.is_empty() {
                    "{}".to_string()
                } else {
                    req.params_json.clone()
                },
                link: non_empty(&req.link),
                fallback_text: req.fallback_text.trim().to_string(),
            };
            self.repo
                .enqueue_delivery(&delivery, now)
                .await
                .map_err(|e| internal("queue email", e))?;
        }
        Ok(())
    }

    /// One-click unsubscribe from the link in an email: turns that category's email off.
    pub async fn unsubscribe_email(&self, token_str: &str) -> Result<String, Status> {
        self.unsubscribe_email_at(token_str, now()).await
    }

    pub async fn unsubscribe_email_at(&self, token_str: &str, now: i64) -> Result<String, Status> {
        let claim = token::verify(&self.email.unsubscribe_secret, token_str, now)
            .ok_or_else(|| Status::invalid_argument("invalid or expired link"))?;
        let category = category_from_db(&claim.category);
        if !PERSONAL_CATEGORIES.contains(&category) {
            return Err(Status::invalid_argument("invalid or expired link"));
        }
        self.repo
            .set_preference(
                &claim.user_id,
                category_to_db(category),
                channel_to_db(NotificationChannel::Email),
                false,
                default_enabled(category, NotificationChannel::Email),
                now,
            )
            .await
            .map_err(|e| internal("save preference", e))?;
        Ok(claim.category)
    }

    // -----------------------------------------------------------------------
    // Inbox
    // -----------------------------------------------------------------------

    pub async fn list_inbox(
        &self,
        user_id: &str,
        req: ListInboxRequest,
    ) -> Result<ListInboxResponse, Status> {
        self.list_inbox_at(user_id, req, now()).await
    }

    pub async fn list_inbox_at(
        &self,
        user_id: &str,
        req: ListInboxRequest,
        now: i64,
    ) -> Result<ListInboxResponse, Status> {
        let limit = validate::page_limit(req.limit);
        let cursor = decode_cursor(&req.cursor)?;
        let unread_only =
            InboxFilter::try_from(req.filter).unwrap_or(InboxFilter::All) == InboxFilter::Unread;
        let kind = NotificationKind::try_from(req.kind).unwrap_or(NotificationKind::Unspecified);
        let locale = Locale::parse(&req.locale);

        let mut items: Vec<InboxItem> = Vec::new();
        if kind != NotificationKind::System {
            let rows = self
                .repo
                .list_personal(user_id, unread_only, cursor.as_ref(), limit + 1)
                .await
                .map_err(|e| internal("list personal", e))?;
            items.extend(rows.into_iter().map(|r| r.into_proto()));
        }
        if kind != NotificationKind::Personal {
            let rows = self
                .repo
                .list_system(user_id, unread_only, cursor.as_ref(), limit + 1, now)
                .await
                .map_err(|e| internal("list system", e))?;
            items.extend(rows.into_iter().map(|r| r.into_inbox_item(locale)));
        }
        items.sort_by(|a, b| (b.updated_at, b.id.as_str()).cmp(&(a.updated_at, a.id.as_str())));
        let mut next_cursor = String::new();
        if items.len() as i64 > limit {
            items.truncate(limit as usize);
            if let Some(last) = items.last() {
                next_cursor = encode_cursor(last.updated_at, &last.id);
            }
        }
        Ok(ListInboxResponse { items, next_cursor })
    }

    pub async fn unread_count(&self, user_id: &str) -> Result<UnreadCount, Status> {
        self.unread_count_at(user_id, now()).await
    }

    pub async fn unread_count_at(&self, user_id: &str, now: i64) -> Result<UnreadCount, Status> {
        let personal = self
            .repo
            .count_personal_unread(user_id)
            .await
            .map_err(|e| internal("count personal", e))? as i32;
        let system = self
            .repo
            .count_system_unread(user_id, now)
            .await
            .map_err(|e| internal("count system", e))? as i32;
        Ok(UnreadCount {
            personal,
            system,
            total: personal + system,
        })
    }

    pub async fn mark_read(&self, user_id: &str, req: MarkReadRequest) -> Result<(), Status> {
        self.mark_read_at(user_id, req, now()).await
    }

    pub async fn mark_read_at(
        &self,
        user_id: &str,
        req: MarkReadRequest,
        now: i64,
    ) -> Result<(), Status> {
        validate::ids("item_ids", &req.item_ids)?;
        validate::ids("broadcast_ids", &req.broadcast_ids)?;
        self.repo
            .mark_items_read(user_id, &req.item_ids, now)
            .await
            .map_err(|e| internal("mark items read", e))?;
        self.repo
            .mark_broadcasts_read(user_id, &req.broadcast_ids, now)
            .await
            .map_err(|e| internal("mark broadcasts read", e))
    }

    pub async fn mark_all_read(
        &self,
        user_id: &str,
        req: MarkAllReadRequest,
    ) -> Result<(), Status> {
        self.mark_all_read_at(user_id, req, now()).await
    }

    pub async fn mark_all_read_at(
        &self,
        user_id: &str,
        req: MarkAllReadRequest,
        now: i64,
    ) -> Result<(), Status> {
        let kind = NotificationKind::try_from(req.kind).unwrap_or(NotificationKind::Unspecified);
        if kind != NotificationKind::System {
            self.repo
                .mark_all_items_read(user_id, now)
                .await
                .map_err(|e| internal("mark all items read", e))?;
        }
        if kind != NotificationKind::Personal {
            self.repo
                .mark_all_broadcasts_read(user_id, now)
                .await
                .map_err(|e| internal("mark all broadcasts read", e))?;
        }
        Ok(())
    }

    pub async fn archive(&self, user_id: &str, req: ArchiveRequest) -> Result<(), Status> {
        self.archive_at(user_id, req, now()).await
    }

    pub async fn archive_at(
        &self,
        user_id: &str,
        req: ArchiveRequest,
        now: i64,
    ) -> Result<(), Status> {
        validate::ids("item_ids", &req.item_ids)?;
        validate::ids("broadcast_ids", &req.broadcast_ids)?;
        self.repo
            .archive_items(user_id, &req.item_ids, now)
            .await
            .map_err(|e| internal("archive items", e))?;
        self.repo
            .dismiss_broadcasts(user_id, &req.broadcast_ids, now)
            .await
            .map_err(|e| internal("dismiss broadcasts", e))
    }

    pub async fn active_banners(&self, user_id: &str, locale: &str) -> Result<Vec<Banner>, Status> {
        self.active_banners_at(user_id, locale, now()).await
    }

    pub async fn active_banners_at(
        &self,
        user_id: &str,
        locale: &str,
        now: i64,
    ) -> Result<Vec<Banner>, Status> {
        let locale = Locale::parse(locale);
        let rows = self
            .repo
            .list_banners(user_id, now)
            .await
            .map_err(|e| internal("list banners", e))?;
        Ok(rows.into_iter().map(|r| r.into_banner(locale)).collect())
    }

    // -----------------------------------------------------------------------
    // Preferences
    // -----------------------------------------------------------------------

    pub async fn preferences(&self, user_id: &str) -> Result<Preferences, Status> {
        let overrides = self
            .repo
            .preference_overrides(user_id)
            .await
            .map_err(|e| internal("load preferences", e))?;
        let muted = self
            .repo
            .muted_budgets(user_id)
            .await
            .map_err(|e| internal("load mutes", e))?;
        let mut channels = vec![NotificationChannel::InApp];
        if self.email.active {
            channels.push(NotificationChannel::Email);
        }
        let cells = channels
            .iter()
            .flat_map(|&channel| PERSONAL_CATEGORIES.iter().map(move |&c| (c, channel)))
            .map(|(category, channel)| {
                let locked = is_locked(category, channel);
                let stored = overrides.iter().find(|(c, ch, _)| {
                    category_from_db(c) == category && channel_from_db(ch) == channel
                });
                let enabled = if locked {
                    true
                } else {
                    stored
                        .map(|(_, _, e)| *e)
                        .unwrap_or_else(|| default_enabled(category, channel))
                };
                PreferenceCell {
                    category: category as i32,
                    channel: channel as i32,
                    enabled,
                    locked,
                }
            })
            .collect();
        Ok(Preferences {
            cells,
            muted_budget_ids: muted,
        })
    }

    pub async fn update_preferences(
        &self,
        user_id: &str,
        req: UpdatePreferencesRequest,
    ) -> Result<Preferences, Status> {
        self.update_preferences_at(user_id, req, now()).await
    }

    pub async fn update_preferences_at(
        &self,
        user_id: &str,
        req: UpdatePreferencesRequest,
        now: i64,
    ) -> Result<Preferences, Status> {
        // Validate every cell before writing any.
        let mut changes = Vec::new();
        for cell in &req.cells {
            let category = NotificationCategory::try_from(cell.category)
                .unwrap_or(NotificationCategory::Unspecified);
            if !PERSONAL_CATEGORIES.contains(&category) {
                return Err(Status::invalid_argument("unknown notification category"));
            }
            let channel = NotificationChannel::try_from(cell.channel)
                .unwrap_or(NotificationChannel::Unspecified);
            let allowed = channel == NotificationChannel::InApp
                || (channel == NotificationChannel::Email && self.email.active);
            if !allowed {
                return Err(Status::invalid_argument(
                    "this notification channel is not available",
                ));
            }
            if is_locked(category, channel) && !cell.enabled {
                return Err(Status::failed_precondition(
                    "this notification category cannot be turned off",
                ));
            }
            changes.push((category, channel, cell.enabled));
        }
        for (category, channel, enabled) in changes {
            self.repo
                .set_preference(
                    user_id,
                    category_to_db(category),
                    channel_to_db(channel),
                    enabled,
                    default_enabled(category, channel),
                    now,
                )
                .await
                .map_err(|e| internal("save preference", e))?;
        }
        self.preferences(user_id).await
    }

    pub async fn set_budget_mute(
        &self,
        user_id: &str,
        budget_id: &str,
        muted: bool,
    ) -> Result<Preferences, Status> {
        validate::budget_id(budget_id)?;
        self.repo
            .set_budget_mute(user_id, budget_id, muted, now())
            .await
            .map_err(|e| internal("save budget mute", e))?;
        self.preferences(user_id).await
    }

    // -----------------------------------------------------------------------
    // Broadcasts (super admin)
    // -----------------------------------------------------------------------

    pub async fn admin_create_broadcast(
        &self,
        admin_id: &str,
        input: BroadcastInput,
    ) -> Result<Broadcast, Status> {
        self.admin_create_broadcast_at(admin_id, input, now()).await
    }

    pub async fn admin_create_broadcast_at(
        &self,
        admin_id: &str,
        input: BroadcastInput,
        now: i64,
    ) -> Result<Broadcast, Status> {
        let valid = validate::broadcast_input(&input, now)?;
        let write = to_write(&input, &valid);
        let id = uuid::Uuid::new_v4().to_string();
        self.repo
            .insert_broadcast(&id, &write, "draft", admin_id, now)
            .await
            .map_err(|e| internal("create broadcast", e))?;
        self.load_broadcast(&id, now).await
    }

    pub async fn admin_update_broadcast(
        &self,
        broadcast_id: &str,
        input: BroadcastInput,
    ) -> Result<Broadcast, Status> {
        self.admin_update_broadcast_at(broadcast_id, input, now())
            .await
    }

    pub async fn admin_update_broadcast_at(
        &self,
        broadcast_id: &str,
        input: BroadcastInput,
        now: i64,
    ) -> Result<Broadcast, Status> {
        let current = self.must_get(broadcast_id).await?;
        let status = effective_status(&current, now);
        if !matches!(status, BroadcastStatus::Draft | BroadcastStatus::Scheduled) {
            return Err(Status::failed_precondition(
                "only draft or scheduled broadcasts can be edited",
            ));
        }
        let valid = validate::broadcast_input(&input, now)?;
        if status == BroadcastStatus::Scheduled && input.publish_at <= now {
            return Err(Status::invalid_argument(
                "a scheduled broadcast needs a future publish_at; use publish to send it now",
            ));
        }
        let write = to_write(&input, &valid);
        self.repo
            .update_broadcast(broadcast_id, &write, status_to_db(status), now)
            .await
            .map_err(|e| internal("update broadcast", e))?;
        self.load_broadcast(broadcast_id, now).await
    }

    pub async fn admin_publish_broadcast(&self, broadcast_id: &str) -> Result<Broadcast, Status> {
        self.admin_publish_broadcast_at(broadcast_id, now()).await
    }

    pub async fn admin_publish_broadcast_at(
        &self,
        broadcast_id: &str,
        now: i64,
    ) -> Result<Broadcast, Status> {
        let current = self.must_get(broadcast_id).await?;
        match effective_status(&current, now) {
            BroadcastStatus::Draft => {
                let at = current.publish_at.filter(|t| *t > now);
                if let Some(exp) = current.expires_at {
                    if exp <= at.unwrap_or(now) {
                        return Err(Status::invalid_argument(
                            "expires_at must be after the publish time",
                        ));
                    }
                }
                match at {
                    Some(t) => {
                        self.repo
                            .set_broadcast_status(broadcast_id, "scheduled", Some(t), now)
                            .await
                    }
                    None => {
                        self.repo
                            .set_broadcast_status(broadcast_id, "published", Some(now), now)
                            .await
                    }
                }
                .map_err(|e| internal("publish broadcast", e))?;
            }
            BroadcastStatus::Scheduled => {
                // Send a scheduled broadcast now.
                if current.expires_at.is_some_and(|exp| exp <= now) {
                    return Err(Status::invalid_argument(
                        "expires_at must be after the publish time",
                    ));
                }
                self.repo
                    .set_broadcast_status(broadcast_id, "published", Some(now), now)
                    .await
                    .map_err(|e| internal("publish broadcast", e))?;
            }
            BroadcastStatus::Published => {
                return Err(Status::failed_precondition(
                    "broadcast is already published",
                ));
            }
            _ => {
                return Err(Status::failed_precondition(
                    "an archived broadcast cannot be published",
                ));
            }
        }
        self.load_broadcast(broadcast_id, now).await
    }

    pub async fn admin_archive_broadcast(&self, broadcast_id: &str) -> Result<Broadcast, Status> {
        self.admin_archive_broadcast_at(broadcast_id, now()).await
    }

    pub async fn admin_archive_broadcast_at(
        &self,
        broadcast_id: &str,
        now: i64,
    ) -> Result<Broadcast, Status> {
        let current = self.must_get(broadcast_id).await?;
        if current.status != "archived" {
            self.repo
                .set_broadcast_status(broadcast_id, "archived", current.publish_at, now)
                .await
                .map_err(|e| internal("archive broadcast", e))?;
        }
        self.load_broadcast(broadcast_id, now).await
    }

    pub async fn admin_list_broadcasts(
        &self,
        req: AdminListBroadcastsRequest,
    ) -> Result<AdminListBroadcastsResponse, Status> {
        self.admin_list_broadcasts_at(req, now()).await
    }

    pub async fn admin_list_broadcasts_at(
        &self,
        req: AdminListBroadcastsRequest,
        now: i64,
    ) -> Result<AdminListBroadcastsResponse, Status> {
        let limit = validate::page_limit(req.limit);
        let page = req.page.max(1) as i64;
        let status = BroadcastStatus::try_from(req.status).unwrap_or(BroadcastStatus::Unspecified);
        let filter = match status {
            BroadcastStatus::Unspecified => None,
            other => Some(status_to_db(other)),
        };
        let (rows, total) = self
            .repo
            .list_broadcasts(filter, now, limit, (page - 1) * limit)
            .await
            .map_err(|e| internal("list broadcasts", e))?;
        let ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let mut audiences = self
            .repo
            .audience_users(&ids)
            .await
            .map_err(|e| internal("load audiences", e))?;
        let counts = self
            .repo
            .read_counts(&ids)
            .await
            .map_err(|e| internal("load read counts", e))?;
        let mut broadcasts = Vec::with_capacity(rows.len());
        for row in rows {
            let id = row.id.clone();
            let b = self.to_proto(
                row,
                audiences.remove(&id).unwrap_or_default(),
                counts.get(&id).copied().unwrap_or(0) as i32,
                now,
            )?;
            broadcasts.push(b);
        }
        Ok(AdminListBroadcastsResponse {
            broadcasts,
            total: total as i32,
        })
    }

    async fn must_get(&self, id: &str) -> Result<DbBroadcast, Status> {
        validate::budget_id(id).map_err(|_| Status::invalid_argument("invalid broadcast id"))?;
        self.repo
            .get_broadcast(id)
            .await
            .map_err(|e| internal("load broadcast", e))?
            .ok_or_else(|| Status::not_found("broadcast not found"))
    }

    async fn load_broadcast(&self, id: &str, now: i64) -> Result<Broadcast, Status> {
        let row = self.must_get(id).await?;
        let ids = vec![id.to_string()];
        let mut audiences = self
            .repo
            .audience_users(&ids)
            .await
            .map_err(|e| internal("load audience", e))?;
        let counts = self
            .repo
            .read_counts(&ids)
            .await
            .map_err(|e| internal("load read count", e))?;
        self.to_proto(
            row,
            audiences.remove(id).unwrap_or_default(),
            counts.get(id).copied().unwrap_or(0) as i32,
            now,
        )
    }

    fn to_proto(
        &self,
        mut row: DbBroadcast,
        users: Vec<String>,
        read_count: i32,
        now: i64,
    ) -> Result<Broadcast, Status> {
        // Show the effective status: a scheduled broadcast whose time has come is live.
        row.status = status_to_db(effective_status(&row, now)).to_string();
        row.into_proto(users, read_count)
            .map_err(|e| internal("decode broadcast", e))
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    pub async fn run_cleanup(
        &self,
        now: i64,
    ) -> anyhow::Result<crate::manager::repository::CleanupStats> {
        self.repo
            .cleanup(
                now - self.tuning.read_retention_secs,
                now - self.tuning.unread_retention_secs,
                now - 30 * 24 * 3600,
                now - 14 * 24 * 3600,
            )
            .await
    }

    /// Run [`Self::run_cleanup`] periodically until the process stops.
    pub fn spawn_cleanup(self: &std::sync::Arc<Self>) {
        let biz = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            loop {
                match biz.run_cleanup(now()).await {
                    Ok(s) if s.items + s.applied_events > 0 => {
                        tracing::info!(
                            items = s.items,
                            applied_events = s.applied_events,
                            "cleanup"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!("cleanup failed: {e}"),
                }
                tokio::time::sleep(biz.tuning.cleanup_interval).await;
            }
        });
    }
}

fn non_empty(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// `scheduled` whose publish time has passed is `published`.
pub fn effective_status(row: &DbBroadcast, now: i64) -> BroadcastStatus {
    let stored = crate::converters::status_from_db(&row.status);
    if stored == BroadcastStatus::Scheduled && row.publish_at.is_some_and(|t| t <= now) {
        BroadcastStatus::Published
    } else {
        stored
    }
}

fn to_write(input: &BroadcastInput, valid: &validate::ValidBroadcast) -> BroadcastWrite {
    BroadcastWrite {
        audience_kind: audience_to_db(valid.audience).to_string(),
        audience_user_ids: valid.user_ids.clone(),
        severity: severity_to_db(valid.severity).to_string(),
        title_vi: input.title_vi.trim().to_string(),
        body_vi: input.body_vi.trim().to_string(),
        title_en: input.title_en.trim().to_string(),
        body_en: input.body_en.trim().to_string(),
        link: non_empty(&input.link),
        publish_at: (input.publish_at > 0).then_some(input.publish_at),
        expires_at: (input.expires_at > 0).then_some(input.expires_at),
        show_banner: input.show_banner,
    }
}
