//! All SQL for the notification service. Queries are built at runtime (no `sqlx::query!`
//! macros) so the crate builds without a database or offline metadata.

use anyhow::Result;
use sqlx::{MySql, MySqlPool, QueryBuilder};

use crate::converters::{DbBroadcast, DbBroadcastView, DbDelivery, DbItem};

const ITEM_COLS: &str = "id, recipient_user_id, org_id, budget_id, category, event_type, \
    severity, actor_user_id, params_json, link, fallback_text, source_event_id, dedup_key, \
    group_key, group_count, created_at, updated_at, read_at, archived_at";

const BROADCAST_COLS: &str = "id, audience_kind, audience_orgs_json, severity, title_vi, \
    body_vi, title_en, body_en, link, status, publish_at, expires_at, show_banner, created_by, \
    created_at, updated_at";

/// `b` must be `notification_broadcasts`. Binds, in order: now, now, user_id.
const VISIBLE_TO_USER: &str = "b.status IN ('published','scheduled') \
    AND b.publish_at IS NOT NULL AND b.publish_at <= ? \
    AND (b.expires_at IS NULL OR b.expires_at > ?) \
    AND (b.audience_kind = 'all' OR (b.audience_kind = 'users' AND EXISTS ( \
        SELECT 1 FROM notification_broadcast_audience a \
        WHERE a.broadcast_id = b.id AND a.user_id = ?)))";

const VIEW_COLS: &str = "b.id, b.severity, b.title_vi, b.body_vi, b.title_en, b.body_en, \
    b.link, b.show_banner, b.publish_at, b.expires_at, s.read_at";

pub struct NotificationRepository {
    pool: MySqlPool,
}

/// A personal notification to store for one recipient.
#[derive(Debug, Clone)]
pub struct NewItem {
    pub event_id: String,
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
    pub dedup_key: Option<String>,
    pub group_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Created,
    /// Merged into an existing unread item with the same group key.
    Collapsed,
    /// Same event already applied, or the dedup key is already taken.
    Skipped,
}

/// Fields written when a broadcast is created or edited.
#[derive(Debug, Clone)]
pub struct BroadcastWrite {
    pub audience_kind: String,
    pub audience_user_ids: Vec<String>,
    pub severity: String,
    pub title_vi: String,
    pub body_vi: String,
    pub title_en: String,
    pub body_en: String,
    pub link: Option<String>,
    pub publish_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub show_banner: bool,
}

#[derive(Debug, Clone)]
pub struct NewDelivery {
    pub user_id: String,
    pub delivery_key: String,
    pub source_event_id: String,
    pub category: String,
    pub event_type: String,
    pub severity: String,
    pub params_json: String,
    pub link: Option<String>,
    pub fallback_text: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupStats {
    pub items: u64,
    pub applied_events: u64,
}

fn is_duplicate_key(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(db) => {
            db.code().as_deref() == Some("23000") || db.message().contains("Duplicate entry")
        }
        _ => false,
    }
}

fn push_in_list<'a>(qb: &mut QueryBuilder<'a, MySql>, values: &'a [String]) {
    let mut sep = qb.separated(", ");
    for v in values {
        sep.push_bind(v.as_str());
    }
    qb.push(")");
}

impl NotificationRepository {
    /// Connect and run the migrations in `migrations_dir`.
    pub async fn new(database_url: &str, migrations_dir: &str) -> Result<Self> {
        let pool = MySqlPool::connect(database_url).await?;
        philand_storage::migrate::run_idempotent(&pool, migrations_dir)
            .await
            .map_err(|e| anyhow::anyhow!("migrations failed: {e}"))?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    // -----------------------------------------------------------------------
    // Publish
    // -----------------------------------------------------------------------

    /// Drop recipients who turned this category off (in-app) or muted the budget.
    /// Keeps the input order.
    pub async fn filter_recipients(
        &self,
        recipients: &[String],
        category: &str,
        budget_id: Option<&str>,
    ) -> Result<Vec<String>> {
        if recipients.is_empty() {
            return Ok(Vec::new());
        }
        let mut blocked = std::collections::HashSet::<String>::new();

        let mut qb = QueryBuilder::<MySql>::new(
            "SELECT user_id FROM notification_preferences WHERE channel = 'in_app' \
             AND enabled = 0 AND category = ",
        );
        qb.push_bind(category).push(" AND user_id IN (");
        push_in_list(&mut qb, recipients);
        for (u,) in qb
            .build_query_as::<(String,)>()
            .fetch_all(&self.pool)
            .await?
        {
            blocked.insert(u);
        }

        if let Some(budget_id) = budget_id.filter(|b| !b.is_empty()) {
            let mut qb = QueryBuilder::<MySql>::new(
                "SELECT user_id FROM notification_budget_mutes WHERE budget_id = ",
            );
            qb.push_bind(budget_id).push(" AND user_id IN (");
            push_in_list(&mut qb, recipients);
            for (u,) in qb
                .build_query_as::<(String,)>()
                .fetch_all(&self.pool)
                .await?
            {
                blocked.insert(u);
            }
        }

        Ok(recipients
            .iter()
            .filter(|r| !blocked.contains(*r))
            .cloned()
            .collect())
    }

    /// Store `item` for one recipient, exactly once per (event, recipient).
    pub async fn apply_event(
        &self,
        item: &NewItem,
        group_window_secs: i64,
        now: i64,
    ) -> Result<ApplyOutcome> {
        let mut tx = self.pool.begin().await?;

        let applied = sqlx::query(
            "INSERT IGNORE INTO notification_applied_events \
             (event_id, recipient_user_id, applied_at) VALUES (?, ?, ?)",
        )
        .bind(&item.event_id)
        .bind(&item.recipient_user_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if applied.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(ApplyOutcome::Skipped);
        }

        if let Some(group_key) = item.group_key.as_deref().filter(|g| !g.is_empty()) {
            let existing: Option<(String,)> = sqlx::query_as(
                "SELECT id FROM notification_items \
                 WHERE recipient_user_id = ? AND group_key = ? \
                   AND read_at IS NULL AND archived_at IS NULL AND updated_at >= ? \
                 ORDER BY updated_at DESC LIMIT 1 FOR UPDATE",
            )
            .bind(&item.recipient_user_id)
            .bind(group_key)
            .bind(now - group_window_secs)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some((id,)) = existing {
                sqlx::query(
                    "UPDATE notification_items SET group_count = group_count + 1, \
                     params_json = ?, fallback_text = ?, actor_user_id = ?, link = ?, \
                     severity = ?, updated_at = ? WHERE id = ?",
                )
                .bind(&item.params_json)
                .bind(&item.fallback_text)
                .bind(&item.actor_user_id)
                .bind(&item.link)
                .bind(&item.severity)
                .bind(now)
                .bind(&id)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(ApplyOutcome::Collapsed);
            }
        }

        let inserted = sqlx::query(
            "INSERT INTO notification_items (id, recipient_user_id, org_id, budget_id, category, \
             event_type, severity, actor_user_id, params_json, link, fallback_text, \
             source_event_id, dedup_key, group_key, group_count, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&item.recipient_user_id)
        .bind(&item.org_id)
        .bind(&item.budget_id)
        .bind(&item.category)
        .bind(&item.event_type)
        .bind(&item.severity)
        .bind(&item.actor_user_id)
        .bind(&item.params_json)
        .bind(&item.link)
        .bind(&item.fallback_text)
        .bind(&item.event_id)
        .bind(&item.dedup_key)
        .bind(item.group_key.as_deref().filter(|g| !g.is_empty()))
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {
                tx.commit().await?;
                Ok(ApplyOutcome::Created)
            }
            Err(e) if is_duplicate_key(&e) => {
                tx.rollback().await?;
                Ok(ApplyOutcome::Skipped)
            }
            Err(e) => Err(e.into()),
        }
    }

    // -----------------------------------------------------------------------
    // Inbox: reads
    // -----------------------------------------------------------------------

    /// Newest first by (updated_at, id). `cursor` is the (updated_at, id) of the last row of
    /// the previous page.
    pub async fn list_personal(
        &self,
        user_id: &str,
        unread_only: bool,
        cursor: Option<&(i64, String)>,
        limit: i64,
    ) -> Result<Vec<DbItem>> {
        let mut sql = format!(
            "SELECT {ITEM_COLS} FROM notification_items \
             WHERE recipient_user_id = ? AND archived_at IS NULL"
        );
        if unread_only {
            sql.push_str(" AND read_at IS NULL");
        }
        if cursor.is_some() {
            sql.push_str(" AND (updated_at < ? OR (updated_at = ? AND id < ?))");
        }
        sql.push_str(" ORDER BY updated_at DESC, id DESC LIMIT ?");
        let mut q = sqlx::query_as::<_, DbItem>(&sql).bind(user_id);
        if let Some((ts, id)) = cursor {
            q = q.bind(ts).bind(ts).bind(id);
        }
        Ok(q.bind(limit).fetch_all(&self.pool).await?)
    }

    pub async fn list_system(
        &self,
        user_id: &str,
        unread_only: bool,
        cursor: Option<&(i64, String)>,
        limit: i64,
        now: i64,
    ) -> Result<Vec<DbBroadcastView>> {
        let mut sql = format!(
            "SELECT {VIEW_COLS} FROM notification_broadcasts b \
             LEFT JOIN notification_broadcast_states s \
               ON s.broadcast_id = b.id AND s.user_id = ? \
             WHERE {VISIBLE_TO_USER} AND s.dismissed_at IS NULL"
        );
        if unread_only {
            sql.push_str(" AND s.read_at IS NULL");
        }
        if cursor.is_some() {
            sql.push_str(" AND (b.publish_at < ? OR (b.publish_at = ? AND b.id < ?))");
        }
        sql.push_str(" ORDER BY b.publish_at DESC, b.id DESC LIMIT ?");
        let mut q = sqlx::query_as::<_, DbBroadcastView>(&sql)
            .bind(user_id)
            .bind(now)
            .bind(now)
            .bind(user_id);
        if let Some((ts, id)) = cursor {
            q = q.bind(ts).bind(ts).bind(id);
        }
        Ok(q.bind(limit).fetch_all(&self.pool).await?)
    }

    pub async fn count_personal_unread(&self, user_id: &str) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM notification_items \
             WHERE recipient_user_id = ? AND archived_at IS NULL AND read_at IS NULL",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    pub async fn count_system_unread(&self, user_id: &str, now: i64) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(*) FROM notification_broadcasts b \
             LEFT JOIN notification_broadcast_states s \
               ON s.broadcast_id = b.id AND s.user_id = ? \
             WHERE {VISIBLE_TO_USER} AND s.dismissed_at IS NULL AND s.read_at IS NULL"
        );
        let (n,): (i64,) = sqlx::query_as(&sql)
            .bind(user_id)
            .bind(now)
            .bind(now)
            .bind(user_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    pub async fn list_banners(&self, user_id: &str, now: i64) -> Result<Vec<DbBroadcastView>> {
        let sql = format!(
            "SELECT {VIEW_COLS} FROM notification_broadcasts b \
             LEFT JOIN notification_broadcast_states s \
               ON s.broadcast_id = b.id AND s.user_id = ? \
             WHERE {VISIBLE_TO_USER} AND b.show_banner = 1 AND s.dismissed_at IS NULL \
             ORDER BY b.publish_at DESC, b.id DESC LIMIT 5"
        );
        Ok(sqlx::query_as::<_, DbBroadcastView>(&sql)
            .bind(user_id)
            .bind(now)
            .bind(now)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?)
    }

    // -----------------------------------------------------------------------
    // Inbox: writes
    // -----------------------------------------------------------------------

    pub async fn mark_items_read(&self, user_id: &str, ids: &[String], now: i64) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut qb = QueryBuilder::<MySql>::new("UPDATE notification_items SET read_at = ");
        qb.push_bind(now)
            .push(" WHERE read_at IS NULL AND recipient_user_id = ")
            .push_bind(user_id)
            .push(" AND id IN (");
        push_in_list(&mut qb, ids);
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn mark_all_items_read(&self, user_id: &str, now: i64) -> Result<()> {
        sqlx::query(
            "UPDATE notification_items SET read_at = ? \
             WHERE recipient_user_id = ? AND read_at IS NULL AND archived_at IS NULL",
        )
        .bind(now)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn archive_items(&self, user_id: &str, ids: &[String], now: i64) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut qb = QueryBuilder::<MySql>::new("UPDATE notification_items SET archived_at = ");
        qb.push_bind(now)
            .push(", read_at = COALESCE(read_at, ")
            .push_bind(now)
            .push(") WHERE archived_at IS NULL AND recipient_user_id = ")
            .push_bind(user_id)
            .push(" AND id IN (");
        push_in_list(&mut qb, ids);
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Mark the given broadcasts read for `user_id`. Broadcasts the user cannot see are
    /// ignored.
    pub async fn mark_broadcasts_read(
        &self,
        user_id: &str,
        ids: &[String],
        now: i64,
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut qb = QueryBuilder::<MySql>::new(
            "INSERT INTO notification_broadcast_states (broadcast_id, user_id, read_at) \
             SELECT b.id, ",
        );
        qb.push_bind(user_id).push(", ").push_bind(now);
        qb.push(" FROM notification_broadcasts b WHERE ");
        push_visible(&mut qb, user_id, now);
        qb.push(" AND b.id IN (");
        push_in_list(&mut qb, ids);
        qb.push(" ON DUPLICATE KEY UPDATE read_at = COALESCE(read_at, VALUES(read_at))");
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn mark_all_broadcasts_read(&self, user_id: &str, now: i64) -> Result<()> {
        let mut qb = QueryBuilder::<MySql>::new(
            "INSERT INTO notification_broadcast_states (broadcast_id, user_id, read_at) \
             SELECT b.id, ",
        );
        qb.push_bind(user_id).push(", ").push_bind(now);
        qb.push(" FROM notification_broadcasts b WHERE ");
        push_visible(&mut qb, user_id, now);
        qb.push(" ON DUPLICATE KEY UPDATE read_at = COALESCE(read_at, VALUES(read_at))");
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Hide broadcasts from this user's inbox. Critical broadcasts cannot be dismissed.
    pub async fn dismiss_broadcasts(&self, user_id: &str, ids: &[String], now: i64) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut qb = QueryBuilder::<MySql>::new(
            "INSERT INTO notification_broadcast_states \
             (broadcast_id, user_id, read_at, dismissed_at) SELECT b.id, ",
        );
        qb.push_bind(user_id)
            .push(", ")
            .push_bind(now)
            .push(", ")
            .push_bind(now);
        qb.push(" FROM notification_broadcasts b WHERE b.severity <> 'critical' AND ");
        push_visible(&mut qb, user_id, now);
        qb.push(" AND b.id IN (");
        push_in_list(&mut qb, ids);
        qb.push(
            " ON DUPLICATE KEY UPDATE read_at = COALESCE(read_at, VALUES(read_at)), \
             dismissed_at = COALESCE(dismissed_at, VALUES(dismissed_at))",
        );
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Preferences
    // -----------------------------------------------------------------------

    /// (category, channel, enabled) cells the user changed from the default.
    pub async fn preference_overrides(&self, user_id: &str) -> Result<Vec<(String, String, bool)>> {
        Ok(sqlx::query_as(
            "SELECT category, channel, enabled FROM notification_preferences WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Store the cell, or drop the override when `enabled` equals the default.
    pub async fn set_preference(
        &self,
        user_id: &str,
        category: &str,
        channel: &str,
        enabled: bool,
        default_enabled: bool,
        now: i64,
    ) -> Result<()> {
        if enabled == default_enabled {
            sqlx::query(
                "DELETE FROM notification_preferences \
                 WHERE user_id = ? AND category = ? AND channel = ?",
            )
            .bind(user_id)
            .bind(category)
            .bind(channel)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO notification_preferences (user_id, category, channel, enabled, \
                 updated_at) VALUES (?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), \
                 updated_at = VALUES(updated_at)",
            )
            .bind(user_id)
            .bind(category)
            .bind(channel)
            .bind(enabled)
            .bind(now)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn muted_budgets(&self, user_id: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT budget_id FROM notification_budget_mutes WHERE user_id = ? \
             ORDER BY created_at, budget_id",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(b,)| b).collect())
    }

    pub async fn set_budget_mute(
        &self,
        user_id: &str,
        budget_id: &str,
        muted: bool,
        now: i64,
    ) -> Result<()> {
        if muted {
            sqlx::query(
                "INSERT IGNORE INTO notification_budget_mutes (user_id, budget_id, created_at) \
                 VALUES (?, ?, ?)",
            )
            .bind(user_id)
            .bind(budget_id)
            .bind(now)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                "DELETE FROM notification_budget_mutes WHERE user_id = ? AND budget_id = ?",
            )
            .bind(user_id)
            .bind(budget_id)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Broadcasts (admin)
    // -----------------------------------------------------------------------

    pub async fn insert_broadcast(
        &self,
        id: &str,
        w: &BroadcastWrite,
        status: &str,
        created_by: &str,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO notification_broadcasts (id, audience_kind, severity, title_vi, \
             body_vi, title_en, body_en, link, status, publish_at, expires_at, show_banner, \
             created_by, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(&w.audience_kind)
        .bind(&w.severity)
        .bind(&w.title_vi)
        .bind(&w.body_vi)
        .bind(&w.title_en)
        .bind(&w.body_en)
        .bind(&w.link)
        .bind(status)
        .bind(w.publish_at)
        .bind(w.expires_at)
        .bind(w.show_banner)
        .bind(created_by)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        insert_audience(&mut tx, id, &w.audience_user_ids).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn update_broadcast(
        &self,
        id: &str,
        w: &BroadcastWrite,
        status: &str,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE notification_broadcasts SET audience_kind = ?, severity = ?, title_vi = ?, \
             body_vi = ?, title_en = ?, body_en = ?, link = ?, status = ?, publish_at = ?, \
             expires_at = ?, show_banner = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&w.audience_kind)
        .bind(&w.severity)
        .bind(&w.title_vi)
        .bind(&w.body_vi)
        .bind(&w.title_en)
        .bind(&w.body_en)
        .bind(&w.link)
        .bind(status)
        .bind(w.publish_at)
        .bind(w.expires_at)
        .bind(w.show_banner)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM notification_broadcast_audience WHERE broadcast_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        insert_audience(&mut tx, id, &w.audience_user_ids).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn set_broadcast_status(
        &self,
        id: &str,
        status: &str,
        publish_at: Option<i64>,
        now: i64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE notification_broadcasts SET status = ?, publish_at = ?, updated_at = ? \
             WHERE id = ?",
        )
        .bind(status)
        .bind(publish_at)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_broadcast(&self, id: &str) -> Result<Option<DbBroadcast>> {
        let sql = format!("SELECT {BROADCAST_COLS} FROM notification_broadcasts WHERE id = ?");
        Ok(sqlx::query_as::<_, DbBroadcast>(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// `status` is the *effective* status: `published` includes scheduled broadcasts whose
    /// time has come, `scheduled` only the ones still in the future.
    pub async fn list_broadcasts(
        &self,
        status: Option<&str>,
        now: i64,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<DbBroadcast>, i64)> {
        let where_sql = match status {
            Some("published") => {
                " WHERE (status = 'published' OR (status = 'scheduled' AND publish_at <= ?))"
            }
            Some("scheduled") => " WHERE status = 'scheduled' AND publish_at > ?",
            Some(_) => " WHERE status = ?",
            None => "",
        };
        let sql = format!(
            "SELECT {BROADCAST_COLS} FROM notification_broadcasts{where_sql} \
             ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        );
        let count_sql = format!("SELECT COUNT(*) FROM notification_broadcasts{where_sql}");
        let mut q = sqlx::query_as::<_, DbBroadcast>(&sql);
        let mut cq = sqlx::query_as::<_, (i64,)>(&count_sql);
        match status {
            Some("published") | Some("scheduled") => {
                q = q.bind(now);
                cq = cq.bind(now);
            }
            Some(other) => {
                q = q.bind(other.to_string());
                cq = cq.bind(other.to_string());
            }
            None => {}
        }
        let rows = q.bind(limit).bind(offset).fetch_all(&self.pool).await?;
        let (total,) = cq.fetch_one(&self.pool).await?;
        Ok((rows, total))
    }

    /// Audience user ids for the given broadcasts.
    pub async fn audience_users(
        &self,
        broadcast_ids: &[String],
    ) -> Result<std::collections::HashMap<String, Vec<String>>> {
        let mut out: std::collections::HashMap<String, Vec<String>> = Default::default();
        if broadcast_ids.is_empty() {
            return Ok(out);
        }
        let mut qb = QueryBuilder::<MySql>::new(
            "SELECT broadcast_id, user_id FROM notification_broadcast_audience \
             WHERE broadcast_id IN (",
        );
        push_in_list(&mut qb, broadcast_ids);
        qb.push(" ORDER BY user_id");
        for (b, u) in qb
            .build_query_as::<(String, String)>()
            .fetch_all(&self.pool)
            .await?
        {
            out.entry(b).or_default().push(u);
        }
        Ok(out)
    }

    pub async fn read_counts(
        &self,
        broadcast_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>> {
        let mut out = std::collections::HashMap::new();
        if broadcast_ids.is_empty() {
            return Ok(out);
        }
        let mut qb = QueryBuilder::<MySql>::new(
            "SELECT broadcast_id, COUNT(*) FROM notification_broadcast_states \
             WHERE read_at IS NOT NULL AND broadcast_id IN (",
        );
        push_in_list(&mut qb, broadcast_ids);
        qb.push(" GROUP BY broadcast_id");
        for (b, n) in qb
            .build_query_as::<(String, i64)>()
            .fetch_all(&self.pool)
            .await?
        {
            out.insert(b, n);
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Email deliveries (Phase 4)
    // -----------------------------------------------------------------------

    /// Of `recipients`, the users who want email for `category`: their stored choice, or
    /// `default_on` when they never changed it. Muted budgets are dropped when `apply_mutes`.
    pub async fn email_recipients(
        &self,
        recipients: &[String],
        category: &str,
        default_on: bool,
        budget_id: Option<&str>,
        apply_mutes: bool,
    ) -> Result<Vec<String>> {
        if recipients.is_empty() {
            return Ok(Vec::new());
        }
        let mut overrides = std::collections::HashMap::<String, bool>::new();
        let mut qb = QueryBuilder::<MySql>::new(
            "SELECT user_id, enabled FROM notification_preferences WHERE channel = 'email' \
             AND category = ",
        );
        qb.push_bind(category).push(" AND user_id IN (");
        push_in_list(&mut qb, recipients);
        for (u, e) in qb
            .build_query_as::<(String, bool)>()
            .fetch_all(&self.pool)
            .await?
        {
            overrides.insert(u, e);
        }
        let mut muted = std::collections::HashSet::<String>::new();
        if let (true, Some(budget_id)) = (apply_mutes, budget_id.filter(|b| !b.is_empty())) {
            let mut qb = QueryBuilder::<MySql>::new(
                "SELECT user_id FROM notification_budget_mutes WHERE budget_id = ",
            );
            qb.push_bind(budget_id).push(" AND user_id IN (");
            push_in_list(&mut qb, recipients);
            for (u,) in qb
                .build_query_as::<(String,)>()
                .fetch_all(&self.pool)
                .await?
            {
                muted.insert(u);
            }
        }
        Ok(recipients
            .iter()
            .filter(|r| !muted.contains(*r) && overrides.get(*r).copied().unwrap_or(default_on))
            .cloned()
            .collect())
    }

    /// Queue an email once per (user, key). Returns false when it was already queued.
    pub async fn enqueue_delivery(&self, d: &NewDelivery, now: i64) -> Result<bool> {
        let r = sqlx::query(
            "INSERT IGNORE INTO notification_deliveries (id, user_id, channel, delivery_key, \
             source_event_id, category, event_type, severity, params_json, link, fallback_text, \
             status, attempts, next_attempt_at, created_at, updated_at) \
             VALUES (?, ?, 'email', ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&d.user_id)
        .bind(&d.delivery_key)
        .bind(&d.source_event_id)
        .bind(&d.category)
        .bind(&d.event_type)
        .bind(&d.severity)
        .bind(&d.params_json)
        .bind(&d.link)
        .bind(&d.fallback_text)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn due_deliveries(&self, now: i64, limit: i64) -> Result<Vec<DbDelivery>> {
        Ok(sqlx::query_as(
            "SELECT id, user_id, category, event_type, params_json, link, fallback_text, attempts \
             FROM notification_deliveries \
             WHERE status = 'pending' AND channel = 'email' AND next_attempt_at <= ? \
             ORDER BY next_attempt_at, id LIMIT ?",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Take a lease on a due delivery; false when another worker got it.
    pub async fn claim_delivery(&self, id: &str, now: i64, lease_secs: i64) -> Result<bool> {
        let r = sqlx::query(
            "UPDATE notification_deliveries SET attempts = attempts + 1, next_attempt_at = ?, \
             updated_at = ? WHERE id = ? AND status = 'pending' AND next_attempt_at <= ?",
        )
        .bind(now + lease_secs)
        .bind(now)
        .bind(id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn mark_delivery_sent(&self, id: &str, message_id: &str, now: i64) -> Result<()> {
        sqlx::query(
            "UPDATE notification_deliveries SET status = 'sent', provider_message_id = ?, \
             last_error = NULL, updated_at = ? WHERE id = ?",
        )
        .bind(message_id)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Terminal state: `failed` or `skipped`.
    pub async fn finish_delivery(&self, id: &str, status: &str, why: &str, now: i64) -> Result<()> {
        sqlx::query(
            "UPDATE notification_deliveries SET status = ?, last_error = ?, updated_at = ? \
             WHERE id = ?",
        )
        .bind(status)
        .bind(why)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn retry_delivery(&self, id: &str, why: &str, next_at: i64, now: i64) -> Result<()> {
        sqlx::query(
            "UPDATE notification_deliveries SET last_error = ?, next_attempt_at = ?, \
             updated_at = ? WHERE id = ?",
        )
        .bind(why)
        .bind(next_at)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn sent_count_since(&self, user_id: &str, since: i64) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM notification_deliveries WHERE user_id = ? AND channel = 'email' \
             AND status = 'sent' AND updated_at >= ?",
        )
        .bind(user_id)
        .bind(since)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    /// Delete expired personal items and old idempotency rows (batched).
    pub async fn cleanup(
        &self,
        read_before: i64,
        unread_before: i64,
        archived_before: i64,
        applied_before: i64,
    ) -> Result<CleanupStats> {
        let mut stats = CleanupStats::default();
        for _ in 0..100 {
            let n = sqlx::query(
                "DELETE FROM notification_items \
                 WHERE (read_at IS NOT NULL AND read_at < ?) \
                    OR (archived_at IS NOT NULL AND archived_at < ?) \
                    OR updated_at < ? LIMIT 5000",
            )
            .bind(read_before)
            .bind(archived_before)
            .bind(unread_before)
            .execute(&self.pool)
            .await?
            .rows_affected();
            stats.items += n;
            if n < 5000 {
                break;
            }
        }
        // Finished email deliveries are only kept for the daily-cap window plus a margin.
        sqlx::query(
            "DELETE FROM notification_deliveries WHERE status <> 'pending' AND updated_at < ?",
        )
        .bind(applied_before)
        .execute(&self.pool)
        .await?;
        for _ in 0..100 {
            let n = sqlx::query(
                "DELETE FROM notification_applied_events WHERE applied_at < ? LIMIT 5000",
            )
            .bind(applied_before)
            .execute(&self.pool)
            .await?
            .rows_affected();
            stats.applied_events += n;
            if n < 5000 {
                break;
            }
        }
        Ok(stats)
    }
}

/// Append the `VISIBLE_TO_USER` condition with its three binds.
fn push_visible(qb: &mut QueryBuilder<'_, MySql>, user_id: &str, now: i64) {
    let mut parts = VISIBLE_TO_USER.split('?');
    let binds: [Bind; 3] = [
        Bind::I64(now),
        Bind::I64(now),
        Bind::Str(user_id.to_string()),
    ];
    for b in binds {
        qb.push(parts.next().unwrap_or(""));
        match b {
            Bind::I64(v) => {
                qb.push_bind(v);
            }
            Bind::Str(v) => {
                qb.push_bind(v);
            }
        }
    }
    qb.push(parts.next().unwrap_or(""));
}

enum Bind {
    I64(i64),
    Str(String),
}

async fn insert_audience(
    tx: &mut sqlx::Transaction<'_, MySql>,
    broadcast_id: &str,
    user_ids: &[String],
) -> Result<()> {
    for chunk in user_ids.chunks(500) {
        let mut qb = QueryBuilder::<MySql>::new(
            "INSERT INTO notification_broadcast_audience (broadcast_id, user_id) ",
        );
        qb.push_values(chunk, |mut row, uid| {
            row.push_bind(broadcast_id.to_string())
                .push_bind(uid.clone());
        });
        qb.build().execute(&mut **tx).await?;
    }
    Ok(())
}
