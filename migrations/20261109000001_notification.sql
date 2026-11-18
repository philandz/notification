-- Notification service (plan infra/.ai/tasks/plans/2026-11-09-notification-plan.md).
-- Timestamps are unix seconds. JSON payloads are stored as TEXT (portable, never queried).

-- Personal notifications: one row per recipient.
CREATE TABLE IF NOT EXISTS notification_items (
    id                VARCHAR(36)  NOT NULL PRIMARY KEY,
    recipient_user_id VARCHAR(36)  NOT NULL,
    org_id            VARCHAR(36)  DEFAULT NULL,
    budget_id         VARCHAR(36)  DEFAULT NULL,
    category          VARCHAR(32)  NOT NULL,
    event_type        VARCHAR(64)  NOT NULL,
    severity          VARCHAR(16)  NOT NULL DEFAULT 'info',
    actor_user_id     VARCHAR(36)  DEFAULT NULL,
    params_json       TEXT         NOT NULL,
    link              VARCHAR(512) DEFAULT NULL,
    fallback_text     VARCHAR(512) NOT NULL,
    source_event_id   VARCHAR(36)  NOT NULL,
    dedup_key         VARCHAR(191) DEFAULT NULL,
    group_key         VARCHAR(191) DEFAULT NULL,
    group_count       INT          NOT NULL DEFAULT 1,
    created_at        BIGINT       NOT NULL,
    updated_at        BIGINT       NOT NULL,
    read_at           BIGINT       DEFAULT NULL,
    archived_at       BIGINT       DEFAULT NULL,
    UNIQUE KEY uq_notification_dedup (recipient_user_id, dedup_key),
    KEY idx_notification_inbox (recipient_user_id, archived_at, updated_at, id),
    KEY idx_notification_unread (recipient_user_id, archived_at, read_at),
    KEY idx_notification_group (recipient_user_id, group_key, read_at),
    KEY idx_notification_cleanup (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Idempotency guard for Publish: one row per (event, recipient) already applied.
CREATE TABLE IF NOT EXISTS notification_applied_events (
    event_id          VARCHAR(36) NOT NULL,
    recipient_user_id VARCHAR(36) NOT NULL,
    applied_at        BIGINT      NOT NULL,
    PRIMARY KEY (event_id, recipient_user_id),
    KEY idx_notification_applied_at (applied_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- System notifications (broadcasts). Stored once, read by every user in the audience.
CREATE TABLE IF NOT EXISTS notification_broadcasts (
    id                 VARCHAR(36)  NOT NULL PRIMARY KEY,
    audience_kind      VARCHAR(16)  NOT NULL,
    audience_orgs_json TEXT         DEFAULT NULL,
    severity           VARCHAR(16)  NOT NULL DEFAULT 'info',
    title_vi           VARCHAR(200) NOT NULL,
    body_vi            TEXT         NOT NULL,
    title_en           VARCHAR(200) NOT NULL,
    body_en            TEXT         NOT NULL,
    link               VARCHAR(512) DEFAULT NULL,
    status             VARCHAR(16)  NOT NULL,
    publish_at         BIGINT       DEFAULT NULL,
    expires_at         BIGINT       DEFAULT NULL,
    show_banner        BOOLEAN      NOT NULL DEFAULT FALSE,
    created_by         VARCHAR(36)  NOT NULL,
    created_at         BIGINT       NOT NULL,
    updated_at         BIGINT       NOT NULL,
    KEY idx_notification_broadcast_active (status, publish_at, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS notification_broadcast_audience (
    broadcast_id VARCHAR(36) NOT NULL,
    user_id      VARCHAR(36) NOT NULL,
    PRIMARY KEY (broadcast_id, user_id),
    KEY idx_notification_broadcast_audience_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS notification_broadcast_states (
    broadcast_id VARCHAR(36) NOT NULL,
    user_id      VARCHAR(36) NOT NULL,
    read_at      BIGINT      DEFAULT NULL,
    dismissed_at BIGINT      DEFAULT NULL,
    PRIMARY KEY (broadcast_id, user_id),
    KEY idx_notification_broadcast_states_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Only the cells a user changed away from the default are stored.
CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id    VARCHAR(36) NOT NULL,
    category   VARCHAR(32) NOT NULL,
    channel    VARCHAR(16) NOT NULL,
    enabled    BOOLEAN     NOT NULL,
    updated_at BIGINT      NOT NULL,
    PRIMARY KEY (user_id, category, channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS notification_budget_mutes (
    user_id    VARCHAR(36) NOT NULL,
    budget_id  VARCHAR(36) NOT NULL,
    created_at BIGINT      NOT NULL,
    PRIMARY KEY (user_id, budget_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
