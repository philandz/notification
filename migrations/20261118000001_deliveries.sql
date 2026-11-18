-- Phase 4: email deliveries (plan 2026-11-09). One row per (user, channel, delivery_key).
-- The row is a self-contained snapshot, so email does not depend on the in-app item (the user
-- may have turned the in-app channel off). Timestamps are unix seconds.
CREATE TABLE IF NOT EXISTS notification_deliveries (
    id                  VARCHAR(36)  NOT NULL PRIMARY KEY,
    user_id             VARCHAR(36)  NOT NULL,
    channel             VARCHAR(16)  NOT NULL,
    delivery_key        VARCHAR(191) NOT NULL,
    source_event_id     VARCHAR(36)  NOT NULL,
    category            VARCHAR(32)  NOT NULL,
    event_type          VARCHAR(64)  NOT NULL,
    severity            VARCHAR(16)  NOT NULL DEFAULT 'info',
    params_json         TEXT         NOT NULL,
    link                VARCHAR(512) DEFAULT NULL,
    fallback_text       VARCHAR(512) NOT NULL,
    status              VARCHAR(16)  NOT NULL DEFAULT 'pending',
    attempts            INT          NOT NULL DEFAULT 0,
    next_attempt_at     BIGINT       NOT NULL,
    last_error          TEXT         DEFAULT NULL,
    provider_message_id VARCHAR(128) DEFAULT NULL,
    created_at          BIGINT       NOT NULL,
    updated_at          BIGINT       NOT NULL,
    UNIQUE KEY uq_delivery (user_id, channel, delivery_key),
    KEY idx_delivery_due (status, next_attempt_at),
    KEY idx_delivery_sent (user_id, status, updated_at),
    KEY idx_delivery_cleanup (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
