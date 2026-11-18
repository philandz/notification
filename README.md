# notification

Philandz notification service: personal inbox, system broadcasts (super-admin), preferences and per-budget mutes.

- gRPC `service.notification.NotificationService` on `:50109` (proto: `protobuf/notification/notification.proto`)
- HTTP `:9109` exposes `/health` only. Business endpoints are reached through the gateway (`/api/notification/*`).
- MySQL 8, migrations in `./migrations` run on startup.

## Model
- Personal notifications: fan-out on write (one row per recipient). Content is `event_type` + `params_json` + `link` + `fallback_text`; the web renders it via i18n.
- System broadcasts: one row + per-user state (fan-out on read), authored in vi+en by the super admin.
- `Publish` (internal, idempotent on `event_id`) is called by producers' outbox drainers (`libs/event`). It requires header `x-internal-token` == `INTERNAL_SERVICE_TOKEN`, and is never exposed by the gateway.
- `dedup_key` is unique per recipient; `group_key` collapses unread items within `NOTIFICATION_GROUP_WINDOW_SECS`.
- Security category and critical severity bypass preferences/mutes. Links must be relative (`/path`).
- Cleanup task: read 90d, unread 180d, archived 30d, applied events 14d.

## Email channel (Phase 4)
Email is **opt-in** because the provider (Resend) bills per message: in-app is on by default, email is
off for every category except `security`. Users switch it per category in `/settings/notifications`
(the Email column only appears when the channel is active). Nothing is sent unless
`NOTIFY_EMAIL_ENABLED=true` **and** `RESEND_API_KEY` is set; with either missing the service behaves
exactly as before.

- On `Publish`, a recipient who opted in gets a row in `notification_deliveries` (idempotent per
  `dedup_key` or `event_id`). It is independent of the in-app setting.
- A worker (every `NOTIFY_EMAIL_INTERVAL_SECS`) claims due rows, resolves email + locale through
  identity's internal `GetUserContacts` RPC (`x-internal-token`), renders vi/en and sends with
  `libs/notify::ResendMailer`. Retries use backoff (1m, 4m, 16m… up to 6h) and stop after
  `NOTIFY_EMAIL_MAX_ATTEMPTS`; 4xx from Resend (other than 429) fail immediately.
- `NOTIFY_EMAIL_DAILY_CAP` (default 3) limits emails per user per 24h; extra ones are marked `skipped`.
- Every email links to a signed one-click unsubscribe (`GET /api/notification/unsubscribe?token=` shows a
  confirm button, `POST` applies it and turns that category's email off).
- Inspect: `SELECT status, last_error, COUNT(*) FROM notification_deliveries GROUP BY 1,2;`

## Run locally
```bash
cp .env.example .env     # then edit values
export $(grep -v '^#' .env | sed 's/ *#.*//' | xargs)
cargo run
curl localhost:9109/health
```

## Tests
```bash
# unit + validation
cargo test
# DB-backed + gRPC tests (need a MySQL database, tests create their own rows)
DATABASE_URL=mysql://phil:phil@127.0.0.1:3306/philand_test cargo test -- --include-ignored
cargo clippy --all-targets -- -D warnings
```

## Producers
Producers enable publishing with `NOTIFY_PUBLISH_ENABLED=true`, `NOTIFICATION_GRPC_URL`, `INTERNAL_SERVICE_TOKEN`; see `libs/event`.
