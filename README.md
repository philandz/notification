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
