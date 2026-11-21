//! DB-backed: `DATABASE_URL=mysql://… cargo test --test notification_test -- --include-ignored`
//!
//! Tests share one database and run in parallel, so every test uses fresh user ids and
//! only asserts on its own rows. Broadcasts for ALL users are global: those tests look for
//! their own broadcast id instead of counting.
use notification::config::NotificationTuning;
use notification::manager::biz::NotificationBiz;
use notification::manager::repository::NotificationRepository;
use notification::pb::service::notification::{
    AdminListBroadcastsRequest, ArchiveRequest, BroadcastAudience, BroadcastInput, BroadcastStatus,
    InboxFilter, ListInboxRequest, MarkAllReadRequest, MarkReadRequest, NotificationCategory,
    NotificationChannel, NotificationKind, NotificationSeverity, PreferenceCell, PublishRequest,
    UpdatePreferencesRequest,
};
use tonic::Code;

const T0: i64 = 1_800_000_000;
/// Broadcasts for ALL users are global, so the one test that publishes them lives in its own
/// time window (about three years later). Every other test queries around `T0` and never
/// sees them, even while that test runs in parallel.
const TA: i64 = T0 + 100_000_000;

async fn biz() -> NotificationBiz {
    static INIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT.lock().await;
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let repo = NotificationRepository::new(&url, "./migrations")
        .await
        .expect("repo");
    NotificationBiz::new(repo, "tok".into(), NotificationTuning::default())
}

fn uid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn event(recipients: &[&str]) -> PublishRequest {
    PublishRequest {
        event_id: uid(),
        source: "budget".into(),
        r#type: "debt.payment_due".into(),
        category: NotificationCategory::Reminder as i32,
        severity: NotificationSeverity::Info as i32,
        recipient_user_ids: recipients.iter().map(|s| s.to_string()).collect(),
        fallback_text: "Payment due".into(),
        params_json: r#"{"n":1}"#.into(),
        ..Default::default()
    }
}

fn inbox(limit: i32) -> ListInboxRequest {
    ListInboxRequest {
        limit,
        locale: "en".into(),
        ..Default::default()
    }
}

fn broadcast(audience: BroadcastAudience, users: &[&str]) -> BroadcastInput {
    BroadcastInput {
        audience: audience as i32,
        audience_user_ids: users.iter().map(|s| s.to_string()).collect(),
        severity: NotificationSeverity::Info as i32,
        title_vi: "Tiêu đề".into(),
        body_vi: "Nội dung".into(),
        title_en: "Title".into(),
        body_en: "Body".into(),
        ..Default::default()
    }
}

fn cell(category: NotificationCategory, enabled: bool) -> PreferenceCell {
    PreferenceCell {
        category: category as i32,
        channel: NotificationChannel::InApp as i32,
        enabled,
        locked: false,
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn publish_is_idempotent_per_event_and_recipient() {
    let biz = biz().await;
    let (a, b) = (uid(), uid());
    let mut ev = event(&[&a, &b]);
    ev.link = "/budgets/x/debt".into();
    let r = biz.publish_at(ev.clone(), T0).await.unwrap();
    assert_eq!((r.created, r.collapsed, r.skipped), (2, 0, 0));

    // The producer retries the same event: nothing new.
    let r = biz.publish_at(ev.clone(), T0 + 5).await.unwrap();
    assert_eq!((r.created, r.collapsed, r.skipped), (0, 0, 2));

    let page = biz.list_inbox_at(&a, inbox(10), T0 + 10).await.unwrap();
    assert_eq!(page.items.len(), 1);
    let item = &page.items[0];
    assert_eq!(item.r#type, "debt.payment_due");
    assert_eq!(item.link, "/budgets/x/debt");
    assert_eq!(item.params_json, r#"{"n":1}"#);
    assert_eq!(item.read_at, 0);
    assert_eq!(item.kind, NotificationKind::Personal as i32);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn the_actor_never_gets_their_own_notification() {
    let biz = biz().await;
    let (actor, other) = (uid(), uid());
    let mut ev = event(&[&actor, &other]);
    ev.actor_user_id = actor.clone();
    let r = biz.publish_at(ev, T0).await.unwrap();
    assert_eq!(r.created, 1);
    assert!(biz
        .list_inbox_at(&actor, inbox(10), T0)
        .await
        .unwrap()
        .items
        .is_empty());
    assert_eq!(
        biz.list_inbox_at(&other, inbox(10), T0)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn dedup_key_allows_one_item_per_recipient() {
    let biz = biz().await;
    let u = uid();
    let key = format!("debt:{}:2026-12:d3", uid());
    let mut first = event(&[&u]);
    first.dedup_key = key.clone();
    let mut second = event(&[&u]); // a different event with the same key (next sweep run)
    second.dedup_key = key;
    assert_eq!(biz.publish_at(first, T0).await.unwrap().created, 1);
    let r = biz.publish_at(second, T0 + 60).await.unwrap();
    assert_eq!((r.created, r.skipped), (0, 1));
    assert_eq!(
        biz.list_inbox_at(&u, inbox(10), T0 + 60)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn group_key_collapses_unread_items_inside_the_window() {
    let biz = biz().await;
    let u = uid();
    let group = format!("sharing:{}:expense", uid());
    let with = |n: i32| {
        let mut ev = event(&[&u]);
        ev.group_key = group.clone();
        ev.params_json = format!(r#"{{"n":{n}}}"#);
        ev.fallback_text = format!("{n} expenses");
        ev
    };

    assert_eq!(biz.publish_at(with(1), T0).await.unwrap().created, 1);
    assert_eq!(biz.publish_at(with(2), T0 + 60).await.unwrap().collapsed, 1);
    assert_eq!(
        biz.publish_at(with(3), T0 + 120).await.unwrap().collapsed,
        1
    );

    let items = biz
        .list_inbox_at(&u, inbox(10), T0 + 120)
        .await
        .unwrap()
        .items;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].group_count, 3);
    assert_eq!(items[0].params_json, r#"{"n":3}"#, "latest params win");
    assert_eq!(items[0].fallback_text, "3 expenses");
    assert_eq!(items[0].created_at, T0);
    assert_eq!(items[0].updated_at, T0 + 120);

    // Retrying the same event must not bump the counter again.
    let again = with(4);
    assert_eq!(
        biz.publish_at(again.clone(), T0 + 150)
            .await
            .unwrap()
            .collapsed,
        1
    );
    assert_eq!(biz.publish_at(again, T0 + 160).await.unwrap().skipped, 1);
    let items = biz
        .list_inbox_at(&u, inbox(10), T0 + 160)
        .await
        .unwrap()
        .items;
    assert_eq!(items[0].group_count, 4);

    // Outside the 15 minute window a new item starts.
    let late = biz
        .publish_at(with(5), T0 + 150 + 15 * 60 + 1)
        .await
        .unwrap();
    assert_eq!(late.created, 1);
    assert_eq!(
        biz.list_inbox_at(&u, inbox(10), T0 + 2000)
            .await
            .unwrap()
            .items
            .len(),
        2
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_read_item_is_not_collapsed_into() {
    let biz = biz().await;
    let u = uid();
    let group = uid();
    let mut first = event(&[&u]);
    first.group_key = group.clone();
    biz.publish_at(first, T0).await.unwrap();
    biz.mark_all_read_at(&u, MarkAllReadRequest::default(), T0 + 1)
        .await
        .unwrap();

    let mut second = event(&[&u]);
    second.group_key = group;
    assert_eq!(biz.publish_at(second, T0 + 2).await.unwrap().created, 1);
    // Only the new item is unread (a parallel test may have a global broadcast live, so
    // look at the personal count, not the total).
    assert_eq!(biz.unread_count_at(&u, T0 + 3).await.unwrap().personal, 1);
    let personal = ListInboxRequest {
        kind: NotificationKind::Personal as i32,
        ..inbox(10)
    };
    assert_eq!(
        biz.list_inbox_at(&u, personal, T0 + 3)
            .await
            .unwrap()
            .items
            .len(),
        2
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn disabled_categories_and_muted_budgets_are_skipped() {
    let biz = biz().await;
    let (quiet, normal) = (uid(), uid());
    let budget = uid();

    biz.update_preferences_at(
        &quiet,
        UpdatePreferencesRequest {
            cells: vec![cell(NotificationCategory::Activity, false)],
        },
        T0,
    )
    .await
    .unwrap();

    let mut ev = event(&[&quiet, &normal]);
    ev.category = NotificationCategory::Activity as i32;
    ev.budget_id = budget.clone();
    let r = biz.publish_at(ev, T0).await.unwrap();
    assert_eq!((r.created, r.skipped), (1, 1));
    assert!(biz
        .list_inbox_at(&quiet, inbox(5), T0)
        .await
        .unwrap()
        .items
        .is_empty());

    // Other categories still reach the user who turned activity off.
    let r = biz.publish_at(event(&[&quiet]), T0).await.unwrap();
    assert_eq!(r.created, 1);

    // Mute a budget: all its non-critical notifications are skipped, other budgets are not.
    biz.set_budget_mute(&normal, &budget, true).await.unwrap();
    let mut ev = event(&[&normal]);
    ev.budget_id = budget.clone();
    assert_eq!(biz.publish_at(ev, T0 + 1).await.unwrap().skipped, 1);
    let mut other_budget = event(&[&normal]);
    other_budget.budget_id = uid();
    assert_eq!(
        biz.publish_at(other_budget, T0 + 1).await.unwrap().created,
        1
    );

    biz.set_budget_mute(&normal, &budget, false).await.unwrap();
    let mut ev = event(&[&normal]);
    ev.budget_id = budget;
    assert_eq!(biz.publish_at(ev, T0 + 2).await.unwrap().created, 1);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn critical_and_security_ignore_preferences_and_mutes() {
    let biz = biz().await;
    let u = uid();
    let budget = uid();
    biz.update_preferences_at(
        &u,
        UpdatePreferencesRequest {
            cells: vec![
                cell(NotificationCategory::Alert, false),
                cell(NotificationCategory::Reminder, false),
            ],
        },
        T0,
    )
    .await
    .unwrap();
    biz.set_budget_mute(&u, &budget, true).await.unwrap();

    let mut critical = event(&[&u]);
    critical.category = NotificationCategory::Alert as i32;
    critical.severity = NotificationSeverity::Critical as i32;
    critical.budget_id = budget.clone();
    assert_eq!(biz.publish_at(critical, T0).await.unwrap().created, 1);

    let mut security = event(&[&u]);
    security.category = NotificationCategory::Security as i32;
    security.budget_id = budget.clone();
    assert_eq!(biz.publish_at(security, T0).await.unwrap().created, 1);

    let mut ordinary = event(&[&u]);
    ordinary.category = NotificationCategory::Alert as i32;
    ordinary.budget_id = budget;
    assert_eq!(biz.publish_at(ordinary, T0).await.unwrap().skipped, 1);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn preferences_matrix_defaults_locks_and_overrides() {
    let biz = biz().await;
    let u = uid();
    let prefs = biz.preferences(&u).await.unwrap();
    assert_eq!(prefs.cells.len(), 5);
    assert!(prefs
        .cells
        .iter()
        .all(|c| c.enabled && c.channel == NotificationChannel::InApp as i32));
    let locked: Vec<_> = prefs.cells.iter().filter(|c| c.locked).collect();
    assert_eq!(locked.len(), 1);
    assert_eq!(locked[0].category, NotificationCategory::Security as i32);

    let prefs = biz
        .update_preferences_at(
            &u,
            UpdatePreferencesRequest {
                cells: vec![cell(NotificationCategory::Reminder, false)],
            },
            T0,
        )
        .await
        .unwrap();
    let reminder = prefs
        .cells
        .iter()
        .find(|c| c.category == NotificationCategory::Reminder as i32)
        .unwrap();
    assert!(!reminder.enabled);

    // Back to the default removes the stored override.
    biz.update_preferences_at(
        &u,
        UpdatePreferencesRequest {
            cells: vec![cell(NotificationCategory::Reminder, true)],
        },
        T0 + 1,
    )
    .await
    .unwrap();
    assert!(biz
        .repo()
        .preference_overrides(&u)
        .await
        .unwrap()
        .is_empty());

    for bad in [
        cell(NotificationCategory::Security, false), // locked
        cell(NotificationCategory::System, true),    // not a personal category
        PreferenceCell {
            category: NotificationCategory::Activity as i32,
            channel: NotificationChannel::Email as i32, // not available yet
            enabled: false,
            locked: false,
        },
    ] {
        let err = biz
            .update_preferences_at(&u, UpdatePreferencesRequest { cells: vec![bad] }, T0)
            .await
            .unwrap_err();
        assert!(matches!(
            err.code(),
            Code::FailedPrecondition | Code::InvalidArgument
        ));
    }
    // A rejected request writes nothing, even for its valid cells.
    let err = biz
        .update_preferences_at(
            &u,
            UpdatePreferencesRequest {
                cells: vec![
                    cell(NotificationCategory::Activity, false),
                    cell(NotificationCategory::Security, false),
                ],
            },
            T0,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(biz
        .repo()
        .preference_overrides(&u)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn read_state_archive_and_unread_count() {
    let biz = biz().await;
    let u = uid();
    for i in 0..3 {
        biz.publish_at(event(&[&u]), T0 + i).await.unwrap();
    }
    let page = biz.list_inbox_at(&u, inbox(10), T0 + 10).await.unwrap();
    assert_eq!(page.items.len(), 3);
    assert_eq!(biz.unread_count_at(&u, T0 + 10).await.unwrap().total, 3);

    biz.mark_read_at(
        &u,
        MarkReadRequest {
            item_ids: vec![page.items[0].id.clone()],
            broadcast_ids: vec![],
        },
        T0 + 11,
    )
    .await
    .unwrap();
    assert_eq!(biz.unread_count_at(&u, T0 + 12).await.unwrap().total, 2);

    let unread = biz
        .list_inbox_at(
            &u,
            ListInboxRequest {
                filter: InboxFilter::Unread as i32,
                ..inbox(10)
            },
            T0 + 12,
        )
        .await
        .unwrap();
    assert_eq!(unread.items.len(), 2);

    biz.archive_at(
        &u,
        ArchiveRequest {
            item_ids: vec![page.items[1].id.clone()],
            broadcast_ids: vec![],
        },
        T0 + 13,
    )
    .await
    .unwrap();
    assert_eq!(
        biz.list_inbox_at(&u, inbox(10), T0 + 14)
            .await
            .unwrap()
            .items
            .len(),
        2
    );
    assert_eq!(biz.unread_count_at(&u, T0 + 14).await.unwrap().total, 1);

    biz.mark_all_read_at(&u, MarkAllReadRequest::default(), T0 + 15)
        .await
        .unwrap();
    assert_eq!(biz.unread_count_at(&u, T0 + 16).await.unwrap().total, 0);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn users_cannot_touch_each_others_items() {
    let biz = biz().await;
    let (owner, intruder) = (uid(), uid());
    biz.publish_at(event(&[&owner]), T0).await.unwrap();
    let id = biz.list_inbox_at(&owner, inbox(5), T0).await.unwrap().items[0]
        .id
        .clone();

    biz.mark_read_at(
        &intruder,
        MarkReadRequest {
            item_ids: vec![id.clone()],
            broadcast_ids: vec![],
        },
        T0 + 1,
    )
    .await
    .unwrap();
    biz.archive_at(
        &intruder,
        ArchiveRequest {
            item_ids: vec![id],
            broadcast_ids: vec![],
        },
        T0 + 1,
    )
    .await
    .unwrap();
    let items = biz
        .list_inbox_at(&owner, inbox(5), T0 + 2)
        .await
        .unwrap()
        .items;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].read_at, 0);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn inbox_pages_personal_and_system_in_one_stream() {
    let biz = biz().await;
    let (u, other, admin) = (uid(), uid(), uid());
    let mut ids = Vec::new();
    for i in 0..3 {
        biz.publish_at(event(&[&u]), T0 + 100 + i * 10)
            .await
            .unwrap();
    }
    for i in 0..2 {
        let mut input = broadcast(BroadcastAudience::Users, &[&u]);
        input.title_en = format!("Notice {i}");
        let b = biz
            .admin_create_broadcast_at(&admin, input, T0)
            .await
            .unwrap();
        biz.admin_publish_broadcast_at(&b.id, T0 + 105 + i * 10)
            .await
            .unwrap();
        ids.push(b.id);
    }

    // Walk the whole stream two at a time.
    let mut seen = Vec::new();
    let mut cursor = String::new();
    loop {
        let page = biz
            .list_inbox_at(
                &u,
                ListInboxRequest {
                    cursor: cursor.clone(),
                    ..inbox(2)
                },
                T0 + 1000,
            )
            .await
            .unwrap();
        seen.extend(
            page.items
                .iter()
                .map(|i| (i.updated_at, i.id.clone(), i.kind)),
        );
        if page.next_cursor.is_empty() {
            break;
        }
        cursor = page.next_cursor;
        assert!(seen.len() < 20, "cursor does not advance");
    }
    assert_eq!(seen.len(), 5);
    let mut sorted = seen.clone();
    sorted.sort_by(|a, b| (b.0, &b.1).cmp(&(a.0, &a.1)));
    assert_eq!(seen, sorted, "newest first, no duplicates");
    assert_eq!(
        seen.iter()
            .filter(|s| s.2 == NotificationKind::System as i32)
            .count(),
        2
    );

    // Kind filter and locale.
    let only_system = biz
        .list_inbox_at(
            &u,
            ListInboxRequest {
                kind: NotificationKind::System as i32,
                locale: "vi".into(),
                ..inbox(10)
            },
            T0 + 1000,
        )
        .await
        .unwrap();
    assert_eq!(only_system.items.len(), 2);
    assert_eq!(only_system.items[0].title, "Tiêu đề");
    assert_eq!(only_system.items[0].r#type, "system.broadcast");

    // Someone outside the audience sees none of it.
    assert!(biz
        .list_inbox_at(&other, inbox(10), T0 + 1000)
        .await
        .unwrap()
        .items
        .is_empty());
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn broadcast_lifecycle_visibility_and_read_state() {
    let biz = biz().await;
    let (a, b, outsider, admin) = (uid(), uid(), uid(), uid());

    let created = biz
        .admin_create_broadcast_at(&admin, broadcast(BroadcastAudience::Users, &[&a, &b]), T0)
        .await
        .unwrap();
    assert_eq!(created.status, BroadcastStatus::Draft as i32);
    assert_eq!(created.created_by, admin);
    assert_eq!(created.audience_user_ids.len(), 2);
    // A draft is invisible.
    assert_eq!(biz.unread_count_at(&a, T0 + 1).await.unwrap().system, 0);

    // Editing a draft works.
    let mut edited = broadcast(BroadcastAudience::Users, &[&a, &b]);
    edited.title_en = "Edited".into();
    let created = biz
        .admin_update_broadcast_at(&created.id, edited, T0 + 2)
        .await
        .unwrap();
    assert_eq!(created.title_en, "Edited");

    let live = biz
        .admin_publish_broadcast_at(&created.id, T0 + 10)
        .await
        .unwrap();
    assert_eq!(live.status, BroadcastStatus::Published as i32);
    assert_eq!(live.publish_at, T0 + 10);

    // Audience members see it, outsiders do not.
    assert_eq!(biz.unread_count_at(&a, T0 + 11).await.unwrap().system, 1);
    assert_eq!(
        biz.unread_count_at(&outsider, T0 + 11)
            .await
            .unwrap()
            .system,
        0
    );

    // Reading is per user.
    biz.mark_read_at(
        &a,
        MarkReadRequest {
            item_ids: vec![],
            broadcast_ids: vec![live.id.clone()],
        },
        T0 + 12,
    )
    .await
    .unwrap();
    assert_eq!(biz.unread_count_at(&a, T0 + 13).await.unwrap().system, 0);
    assert_eq!(biz.unread_count_at(&b, T0 + 13).await.unwrap().system, 1);
    let item = &biz
        .list_inbox_at(&a, inbox(5), T0 + 13)
        .await
        .unwrap()
        .items[0];
    assert_eq!(item.read_at, T0 + 12);
    // Marking a broadcast the user cannot see read is ignored.
    biz.mark_read_at(
        &outsider,
        MarkReadRequest {
            item_ids: vec![],
            broadcast_ids: vec![live.id.clone()],
        },
        T0 + 14,
    )
    .await
    .unwrap();
    let listed = biz
        .admin_list_broadcasts_at(
            AdminListBroadcastsRequest {
                status: BroadcastStatus::Published as i32,
                page: 1,
                limit: 100,
            },
            T0 + 15,
        )
        .await
        .unwrap();
    let row = listed
        .broadcasts
        .iter()
        .find(|x| x.id == live.id)
        .expect("listed");
    assert_eq!(
        row.read_count, 1,
        "only the audience member who read it counts"
    );

    // A published broadcast is final: no edit, no second publish.
    assert_eq!(
        biz.admin_update_broadcast_at(&live.id, broadcast(BroadcastAudience::All, &[]), T0 + 16)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        biz.admin_publish_broadcast_at(&live.id, T0 + 16)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );

    // Archiving withdraws it for everyone, and is idempotent.
    let archived = biz
        .admin_archive_broadcast_at(&live.id, T0 + 20)
        .await
        .unwrap();
    assert_eq!(archived.status, BroadcastStatus::Archived as i32);
    biz.admin_archive_broadcast_at(&live.id, T0 + 21)
        .await
        .unwrap();
    assert_eq!(biz.unread_count_at(&b, T0 + 22).await.unwrap().system, 0);
    assert_eq!(
        biz.admin_publish_broadcast_at(&live.id, T0 + 23)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        biz.admin_publish_broadcast_at("does-not-exist", T0)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn scheduled_and_expiring_broadcasts_follow_the_clock() {
    let biz = biz().await;
    let (u, admin) = (uid(), uid());

    let mut input = broadcast(BroadcastAudience::Users, &[&u]);
    input.publish_at = T0 + 1000;
    input.expires_at = T0 + 2000;
    let b = biz
        .admin_create_broadcast_at(&admin, input.clone(), T0)
        .await
        .unwrap();
    let scheduled = biz.admin_publish_broadcast_at(&b.id, T0 + 1).await.unwrap();
    assert_eq!(scheduled.status, BroadcastStatus::Scheduled as i32);

    assert_eq!(
        biz.unread_count_at(&u, T0 + 999).await.unwrap().system,
        0,
        "not yet"
    );
    assert_eq!(
        biz.unread_count_at(&u, T0 + 1000).await.unwrap().system,
        1,
        "now live"
    );
    assert_eq!(biz.unread_count_at(&u, T0 + 1999).await.unwrap().system, 1);
    assert_eq!(
        biz.unread_count_at(&u, T0 + 2000).await.unwrap().system,
        0,
        "expired"
    );

    // While still in the future a scheduled broadcast can be edited, but only with a future time.
    let b2 = biz
        .admin_create_broadcast_at(&admin, input.clone(), T0)
        .await
        .unwrap();
    biz.admin_publish_broadcast_at(&b2.id, T0 + 1)
        .await
        .unwrap();
    let mut soon = input.clone();
    soon.publish_at = T0 + 1500;
    soon.expires_at = T0 + 2500;
    biz.admin_update_broadcast_at(&b2.id, soon, T0 + 2)
        .await
        .unwrap();
    let mut past = input.clone();
    past.publish_at = T0;
    assert_eq!(
        biz.admin_update_broadcast_at(&b2.id, past, T0 + 3)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    // Once its time has come it counts as published and is locked.
    assert_eq!(
        biz.admin_update_broadcast_at(&b2.id, input, T0 + 1600)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // "Publish now" sends a still-scheduled broadcast immediately.
    let mut later = broadcast(BroadcastAudience::Users, &[&u]);
    later.publish_at = T0 + 9000;
    let b3 = biz
        .admin_create_broadcast_at(&admin, later, T0)
        .await
        .unwrap();
    biz.admin_publish_broadcast_at(&b3.id, T0 + 1)
        .await
        .unwrap();
    let now_live = biz
        .admin_publish_broadcast_at(&b3.id, T0 + 50)
        .await
        .unwrap();
    assert_eq!(now_live.status, BroadcastStatus::Published as i32);
    assert_eq!(now_live.publish_at, T0 + 50);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn all_audience_reaches_every_user_and_dismiss_respects_critical() {
    let biz = biz().await;
    let (u1, u2, admin) = (uid(), uid(), uid());

    let mut normal = broadcast(BroadcastAudience::All, &[]);
    normal.show_banner = true;
    let normal = biz
        .admin_create_broadcast_at(&admin, normal, TA)
        .await
        .unwrap();
    biz.admin_publish_broadcast_at(&normal.id, TA + 1)
        .await
        .unwrap();

    let mut critical = broadcast(BroadcastAudience::All, &[]);
    critical.severity = NotificationSeverity::Critical as i32;
    critical.show_banner = true;
    let critical = biz
        .admin_create_broadcast_at(&admin, critical, TA)
        .await
        .unwrap();
    biz.admin_publish_broadcast_at(&critical.id, TA + 1)
        .await
        .unwrap();

    for user in [&u1, &u2] {
        let ids: Vec<_> = biz
            .list_inbox_at(user, inbox(100), TA + 5)
            .await
            .unwrap()
            .items
            .into_iter()
            .map(|i| i.id)
            .collect();
        assert!(ids.contains(&normal.id) && ids.contains(&critical.id));
    }

    let banners = biz.active_banners_at(&u1, "en", TA + 5).await.unwrap();
    let b_normal = banners
        .iter()
        .find(|b| b.broadcast_id == normal.id)
        .expect("banner");
    let b_critical = banners
        .iter()
        .find(|b| b.broadcast_id == critical.id)
        .expect("banner");
    assert!(b_normal.dismissible);
    assert!(!b_critical.dismissible);
    assert_eq!(b_normal.title, "Title");

    // u1 dismisses both: only the normal one goes away, and only for u1.
    biz.archive_at(
        &u1,
        ArchiveRequest {
            item_ids: vec![],
            broadcast_ids: vec![normal.id.clone(), critical.id.clone()],
        },
        TA + 6,
    )
    .await
    .unwrap();
    let banners = biz.active_banners_at(&u1, "en", TA + 7).await.unwrap();
    assert!(banners.iter().all(|b| b.broadcast_id != normal.id));
    assert!(banners.iter().any(|b| b.broadcast_id == critical.id));
    let u2_banners = biz.active_banners_at(&u2, "vi", TA + 7).await.unwrap();
    assert!(u2_banners.iter().any(|b| b.broadcast_id == normal.id));
    assert_eq!(
        u2_banners
            .iter()
            .find(|b| b.broadcast_id == normal.id)
            .unwrap()
            .title,
        "Tiêu đề"
    );

    // Withdraw so the global rows do not leak into other tests' banner lists.
    biz.admin_archive_broadcast_at(&normal.id, TA + 8)
        .await
        .unwrap();
    biz.admin_archive_broadcast_at(&critical.id, TA + 8)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn mark_all_read_covers_both_kinds_and_can_be_limited() {
    let biz = biz().await;
    let (u, admin) = (uid(), uid());
    biz.publish_at(event(&[&u]), T0).await.unwrap();
    let b = biz
        .admin_create_broadcast_at(&admin, broadcast(BroadcastAudience::Users, &[&u]), T0)
        .await
        .unwrap();
    biz.admin_publish_broadcast_at(&b.id, T0 + 1).await.unwrap();
    assert_eq!(biz.unread_count_at(&u, T0 + 2).await.unwrap().total, 2);

    biz.mark_all_read_at(
        &u,
        MarkAllReadRequest {
            kind: NotificationKind::System as i32,
        },
        T0 + 3,
    )
    .await
    .unwrap();
    let c = biz.unread_count_at(&u, T0 + 4).await.unwrap();
    assert_eq!((c.personal, c.system, c.total), (1, 0, 1));

    biz.mark_all_read_at(&u, MarkAllReadRequest::default(), T0 + 5)
        .await
        .unwrap();
    assert_eq!(biz.unread_count_at(&u, T0 + 6).await.unwrap().total, 0);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn cleanup_removes_old_items_and_keeps_recent_ones() {
    let biz = biz().await;
    let u = uid();
    let day = 24 * 3600;
    // Far in the past relative to "now" used by cleanup below.
    let old_read = event(&[&u]);
    let old_unread = event(&[&u]);
    let fresh = event(&[&u]);
    biz.publish_at(old_read, T0).await.unwrap();
    biz.publish_at(old_unread, T0 + 1).await.unwrap();
    biz.publish_at(fresh, T0 + 200 * day).await.unwrap();

    // Read the first one 100 days after it arrived.
    let items = biz
        .list_inbox_at(&u, inbox(10), T0 + 200 * day)
        .await
        .unwrap()
        .items;
    let first = items
        .iter()
        .find(|i| i.created_at == T0)
        .unwrap()
        .id
        .clone();
    biz.mark_read_at(
        &u,
        MarkReadRequest {
            item_ids: vec![first],
            broadcast_ids: vec![],
        },
        T0 + 100 * day,
    )
    .await
    .unwrap();

    // 200 days in: the read item (read 100 days ago, > 90) and the unread one
    // (updated 200 days ago, > 180) are gone; the fresh one stays.
    let stats = biz.run_cleanup(T0 + 200 * day + 10).await.unwrap();
    assert!(stats.items >= 2);
    let left = biz
        .list_inbox_at(&u, inbox(10), T0 + 200 * day + 10)
        .await
        .unwrap()
        .items;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].created_at, T0 + 200 * day);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn the_inbox_filters_by_category() {
    let biz = biz().await;
    let u = uid();
    biz.publish_at(event(&[&u]), T0).await.unwrap();
    let mut comment = event(&[&u]);
    comment.r#type = "entry.comment_added".into();
    comment.category = NotificationCategory::Activity as i32;
    biz.publish_at(comment, T0 + 1).await.unwrap();

    let only = |c: NotificationCategory| ListInboxRequest {
        category: c as i32,
        ..inbox(10)
    };
    let reminders = biz
        .list_inbox_at(&u, only(NotificationCategory::Reminder), T0 + 2)
        .await
        .unwrap()
        .items;
    assert_eq!(reminders.len(), 1);
    assert_eq!(reminders[0].r#type, "debt.payment_due");

    let activity = biz
        .list_inbox_at(&u, only(NotificationCategory::Activity), T0 + 2)
        .await
        .unwrap()
        .items;
    assert_eq!(activity.len(), 1);
    assert_eq!(activity[0].r#type, "entry.comment_added");

    // SYSTEM means broadcasts only, so none of this user's personal items show up.
    let system = biz
        .list_inbox_at(&u, only(NotificationCategory::System), T0 + 2)
        .await
        .unwrap()
        .items;
    assert!(system
        .iter()
        .all(|i| i.kind == NotificationKind::System as i32));

    assert_eq!(
        biz.list_inbox_at(&u, inbox(10), T0 + 2)
            .await
            .unwrap()
            .items
            .len(),
        2
    );
}
