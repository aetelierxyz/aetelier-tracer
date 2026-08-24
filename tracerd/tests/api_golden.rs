use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use http_body_util::BodyExt;
use tower::ServiceExt;

use tracer_engine::ledger::{
    EVENT_SCHEMA, Event, EventKind, LedgerWriter, Leg, Mode, PostState, Verdict, event_id, trade_id,
};

#[path = "../src/api.rs"]
#[allow(dead_code)]
mod api;
use api::{AppState, CorrectedLive, router};

const FIXED_NOW_MS: u64 = 1_787_600_000_000;

fn fixed_now() -> u64 {
    FIXED_NOW_MS
}

#[allow(clippy::too_many_arguments)]
fn event(
    t_post_ms: u64,
    side: i8,
    seq: u32,
    kind: EventKind,
    leg: Leg,
    ts_ms: u64,
    regime: &str,
) -> Event {
    let tid = trade_id("xyz_TSLA", t_post_ms, side);
    Event {
        schema: EVENT_SCHEMA.into(),
        event_id: event_id(&tid, seq),
        trade_id: tid,
        seq,
        kind,
        leg,
        mode: Mode::Live,
        ts_ms,
        wall_ts_ms: ts_ms + 1_500,
        engine_version: "0.1.0".into(),
        params_version: 1,
        symbol: "xyz_TSLA".into(),
        signal_side: side,
        maker_side: if (side > 0) == matches!(leg, Leg::Entry) {
            "buy".into()
        } else {
            "sell".into()
        },
        decile: if side > 0 { 10 } else { 1 },
        score: 0.87 * side as f64,
        regime: regime.into(),
        posted_px: 330.39,
        qty_usd: 1_000.0,
        window_end_ms: t_post_ms + 900_000,
        post_state: None,
        depth_ahead_usd: None,
        book_ts_ms: None,
        book_staleness_ms: None,
        exit_price_stale: None,
        milestone: None,
        evidence_px: None,
        cum_opp_usd: None,
        filled_touch_ts_ms: None,
        filled_through_ts_ms: None,
        filled_queue_ts_ms: None,
        verdict: None,
        cross_px: None,
        cross_kind: None,
        taker_fee_usd: None,
        gap_flags: vec![],
    }
}

fn build_fixture_ledger(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    let mut w = LedgerWriter::new(dir).unwrap();
    let day1 = FIXED_NOW_MS - 30 * 3_600_000;
    let day2 = FIXED_NOW_MS - 2 * 3_600_000;

    let mut e = event(day1, 1, 0, EventKind::Posted, Leg::Entry, day1, "EXT");
    e.post_state = Some(PostState::Passive);
    w.append(&e).unwrap();
    let mut e = event(
        day1,
        1,
        1,
        EventKind::Expired,
        Leg::Entry,
        day1 + 900_000,
        "EXT",
    );
    e.verdict = Some(Verdict::Unfilled);
    e.filled_touch_ts_ms = Some(day1 + 30_000);
    w.append(&e).unwrap();

    let t2 = day2;
    let mut e = event(t2, -1, 0, EventKind::Posted, Leg::Entry, t2, "RTH");
    e.post_state = Some(PostState::AlreadyThrough);
    e.depth_ahead_usd = Some(12_345.6);
    e.book_ts_ms = Some(t2 - 1_200);
    e.book_staleness_ms = Some(1_200);
    w.append(&e).unwrap();
    let mut e = event(
        t2,
        -1,
        1,
        EventKind::Observed,
        Leg::Entry,
        t2 + 5_000,
        "RTH",
    );
    e.milestone = Some("touch".into());
    e.evidence_px = Some(330.4);
    w.append(&e).unwrap();
    let mut e = event(t2, -1, 2, EventKind::Filled, Leg::Entry, t2 + 9_000, "RTH");
    e.verdict = Some(Verdict::FilledThrough);
    e.milestone = Some("through".into());
    e.filled_touch_ts_ms = Some(t2 + 5_000);
    e.filled_through_ts_ms = Some(t2 + 9_000);
    e.cum_opp_usd = Some(9_000.0);
    w.append(&e).unwrap();
    let mut e = event(t2, -1, 3, EventKind::Posted, Leg::Exit, t2 + 900_000, "RTH");
    e.exit_price_stale = Some(false);
    w.append(&e).unwrap();
    let mut e = event(
        t2,
        -1,
        4,
        EventKind::Expired,
        Leg::Exit,
        t2 + 1_200_000,
        "RTH",
    );
    e.verdict = Some(Verdict::Unfilled);
    w.append(&e).unwrap();
    let mut e = event(
        t2,
        -1,
        5,
        EventKind::TakerCross,
        Leg::Exit,
        t2 + 1_230_000,
        "RTH",
    );
    e.cross_px = Some(329.9);
    e.cross_kind = Some("opposite_print".into());
    e.taker_fee_usd = Some(0.1044);
    w.append(&e).unwrap();

    let t3 = day2 + 3_600_000 / 2;
    let mut e = event(t3, 1, 0, EventKind::Posted, Leg::Entry, t3, "RTH");
    e.post_state = Some(PostState::Passive);
    w.append(&e).unwrap();
    let mut e = event(
        t3,
        1,
        1,
        EventKind::Expired,
        Leg::Entry,
        t3 + 900_000,
        "RTH",
    );
    e.verdict = Some(Verdict::UnfilledGapped);
    e.gap_flags = vec!["gap_overlap".into()];
    w.append(&e).unwrap();

    let t4 = t3 + 60_000;
    let mut e = event(t4, 1, 0, EventKind::Posted, Leg::Entry, t4, "RTH");
    e.post_state = Some(PostState::Passive);
    e.book_ts_ms = Some(t4 - 9_000);
    e.book_staleness_ms = Some(9_000);
    w.append(&e).unwrap();
    let mut e = event(t4, 1, 1, EventKind::Filled, Leg::Entry, t4 + 20_000, "RTH");
    e.verdict = Some(Verdict::FilledQueue);
    e.milestone = Some("queue_cleared".into());
    e.filled_touch_ts_ms = Some(t4 + 11_000);
    e.filled_queue_ts_ms = Some(t4 + 20_000);
    e.cum_opp_usd = Some(15_000.0);
    w.append(&e).unwrap();

    w.flush().unwrap();
}

fn state(dir: &Path) -> AppState {
    AppState {
        status: Arc::new(RwLock::new(status_fixture())),
        corrected: Arc::new(RwLock::new(CorrectedLive {
            n_kept: 2,
            total_usd: -0.35,
            equity: vec![
                (FIXED_NOW_MS - 7_200_000, 0.15),
                (FIXED_NOW_MS - 3_600_000, -0.35),
            ],
        })),
        ready: Arc::new(RwLock::new(true)),
        ledger_dir: dir.to_path_buf(),
        artifacts_dir: None,
        now_ms: fixed_now,
    }
}

fn status_fixture() -> serde_json::Value {
    serde_json::json!({
        "schema": "tracer.status.v1",
        "mode": "live",
        "engine_version": "0.1.0",
        "params_version": 1,
        "symbol": "xyz_TSLA",
        "queue_verdicts": true,
        "trades": {"watermark_ms": FIXED_NOW_MS - 65_000, "last_file": "hyperliquid_xyz-TSLA_trades_sync_20260824_060000.000.parquet", "files_read": 412},
        "books": {"watermark_ms": FIXED_NOW_MS - 62_000, "last_file": "hyperliquid_xyz-TSLA_ob_sync_20260824_060000.000.parquet", "files_read": 977},
        "merged_watermark_ms": FIXED_NOW_MS - 65_000,
        "idle": true,
        "calendar_expired": true,
        "open_trades": 3,
        "counters": {
            "posted": 41, "entry_filled_through": 33, "entry_filled_queue_only": 1,
            "entry_expired_unfilled": 3, "entry_gapped": 1,
            "exit_maker": 25, "exit_taker_cross": 7, "exit_unpriced": 2,
            "would_be_voided_by_paper_filter": 2,
            "queue_vs_through_disagreements": 4, "book_degraded": 5, "duplicates_dropped": 12
        },
        "corrected_n_kept": 32,
        "corrected_total_usd": -14.7,
        "events_written": 208,
        "ready": true
    })
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/api")
}

async fn get_json(app: axum::Router, uri: &str) -> (u16, serde_json::Value) {
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri(uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let code = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::String(
        String::from_utf8_lossy(&bytes).into_owned(),
    ));
    (code, value)
}

fn assert_or_bless(name: &str, got: &serde_json::Value) {
    let path = fixtures_dir().join(name);
    if std::env::var("TRACER_BLESS").is_ok() {
        std::fs::create_dir_all(fixtures_dir()).unwrap();
        std::fs::write(&path, serde_json::to_string_pretty(got).unwrap() + "\n").unwrap();
        return;
    }
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(got, &expected, "{name} drifted from committed golden");
}

#[tokio::test]
async fn api_responses_match_committed_goldens() {
    let dir = std::env::temp_dir().join("tracer_api_golden_ledger");
    build_fixture_ledger(&dir);
    let origins = vec!["http://localhost:3000".to_string()];
    let app = || router(state(&dir), &origins);

    let (code, health) = get_json(app(), "/health").await;
    assert_eq!(code, 200);
    assert_eq!(health, serde_json::Value::String("ok".into()));
    let (code, _) = get_json(app(), "/readyz").await;
    assert_eq!(code, 200);

    let (code, status) = get_json(app(), "/api/tracer/status").await;
    assert_eq!(code, 200);
    assert_or_bless("status.json", &status);

    let (code, page1) = get_json(app(), "/api/tracer/events?limit=6").await;
    assert_eq!(code, 200);
    assert_or_bless("events_page1.json", &page1);
    let cursor = page1["older_cursor"].as_str().unwrap();
    let (code, page2) = get_json(
        app(),
        &format!("/api/tracer/events?limit=6&cursor={cursor}"),
    )
    .await;
    assert_eq!(code, 200);
    assert_or_bless("events_page2.json", &page2);
    assert!(page2["older_cursor"].is_null());
    let total =
        page1["events"].as_array().unwrap().len() + page2["events"].as_array().unwrap().len();
    assert_eq!(total, 12);

    let (code, fr24) = get_json(app(), "/api/tracer/fillrates?window=24h").await;
    assert_eq!(code, 200);
    assert_or_bless("fillrates_24h.json", &fr24);
    let (code, fr_all) = get_json(app(), "/api/tracer/fillrates?window=all").await;
    assert_eq!(code, 200);
    assert_or_bless("fillrates_all.json", &fr_all);
    let (code, _) = get_json(app(), "/api/tracer/fillrates?window=1y").await;
    assert_eq!(code, 400);

    let (code, ledger) = get_json(app(), "/api/tracer/ledger?points=10").await;
    assert_eq!(code, 200);
    assert_or_bless("ledger.json", &ledger);

    let (code, _) = get_json(app(), "/api/tracer/events?cursor=%%%").await;
    assert_eq!(code, 400);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn serve_mode_returns_replay_artifacts_when_present() {
    let dir = std::env::temp_dir().join("tracer_api_serve_artifacts");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("corrected_ledger.json"),
        r#"{"schema":"tracer.corrected.v1","mode":"replay","paper":{"n":14433}}"#,
    )
    .unwrap();
    let mut s = state(&dir);
    s.artifacts_dir = Some(dir.clone());
    let app = router(s, &["http://localhost:3000".to_string()]);
    let (code, body) = get_json(app, "/api/tracer/ledger").await;
    assert_eq!(code, 200);
    assert_eq!(body["schema"], "tracer.corrected.v1");
    assert_eq!(body["paper"]["n"], 14433);
    std::fs::remove_dir_all(&dir).unwrap();
}
