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
#[path = "../src/metrics.rs"]
#[allow(dead_code)]
mod metrics;
#[path = "../src/tape_cache.rs"]
#[allow(dead_code)]
mod tape_cache;
#[path = "../src/traces.rs"]
#[allow(dead_code)]
mod traces;
use api::{AppState, LedgerLive, router};

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
        trace_id: None,
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
        evidence_opp_notional_usd: None,
        evidence_opp_vwap: None,
        evidence_n_prints: None,
        bar_buy_usd: None,
        bar_sell_usd: None,
        bar_n: None,
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

    let t5 = t4 + 120_000;
    let mut e = event(t5, -1, 0, EventKind::Posted, Leg::Entry, t5, "RTH");
    e.post_state = Some(PostState::Passive);
    e.bar_buy_usd = Some(61_250.0);
    e.bar_sell_usd = Some(38_750.0);
    e.bar_n = Some(42);
    w.append(&e).unwrap();
    let mut e = event(t5, -1, 1, EventKind::Filled, Leg::Entry, t5 + 7_000, "RTH");
    e.verdict = Some(Verdict::FilledThrough);
    e.milestone = Some("through".into());
    e.filled_touch_ts_ms = Some(t5 + 4_000);
    e.filled_through_ts_ms = Some(t5 + 7_000);
    w.append(&e).unwrap();
    let mut e = event(
        t5,
        -1,
        2,
        EventKind::Evidence,
        Leg::Entry,
        t5 + 900_000,
        "RTH",
    );
    e.evidence_opp_notional_usd = Some(12_345.67);
    e.evidence_opp_vwap = Some(330.41);
    e.evidence_n_prints = Some(17);
    w.append(&e).unwrap();

    w.flush().unwrap();

    let gaps_dir = dir.join("binding/hyperliquid/perpetual/gaps");
    std::fs::create_dir_all(&gaps_dir).unwrap();
    let in_window_open = (FIXED_NOW_MS - 2 * 3_600_000) * 1_000;
    let in_window_close = in_window_open + 600_000_000;
    let old_open = (FIXED_NOW_MS - 9 * 24 * 3_600_000) * 1_000;
    let old_close = old_open + 60_000_000;
    std::fs::write(
        gaps_dir.join("hyperliquid_xyz-TSLA_gap_ledger.jsonl"),
        format!(
            "{}\n{}\n{}\n",
            format_args!(
                r#"{{"opened_epoch_us":{in_window_open},"closed_epoch_us":{in_window_close},"window_us":600000000,"cause":"disconnect","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":9}}"#
            ),
            format_args!(
                r#"{{"opened_epoch_us":{old_open},"closed_epoch_us":{old_close},"window_us":60000000,"cause":"resync","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":null}}"#
            ),
            format_args!(
                r#"{{"opened_epoch_us":{in_window_open},"closed_epoch_us":{in_window_close},"window_us":600000000,"cause":"disconnect","exchange":"hyperliquid","symbol":"xyz:NVDA","possible_dropped_trades":4}}"#
            ),
        ),
    )
    .unwrap();
}

fn state(dir: &Path) -> AppState {
    AppState {
        status: Arc::new(RwLock::new(status_fixture())),
        ledger_live: Arc::new(RwLock::new(LedgerLive {
            since_ts_ms: Some(FIXED_NOW_MS - 30 * 3_600_000),
            n_kept: 2,
            total_usd: -0.35,
            fees_usd: -0.006,
            equity: vec![
                (FIXED_NOW_MS - 7_200_000, 0.15),
                (FIXED_NOW_MS - 3_600_000, -0.35),
            ],
            paper_n: 4,
            paper_total_usd: 1.85,
            paper_fees_usd: -0.012,
            paper_equity: vec![
                (FIXED_NOW_MS - 29 * 3_600_000, 0.4),
                (FIXED_NOW_MS - 7_200_000, 1.1),
                (FIXED_NOW_MS - 5_400_000, 0.7),
                (FIXED_NOW_MS - 3_600_000, 1.85),
            ],
        })),
        ready: Arc::new(RwLock::new(api::Readiness::ready())),
        metrics: metrics::Metrics::shared(),
        ledger_dir: dir.to_path_buf(),
        artifacts_dir: None,
        trades_binding: Some(dir.join("binding")),
        books_binding: None,
        symbol_stem: Some("xyz_TSLA".into()),
        traces: None,
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
        "trades": {"watermark_ms": FIXED_NOW_MS - 65_000, "last_file": "hyperliquid_xyz-TSLA_trades_sync_20260824_060000.000.parquet", "files_read": 412, "advance_age_ms": 48_000, "stalled": true},
        "books": {"watermark_ms": FIXED_NOW_MS - 245_000, "last_file": "hyperliquid_xyz-TSLA_ob_sync_20260824_060000.000.parquet", "files_read": 977, "advance_age_ms": 150_000, "stalled": true},
        "merged_watermark_ms": FIXED_NOW_MS - 245_000,
        "idle": true,
        "session": "RTH",
        "verdict_trail_ms": 180_000,
        "calendar_expired": true,
        "open_trades": 3,
        "counters": {
            "posted": 41, "entry_filled_through": 33, "entry_filled_queue_only": 1,
            "entry_expired_unfilled": 3, "entry_gapped": 1,
            "exit_maker": 25, "exit_taker_cross": 7, "exit_unpriced": 2,
            "would_be_voided_by_paper_filter": 2,
            "queue_vs_through_disagreements": 4, "book_degraded": 5, "duplicates_dropped": 12,
            "paper_exit_unpriced": 0
        },
        "corrected_n_kept": 32,
        "corrected_total_usd": -14.7,
        "corrected_fees_usd": -0.096,
        "paper_n": 38,
        "paper_total_usd": 22.4,
        "paper_fees_usd": -0.114,
        "first_post_ts_ms": FIXED_NOW_MS - 30 * 3_600_000,
        "events_written": 208,
        "ready": true,
        "binding_pending": false,
        "maker_fee_leg_bps": -0.015,
        "taker_fee_leg_bps": 1.044,
        "notional_usd": 1000.0
    })
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/api")
}

fn urlenc(s: &str) -> String {
    s.replace('|', "%7C")
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

    let (code, page1) = get_json(app(), "/api/tracer/events?limit=8").await;
    assert_eq!(code, 200);
    assert_or_bless("events_page1.json", &page1);
    let cursor = page1["older_cursor"].as_str().unwrap();
    let (code, page2) = get_json(
        app(),
        &format!("/api/tracer/events?limit=8&cursor={cursor}"),
    )
    .await;
    assert_eq!(code, 200);
    assert_or_bless("events_page2.json", &page2);
    assert!(page2["older_cursor"].is_null());
    let total =
        page1["events"].as_array().unwrap().len() + page2["events"].as_array().unwrap().len();
    assert_eq!(total, 15);

    let (code, ev) = get_json(app(), "/api/tracer/events?kind=EVIDENCE").await;
    assert_eq!(code, 200);
    let rows = ev["events"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["evidence_n_prints"], 17);
    assert_eq!(rows[0]["evidence_opp_notional_usd"], 12_345.67);

    let tid = rows[0]["trade_id"].as_str().unwrap().to_string();
    let (code, one) = get_json(
        app(),
        &format!("/api/tracer/events?trade_id={}", urlenc(&tid)),
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(one["events"].as_array().unwrap().len(), 3);

    let (code, exits) = get_json(app(), "/api/tracer/events?leg=exit").await;
    assert_eq!(code, 200);
    assert_eq!(exits["events"].as_array().unwrap().len(), 3);

    let (code, _) = get_json(app(), "/api/tracer/events?kind=BOGUS").await;
    assert_eq!(code, 400);

    let mut seen_posted = 0;
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let uri = match &cursor {
            None => "/api/tracer/events?kind=POSTED&limit=2".to_string(),
            Some(c) => format!("/api/tracer/events?kind=POSTED&limit=2&cursor={c}"),
        };
        let (code, page) = get_json(app(), &uri).await;
        assert_eq!(code, 200);
        let rows = page["events"].as_array().unwrap();
        assert!(rows.iter().all(|e| e["kind"] == "POSTED"));
        seen_posted += rows.len();
        match page["older_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert_eq!(seen_posted, 6);

    let (code, gaps24) = get_json(app(), "/api/tracer/gaps?window=24h").await;
    assert_eq!(code, 200);
    assert_or_bless("gaps_24h.json", &gaps24);
    assert_eq!(gaps24["incident_count"], 1);
    assert_eq!(gaps24["est_dropped_trades_sum"], 9);
    assert_eq!(gaps24["total_gap_ms"], 600_000);
    let (code, gaps_all) = get_json(app(), "/api/tracer/gaps?window=all").await;
    assert_eq!(code, 200);
    assert_eq!(gaps_all["incident_count"], 2);
    assert!(gaps_all["coverage_pct"].as_f64().unwrap() < 100.0);
    let (code, _) = get_json(app(), "/api/tracer/gaps?window=1y").await;
    assert_eq!(code, 400);

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
async fn replay_routes_serve_artifacts_beside_the_live_ledger() {
    let dir = std::env::temp_dir().join("tracer_api_replay_routes");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("corrected_ledger.json"),
        r#"{"schema":"tracer.corrected.v1","mode":"replay","paper":{"n":14433}}"#,
    )
    .unwrap();
    let mut s = state(&dir);
    s.artifacts_dir = Some(dir.clone());
    let app = || router(s.clone(), &["http://localhost:3000".to_string()]);

    let (code, live) = get_json(app(), "/api/tracer/ledger").await;
    assert_eq!(code, 200);
    assert_eq!(live["schema"], "tracer.ledger.live.v1");
    assert_eq!(live["paper"]["denominator"], "every_posted_signal");

    let (code, replay) = get_json(app(), "/api/tracer/replay/ledger").await;
    assert_eq!(code, 200);
    assert_eq!(replay["schema"], "tracer.corrected.v1");
    assert_eq!(replay["paper"]["n"], 14433);

    let (code, _) = get_json(app(), "/api/tracer/replay/fillrates").await;
    assert_eq!(code, 404);
    std::fs::write(
        dir.join("fillrates.json"),
        r#"{"schema":"tracer.fillrates.v1","mode":"replay","entry_cells":[]}"#,
    )
    .unwrap();
    let (code, fr) = get_json(app(), "/api/tracer/replay/fillrates").await;
    assert_eq!(code, 200);
    assert_eq!(fr["schema"], "tracer.fillrates.v1");

    let no_artifacts = state(&dir);
    let bare = router(no_artifacts, &["http://localhost:3000".to_string()]);
    let (code, _) = get_json(bare, "/api/tracer/replay/ledger").await;
    assert_eq!(code, 404);

    let mut no_bind = state(&dir);
    no_bind.trades_binding = None;
    let unbound = router(no_bind, &["http://localhost:3000".to_string()]);
    let (code, _) = get_json(unbound, "/api/tracer/gaps").await;
    assert_eq!(code, 404);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn duplicate_reappended_events_do_not_move_displayed_rates() {
    let dir = std::env::temp_dir().join("tracer_api_dedupe_ledger");
    build_fixture_ledger(&dir);
    let origins = vec!["http://localhost:3000".to_string()];
    let app = || router(state(&dir), &origins);

    let (_, events_before) = get_json(app(), "/api/tracer/events?limit=100").await;
    let (_, fr_before) = get_json(app(), "/api/tracer/fillrates?window=all").await;

    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .collect();
    files.sort();
    let replayed: String = files
        .iter()
        .map(|p| std::fs::read_to_string(p).unwrap())
        .collect();
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(files.last().unwrap())
        .unwrap();
    f.write_all(replayed.as_bytes()).unwrap();
    f.flush().unwrap();

    let (_, events_after) = get_json(app(), "/api/tracer/events?limit=100").await;
    let (_, fr_after) = get_json(app(), "/api/tracer/fillrates?window=all").await;
    assert_eq!(events_before, events_after);
    assert_eq!(fr_before, fr_after);

    let (_, page1) = get_json(app(), "/api/tracer/events?limit=6").await;
    let cursor = page1["older_cursor"].as_str().unwrap();
    let (_, page2) = get_json(
        app(),
        &format!("/api/tracer/events?limit=6&cursor={cursor}"),
    )
    .await;
    let mut ids: Vec<String> = page1["events"]
        .as_array()
        .unwrap()
        .iter()
        .chain(page2["events"].as_array().unwrap().iter())
        .map(|e| e["event_id"].as_str().unwrap().to_string())
        .collect();
    let n = ids.len();
    assert_eq!(n, 12);
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), n, "duplicate event_ids leaked across pages");
    std::fs::remove_dir_all(&dir).unwrap();
}
