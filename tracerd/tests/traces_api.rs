use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use http_body_util::BodyExt;
use tower::ServiceExt;

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
use api::{AppState, LedgerLive, router, wall_now_ms};

use aetelier_io::orderbooks::ob_parquet::write_ob_parquet;
use aetelier_io::trades::trades_parquet::write_trades_parquet;
use aetelier_types::orderbooks::core::{Orderbook, f64_to_decimal};
use aetelier_types::trades::{Trade, TradeSide};
use aetelier_types::trading_pair::TradingPair;
use tracer_engine::bars::read_fills_csv;
use tracer_engine::nyse::regime;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tracerd_traces_api_{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct World {
    dir: PathBuf,
    trades_parquet: PathBuf,
    ob_parquet: PathBuf,
    span: (u64, u64),
    state: AppState,
}

fn world(name: &str) -> World {
    world_with_quota(name, traces::DEFAULT_MAX_TRACES_PER_USER)
}

fn world_with_quota(name: &str, quota: usize) -> World {
    let dir = temp_dir(name);
    let rows = read_fills_csv(&repo_root().join("fixtures/mini_fills_xyz_TSLA.csv")).unwrap();
    let pair = TradingPair::from_str("xyz:TSLA/USDC").unwrap();
    let trades: Vec<Trade> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| Trade {
            source_trade_ts_us: r.time_ms * 1_000,
            local_trade_ts_us: r.time_ms * 1_000,
            source_trade_rtt_us: 0,
            pair: pair.clone(),
            side: if r.is_buy {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            },
            amount: f64_to_decimal(r.sz),
            price: f64_to_decimal(r.px),
            exchange: "hyperliquid".into(),
            id: format!("t{i:012}"),
            origin: Default::default(),
        })
        .collect();
    let trades_parquet = dir.join("trades.parquet");
    write_trades_parquet(&trades, &trades_parquet).unwrap();

    let t_min = rows.first().unwrap().time_ms;
    let t_max = rows.last().unwrap().time_ms;
    let mut bids = std::collections::BTreeMap::new();
    let mut asks = std::collections::BTreeMap::new();
    bids.insert(
        f64_to_decimal(390.0),
        aetelier_types::levels::Level::new(
            0,
            aetelier_types::OrderSide::Bids,
            f64_to_decimal(390.0),
            f64_to_decimal(1.0),
            vec![],
        ),
    );
    asks.insert(
        f64_to_decimal(390.1),
        aetelier_types::levels::Level::new(
            1,
            aetelier_types::OrderSide::Asks,
            f64_to_decimal(390.1),
            f64_to_decimal(1.0),
            vec![],
        ),
    );
    let snaps: Vec<Orderbook> = [t_min, (t_min + t_max) / 2, t_max]
        .iter()
        .map(|t| Orderbook {
            orderbook_id: 0,
            orderbook_ts_us: t * 1_000,
            source_orderbook_ts_us: t * 1_000,
            local_orderbook_ts_us: t * 1_000,
            source_orderbook_rtt_us: 0,
            pair: pair.clone(),
            exchange: "hyperliquid".into(),
            bids: bids.clone(),
            asks: asks.clone(),
        })
        .collect();
    let ob_dir = dir.join("ob");
    std::fs::create_dir_all(&ob_dir).unwrap();
    let ob_parquet = write_ob_parquet(&snaps, &ob_dir, "sync").unwrap();

    let metrics = metrics::Metrics::shared();
    let rt = traces::TraceRuntime::start(
        dir.join("traces"),
        repo_root().join("params/tracer_params.v1.json"),
        300_000,
        60_000,
        None,
        None,
        None,
        quota,
        traces::DEFAULT_TAPE_RETENTION_MS,
        metrics.clone(),
    )
    .unwrap();
    let ledger_dir = dir.join("live_ledger");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    let state = AppState {
        status: Arc::new(RwLock::new(serde_json::json!({"mode": "serve"}))),
        ledger_live: Arc::new(RwLock::new(LedgerLive::default())),
        ready: Arc::new(RwLock::new(api::Readiness::ready())),
        metrics,
        ledger_dir,
        artifacts_dir: None,
        trades_binding: None,
        books_binding: None,
        symbol_stem: None,
        traces: Some(rt),
        params_path: Some(repo_root().join("params/tracer_params.v1.json")),
        now_ms: wall_now_ms,
    };
    World {
        dir,
        trades_parquet,
        ob_parquet,
        span: (t_min, t_max),
        state,
    }
}

fn app(w: &World) -> axum::Router {
    router(w.state.clone(), &["http://localhost:3000".to_string()])
}

const BOUNDARY: &str = "XtestboundaryX";

fn multipart_body(config: &serde_json::Value, files: &[(&str, &Path)]) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(
        format!(
            "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"config\"\r\n\r\n{config}\r\n"
        )
        .as_bytes(),
    );
    for (name, path) in files {
        let filename = path.file_name().unwrap().to_string_lossy();
        body.extend_from_slice(
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(&std::fs::read(path).unwrap());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

const OWNER: &str = "user-a";

async fn post_trace(
    w: &World,
    config: &serde_json::Value,
    files: &[(&str, &Path)],
    user: Option<&str>,
) -> (axum::http::StatusCode, serde_json::Value) {
    let mut req = axum::http::Request::builder()
        .method("POST")
        .uri("/api/tracer/traces")
        .header(
            "content-type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        );
    if let Some(u) = user {
        req = req.header("x-aetelier-user", u);
    }
    let req = req
        .body(axum::body::Body::from(multipart_body(config, files)))
        .unwrap();
    let resp = app(w).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn get_json(
    w: &World,
    uri: &str,
    user: Option<&str>,
) -> (axum::http::StatusCode, Option<serde_json::Value>) {
    let mut req = axum::http::Request::builder().method("GET").uri(uri);
    if let Some(u) = user {
        req = req.header("x-aetelier-user", u);
    }
    let req = req.body(axum::body::Body::empty()).unwrap();
    let resp = app(w).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).ok())
}

async fn wait_complete(w: &World, trace_id: &str, user: &str) -> serde_json::Value {
    for _ in 0..300 {
        let (status, body) =
            get_json(w, &format!("/api/tracer/traces/{trace_id}"), Some(user)).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let body = body.unwrap();
        match body["status"].as_str().unwrap() {
            "complete" => return body,
            "failed" => panic!("trace failed: {}", body["error"]),
            _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    panic!("trace {trace_id} did not complete in time");
}

#[tokio::test]
async fn strategy_upload_trace_lifecycle_and_artifact_routes() {
    let w = world("lifecycle");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let (status, body) = post_trace(
        &w,
        &config,
        &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)],
        Some("user-a"),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let trace_id = body["trace_id"].as_str().unwrap().to_string();
    assert!(trace_id.starts_with("tr_"));
    assert_eq!(body["status"], "queued");
    assert_eq!(body["preflight"]["schema"], "tracer.preflight.v1");
    assert_eq!(body["preflight"]["files"].as_array().unwrap().len(), 2);

    let record = wait_complete(&w, &trace_id, OWNER).await;
    assert_eq!(record["order_source"], "strategy");
    assert_eq!(record["user_id"], "user-a");
    assert!(record["summary"]["n_signals"].as_u64().unwrap() > 0);

    let (status, ledger) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/ledger"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let ledger = ledger.unwrap();
    assert_eq!(ledger["schema"], "tracer.corrected.v1");
    assert_eq!(ledger["mode"], "trace");
    assert_eq!(ledger["trace_id"], trace_id.as_str());

    let (status, fr) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/fillrates"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(fr.unwrap()["schema"], "tracer.fillrates.v1");

    let (status, events) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/events?limit=10"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let events = events.unwrap();
    assert_eq!(events["events"].as_array().unwrap().len(), 10);
    assert_eq!(events["events"][0]["mode"], "trace");

    let (status, book) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/orderbook"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let book = book.unwrap();
    assert_eq!(book["schema"], "tracer.traceorderbook.v1");
    assert_eq!(book["points"].as_array().unwrap().len(), 3);
    assert_eq!(book["points"][0]["bid_px"], 390.0);

    let (status, trades) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/trades?limit=5"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let trades = trades.unwrap();
    assert_eq!(trades["trades"].as_array().unwrap().len(), 5);
    assert_eq!(
        trades["trades"][0]["ts_ms"].as_u64().unwrap(),
        w.span.1,
        "trades must page newest-first"
    );
    assert!(trades["older_cursor"].as_u64().is_some());

    for source in ["orderbook", "trades"] {
        let (status, schema) = get_json(
            &w,
            &format!("/api/tracer/traces/{trace_id}/schema?source={source}"),
            Some(OWNER),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let schema = schema.unwrap();
        assert_eq!(schema["schema"], "tracer.traceschema.v1");
        let first = schema["fields"][0]["name"].as_str().unwrap();
        match source {
            "orderbook" => assert_eq!(first, "orderbook_ts_us"),
            _ => assert_eq!(first, "source_trade_ts_us"),
        }
    }

    let (status, list) = get_json(&w, "/api/tracer/traces", Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let list = list.unwrap();
    assert_eq!(list["traces"].as_array().unwrap().len(), 1);
    assert!(list["live"].is_null());

    let (status, list_b) = get_json(&w, "/api/tracer/traces", Some("user-b")).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(list_b.unwrap()["traces"].as_array().unwrap().len(), 0);

    let (status, _) = get_json(&w, "/api/tracer/traces/tr_missing000000", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn strict_rejection_returns_422_row_error_report() {
    let w = world("reject");
    let (t_min, _) = w.span;
    let t_entry = t_min + 3_600_000;
    let wrong_regime = if regime(t_entry).label() == "RTH" {
        "EXT"
    } else {
        "RTH"
    };
    let signals = format!(
        "{}\nxyz_TSLA,3,0.5,11,{t_entry},396.0,{},{},395.0,-0.1,-0.1,0.0,{wrong_regime}\n",
        tracer_engine::trace::SIGNALS_HEADER,
        t_entry + 900_000 + 5,
        t_entry + 900_000,
    );
    let signals_path = w.dir.join("bad.csv");
    std::fs::write(&signals_path, signals).unwrap();
    let config = serde_json::json!({"order_source": "signals", "tape": "upload"});
    let (status, body) = post_trace(
        &w,
        &config,
        &[
            ("orderbook", &w.ob_parquet),
            ("trades", &w.trades_parquet),
            ("signals", &signals_path),
        ],
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["schema"], "tracer.preflight.v1");
    assert_eq!(body["signal_rows"], 1);
    let errors = body["row_errors"].as_array().unwrap();
    let fields: Vec<&str> = errors
        .iter()
        .map(|e| e["field"].as_str().unwrap())
        .collect();
    for want in ["side", "decile", "t_exit_ms", "regime"] {
        assert!(fields.contains(&want), "missing {want} in {fields:?}");
    }
    assert!(errors.iter().all(|e| e["row"] == 1));

    let (status, _) = get_json(&w, "/api/tracer/traces", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
}

#[tokio::test]
async fn preflight_only_reports_without_persisting() {
    let w = world("preflight");
    let config =
        serde_json::json!({"order_source": "strategy", "tape": "upload", "preflight_only": true});
    let (status, body) = post_trace(
        &w,
        &config,
        &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)],
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["status"], "preflight");
    assert!(body["preflight"]["span"]["start_ms"].as_u64().is_some());

    let (_, list) = get_json(&w, "/api/tracer/traces", Some(OWNER)).await;
    assert_eq!(list.unwrap()["traces"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn resubmitting_the_same_config_is_idempotent() {
    for (name, user) in [
        ("idempotent", Some(OWNER)),
        ("idempotent_user", Some("user-a")),
    ] {
        let w = world(name);
        let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
        let files: &[(&str, &Path)] =
            &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
        let (status, body) = post_trace(&w, &config, files, user).await;
        assert_eq!(status, axum::http::StatusCode::CREATED, "{user:?}");
        let id = body["trace_id"].as_str().unwrap().to_string();
        wait_complete(&w, &id, OWNER).await;
        let (status, body2) = post_trace(&w, &config, files, user).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{user:?}");
        assert_eq!(body2["trace_id"], id.as_str());
        assert_eq!(body2["status"], "complete");
    }
}

#[tokio::test]
async fn a_failed_trace_is_requeued_when_its_config_is_submitted_again() {
    let w = world("requeue_failed");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, OWNER).await;

    let record_path = w.dir.join("traces").join(&id).join("trace.json");
    let mut record: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&record_path).unwrap()).unwrap();
    record["status"] = serde_json::json!("failed");
    record["error"] = serde_json::json!("engine died mid-run");
    std::fs::write(&record_path, serde_json::to_string_pretty(&record).unwrap()).unwrap();

    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    assert_eq!(body["trace_id"], id.as_str());
    let done = wait_complete(&w, &id, OWNER).await;
    assert!(done["error"].is_null(), "{done}");
}

#[tokio::test]
async fn a_deleted_trace_runs_again_when_its_config_is_submitted_again() {
    let w = world("recreate_deleted");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, OWNER).await;
    assert_eq!(
        delete_trace(&w, &id, Some(OWNER)).await,
        axum::http::StatusCode::NO_CONTENT
    );

    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    assert_eq!(body["trace_id"], id.as_str());
    wait_complete(&w, &id, OWNER).await;
    assert!(
        w.dir
            .join("traces")
            .join(&id)
            .join("corrected_ledger.json")
            .is_file()
    );
}

fn trace_routes(id: &str) -> Vec<String> {
    vec![
        format!("/api/tracer/traces/{id}"),
        format!("/api/tracer/traces/{id}/ledger"),
        format!("/api/tracer/traces/{id}/fillrates"),
        format!("/api/tracer/traces/{id}/events?limit=1"),
        format!("/api/tracer/traces/{id}/orderbook"),
        format!("/api/tracer/traces/{id}/depth"),
        format!("/api/tracer/traces/{id}/trades?limit=1"),
        format!("/api/tracer/traces/{id}/schema?source=trades"),
    ]
}

#[tokio::test]
async fn a_trace_is_invisible_to_a_caller_who_is_not_its_owner() {
    let w = world("owner_scope");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, OWNER).await;
    for route in trace_routes(&id) {
        let (status, _) = get_json(&w, &route, Some("user-b")).await;
        assert_eq!(status, axum::http::StatusCode::NOT_FOUND, "user-b {route}");
        let (status, _) = get_json(&w, &route, Some("user-a")).await;
        assert_eq!(status, axum::http::StatusCode::OK, "owner {route}");
        let (status, _) = get_json(&w, &route, None).await;
        assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED, "anon {route}");
    }
}

#[tokio::test]
async fn identical_strategy_uploads_by_two_users_get_separate_traces() {
    let w = world("two_users");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, a) = post_trace(&w, &config, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{a}");
    let (status, b) = post_trace(&w, &config, files, Some("user-b")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{b}");
    let id_a = a["trace_id"].as_str().unwrap().to_string();
    let id_b = b["trace_id"].as_str().unwrap().to_string();
    assert_ne!(id_a, id_b);
    let rec_a = wait_complete(&w, &id_a, "user-a").await;
    let rec_b = wait_complete(&w, &id_b, "user-b").await;
    assert!(w.dir.join("traces").join(&id_a).is_dir());
    assert!(w.dir.join("traces").join(&id_b).is_dir());
    assert_eq!(rec_a["user_id"], "user-a");
    assert_eq!(rec_b["user_id"], "user-b");
    for key in [
        "n_signals",
        "entry_through",
        "corrected_kept",
        "events_written",
    ] {
        assert_eq!(rec_a["summary"][key], rec_b["summary"][key], "{key}");
    }
    let (_, ledger_a) = get_json(
        &w,
        &format!("/api/tracer/traces/{id_a}/ledger"),
        Some("user-a"),
    )
    .await;
    let (_, ledger_b) = get_json(
        &w,
        &format!("/api/tracer/traces/{id_b}/ledger"),
        Some("user-b"),
    )
    .await;
    let mut ledger_a = ledger_a.unwrap();
    let mut ledger_b = ledger_b.unwrap();
    for ledger in [&mut ledger_a, &mut ledger_b] {
        ledger.as_object_mut().unwrap().remove("trace_id");
    }
    assert_eq!(ledger_a, ledger_b);
}

#[tokio::test]
async fn preflight_only_returns_the_id_the_create_will_use() {
    let w = world("preflight_id");
    let preflight =
        serde_json::json!({"order_source": "strategy", "tape": "upload", "preflight_only": true});
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, pre_a) = post_trace(&w, &preflight, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{pre_a}");
    let (status, pre_b) = post_trace(&w, &preflight, files, Some("user-b")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{pre_b}");
    let (status, _) = post_trace(&w, &preflight, files, None).await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
    assert_ne!(pre_a["trace_id"], pre_b["trace_id"]);
    let (status, created) = post_trace(&w, &config, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{created}");
    assert_eq!(created["trace_id"], pre_a["trace_id"]);
}

#[tokio::test]
async fn an_owner_less_record_is_quarantined_from_every_caller() {
    let w = world("ownerless");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, OWNER).await;
    let record_path = w.dir.join("traces").join(&id).join("trace.json");
    let mut record: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&record_path).unwrap()).unwrap();
    record.as_object_mut().unwrap().remove("user_id");
    std::fs::write(&record_path, serde_json::to_string_pretty(&record).unwrap()).unwrap();
    for route in trace_routes(&id) {
        let (status, _) = get_json(&w, &route, Some(OWNER)).await;
        assert_eq!(status, axum::http::StatusCode::NOT_FOUND, "owner {route}");
        let (status, _) = get_json(&w, &route, None).await;
        assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED, "anon {route}");
    }
    let (status, list) = get_json(&w, "/api/tracer/traces", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(list.unwrap()["traces"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn the_live_record_ignores_ownership_but_needs_a_caller() {
    let mut w = world("live_identity");
    let books_binding = w.dir.join("bnd_books");
    let trades_binding = w.dir.join("bnd_trades");
    let books_dir = books_binding.join("hyperliquid/perpetual/orderbooks");
    let trades_dir = trades_binding.join("hyperliquid/perpetual/trades");
    std::fs::create_dir_all(&books_dir).unwrap();
    std::fs::create_dir_all(&trades_dir).unwrap();
    std::fs::copy(&w.ob_parquet, books_dir.join("ob_sync_1.parquet")).unwrap();
    std::fs::copy(&w.trades_parquet, trades_dir.join("trades_sync_1.parquet")).unwrap();
    let rt = traces::TraceRuntime::start(
        w.dir.join("traces_live"),
        repo_root().join("params/tracer_params.v1.json"),
        300_000,
        60_000,
        Some(trades_binding),
        Some(books_binding),
        Some("xyz_TSLA".into()),
        traces::DEFAULT_MAX_TRACES_PER_USER,
        traces::DEFAULT_TAPE_RETENTION_MS,
        w.state.metrics.clone(),
    )
    .unwrap();
    w.state.traces = Some(rt);
    let (status, _) = get_json(&w, "/api/tracer/traces/live", None).await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
    for user in [Some("user-a"), Some("user-b")] {
        let (status, body) = get_json(&w, "/api/tracer/traces/live", user).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{user:?}");
        assert_eq!(body.unwrap()["trace_id"], "live");
        for route in [
            "/api/tracer/traces/live/trades?limit=1",
            "/api/tracer/traces/live/schema?source=trades",
        ] {
            let (status, _) = get_json(&w, route, user).await;
            assert_eq!(status, axum::http::StatusCode::OK, "{user:?} {route}");
        }
    }
}

async fn delete_trace(w: &World, trace_id: &str, user: Option<&str>) -> axum::http::StatusCode {
    let mut req = axum::http::Request::builder()
        .method("DELETE")
        .uri(format!("/api/tracer/traces/{trace_id}"));
    if let Some(u) = user {
        req = req.header("x-aetelier-user", u);
    }
    let req = req.body(axum::body::Body::empty()).unwrap();
    app(w).oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn an_owner_deletes_a_trace_and_its_whole_subtree() {
    let w = world("delete_owner");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, OWNER).await;
    let dir = w.dir.join("traces").join(&id);
    assert!(dir.join("inputs").is_dir());
    assert!(dir.join("corrected_ledger.json").is_file());
    assert_eq!(
        delete_trace(&w, &id, Some(OWNER)).await,
        axum::http::StatusCode::NO_CONTENT
    );
    assert!(!dir.exists());
    for route in trace_routes(&id) {
        let (status, _) = get_json(&w, &route, Some(OWNER)).await;
        assert_eq!(status, axum::http::StatusCode::NOT_FOUND, "{route}");
    }
    let (_, list) = get_json(&w, "/api/tracer/traces", Some(OWNER)).await;
    assert!(list.unwrap()["traces"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn delete_is_refused_to_strangers_anonymous_callers_and_bad_ids() {
    let w = world("delete_refused");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, "user-a").await;
    let dir = w.dir.join("traces").join(&id);
    assert_eq!(
        delete_trace(&w, &id, Some("user-b")).await,
        axum::http::StatusCode::NOT_FOUND
    );
    assert!(dir.is_dir());
    assert_eq!(
        delete_trace(&w, &id, None).await,
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert!(dir.is_dir());
    for bad in ["../traces", "staging_1_2", "tr_NOTHEX000000", "live"] {
        assert_eq!(
            delete_trace(&w, bad, Some("user-a")).await,
            axum::http::StatusCode::NOT_FOUND,
            "{bad}"
        );
    }
    assert!(dir.is_dir());
    assert_eq!(
        delete_trace(&w, &id, Some("user-a")).await,
        axum::http::StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn the_quota_refuses_a_further_upload_from_the_same_owner() {
    let w = world_with_quota("quota", 1);
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, "user-a").await;
    let other = serde_json::json!({
        "order_source": "strategy", "tape": "upload", "notional_usd": 2_000.0
    });
    let (status, body) = post_trace(&w, &other, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["limit"], 1);
    let (status, _) = post_trace(&w, &other, files, Some("user-b")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED);
    assert_eq!(
        delete_trace(&w, &id, Some("user-a")).await,
        axum::http::StatusCode::NO_CONTENT
    );
    let (status, _) = post_trace(&w, &other, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED);
}

#[tokio::test]
async fn the_service_credential_replaces_header_absence() {
    let w = world("service_cred");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some("user-a")).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id, "user-a").await;

    let route = format!("/api/tracer/traces/{id}");
    let svc = |token: Option<&str>| {
        let mut req = axum::http::Request::builder().method("GET").uri(&route);
        if let Some(t) = token {
            req = req.header("x-aetelier-service", t);
        }
        req.body(axum::body::Body::empty()).unwrap()
    };

    assert_eq!(
        app(&w).oneshot(svc(Some("s3cret"))).await.unwrap().status(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    unsafe { std::env::set_var("TRACER_SERVICE_TOKEN", "s3cret") };
    assert_eq!(
        app(&w).oneshot(svc(Some("s3cret"))).await.unwrap().status(),
        axum::http::StatusCode::OK
    );
    assert_eq!(
        app(&w).oneshot(svc(Some("wrong"))).await.unwrap().status(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app(&w).oneshot(svc(None)).await.unwrap().status(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    unsafe { std::env::remove_var("TRACER_SERVICE_TOKEN") };
}

#[tokio::test]
async fn the_sweep_evicts_tapes_past_retention_and_frees_the_quota() {
    let w = world_with_quota("retention", 1);
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    let record = wait_complete(&w, &id, OWNER).await;
    let terminal_ms = record["terminal_ms"].as_u64().expect("terminal_ms stamped");
    let inputs = w.dir.join("traces").join(&id).join("inputs");
    assert!(inputs.is_dir());

    let rt = w.state.traces.clone().unwrap();
    assert!(rt.sweep_tapes(terminal_ms).is_empty(), "nothing due at t0");
    let just_under = terminal_ms + traces::DEFAULT_TAPE_RETENTION_MS - 1;
    assert!(
        rt.sweep_tapes(just_under).is_empty(),
        "nothing due before the mark"
    );

    let due = terminal_ms + traces::DEFAULT_TAPE_RETENTION_MS;
    assert_eq!(rt.sweep_tapes(due), vec![id.clone()]);
    assert!(!inputs.exists(), "inputs removed");
    assert!(rt.sweep_tapes(due).is_empty(), "eviction is not repeated");

    let (status, after) = get_json(&w, &format!("/api/tracer/traces/{id}"), Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let after = after.unwrap();
    assert_eq!(after["tape_evicted"], true);
    assert!(after["inputs"].as_array().unwrap().is_empty());

    for route in [
        format!("/api/tracer/traces/{id}/ledger"),
        format!("/api/tracer/traces/{id}/fillrates"),
    ] {
        let (status, _) = get_json(&w, &route, Some(OWNER)).await;
        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "{route} survives eviction"
        );
    }

    let other = serde_json::json!({
        "order_source": "strategy", "tape": "upload", "notional_usd": 2_000.0
    });
    let (status, _) = post_trace(&w, &other, files, Some(OWNER)).await;
    assert_eq!(
        status,
        axum::http::StatusCode::CREATED,
        "an evicted trace must not hold a quota slot"
    );
}

#[tokio::test]
async fn a_running_trace_is_never_swept() {
    let w = world("retention_running");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let id = body["trace_id"].as_str().unwrap().to_string();
    let rt = w.state.traces.clone().unwrap();
    let far_future = 10 * traces::DEFAULT_TAPE_RETENTION_MS;
    let swept = rt.sweep_tapes(far_future);
    wait_complete(&w, &id, OWNER).await;
    assert!(
        !swept.contains(&id) || w.dir.join("traces").join(&id).join("inputs").exists(),
        "a trace still queued or running kept its inputs"
    );
}

fn deep_ob_parquet(w: &World, n_levels: usize, ts_list: &[u64]) -> PathBuf {
    let pair = TradingPair::from_str("xyz:TSLA/USDC").unwrap();
    let mut bids = std::collections::BTreeMap::new();
    let mut asks = std::collections::BTreeMap::new();
    for i in 0..n_levels {
        let bid_px = 390.0 - i as f64 * 0.1;
        let ask_px = 390.1 + i as f64 * 0.1;
        bids.insert(
            f64_to_decimal(bid_px),
            aetelier_types::levels::Level::new(
                i as u32,
                aetelier_types::OrderSide::Bids,
                f64_to_decimal(bid_px),
                f64_to_decimal(1.0 + i as f64),
                vec![],
            ),
        );
        asks.insert(
            f64_to_decimal(ask_px),
            aetelier_types::levels::Level::new(
                i as u32,
                aetelier_types::OrderSide::Asks,
                f64_to_decimal(ask_px),
                f64_to_decimal(2.0 + i as f64),
                vec![],
            ),
        );
    }
    let snaps: Vec<Orderbook> = ts_list
        .iter()
        .map(|t| Orderbook {
            orderbook_id: 0,
            orderbook_ts_us: t * 1_000,
            source_orderbook_ts_us: t * 1_000,
            local_orderbook_ts_us: t * 1_000,
            source_orderbook_rtt_us: 0,
            pair: pair.clone(),
            exchange: "hyperliquid".into(),
            bids: bids.clone(),
            asks: asks.clone(),
        })
        .collect();
    let dir = w.dir.join("deep_ob");
    std::fs::create_dir_all(&dir).unwrap();
    write_ob_parquet(&snaps, &dir, "sync").unwrap()
}

#[tokio::test]
async fn depth_route_truncates_levels_windows_and_decimates() {
    let w = world("depth");
    let (t_min, t_max) = w.span;
    let step = (t_max - t_min) / 4;
    let ts_list: Vec<u64> = (0..5).map(|i| t_min + i * step).collect();
    let deep_ob = deep_ob_parquet(&w, 12, &ts_list);

    let config = serde_json::json!({
        "order_source": "strategy",
        "tape": "upload",
        "notional_usd": 1000.0,
        "grace_ms": 300000,
        "cross_window_ms": 60000,
    });
    let (status, body) = post_trace(
        &w,
        &config,
        &[("orderbook", &deep_ob), ("trades", &w.trades_parquet)],
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let trace_id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &trace_id, OWNER).await;

    let (status, depth) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/depth"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let depth = depth.unwrap();
    assert_eq!(depth["schema"], "tracer.tracedepth.v1");
    let rows = depth["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    let bids = rows[0]["bids"].as_array().unwrap();
    let asks = rows[0]["asks"].as_array().unwrap();
    assert_eq!(bids.len(), 10);
    assert_eq!(asks.len(), 10);
    assert_eq!(bids[0][0], 390.0);
    assert_eq!(bids[9][0].as_f64().unwrap(), 389.1);
    assert_eq!(asks[0][0], 390.1);
    assert!((rows[0]["mid"].as_f64().unwrap() - 390.05).abs() < 1e-9);

    let (status, windowed) = get_json(
        &w,
        &format!(
            "/api/tracer/traces/{trace_id}/depth?from={}&to={}",
            ts_list[1], ts_list[3]
        ),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let windowed = windowed.unwrap();
    let wrows = windowed["rows"].as_array().unwrap();
    assert_eq!(wrows.len(), 3);
    assert_eq!(wrows[0]["ts_ms"].as_u64().unwrap(), ts_list[1]);
    assert_eq!(wrows[2]["ts_ms"].as_u64().unwrap(), ts_list[3]);

    let (status, capped) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/depth?max=2"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let capped = capped.unwrap();
    let crows = capped["rows"].as_array().unwrap();
    assert!(crows.len() <= 3, "got {}", crows.len());
    assert_eq!(crows[0]["ts_ms"].as_u64().unwrap(), ts_list[0]);
    assert_eq!(crows.last().unwrap()["ts_ms"].as_u64().unwrap(), ts_list[4]);

    let (status, _) = get_json(&w, "/api/tracer/traces/tr_missing0000/depth", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);

    let (status, _) = get_json(&w, "/api/tracer/traces/live/depth", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

/// A trace that ran on the LIVE tape uploads no parquet, so its record has no
/// input to describe. Its schema must come from the daemon's binding dirs
/// instead of 404ing.
#[tokio::test]
async fn a_live_tape_trace_serves_its_schema_from_the_binding_dirs() {
    let mut w = world("live_schema_fallback");

    // Collector layout: <binding>/<exchange>/<market_type>/<datatype>/*.parquet
    let books_binding = w.dir.join("bnd_books");
    let trades_binding = w.dir.join("bnd_trades");
    let books_dir = books_binding.join("hyperliquid/perpetual/orderbooks");
    let trades_dir = trades_binding.join("hyperliquid/perpetual/trades");
    std::fs::create_dir_all(&books_dir).unwrap();
    std::fs::create_dir_all(&trades_dir).unwrap();
    std::fs::copy(&w.ob_parquet, books_dir.join("ob_sync_1.parquet")).unwrap();
    std::fs::copy(&w.trades_parquet, trades_dir.join("trades_sync_1.parquet")).unwrap();

    let rt = traces::TraceRuntime::start(
        w.dir.join("traces_live"),
        repo_root().join("params/tracer_params.v1.json"),
        300_000,
        60_000,
        Some(trades_binding),
        Some(books_binding),
        Some("xyz_TSLA".into()),
        traces::DEFAULT_MAX_TRACES_PER_USER,
        traces::DEFAULT_TAPE_RETENTION_MS,
        w.state.metrics.clone(),
    )
    .unwrap();

    // A completed live-tape trace: signals input only, no parquet. Written as
    // the on-disk record so the handler's own read path is exercised.
    let trace_dir = w.dir.join("traces_live").join("tr_livetape0001");
    std::fs::create_dir_all(&trace_dir).unwrap();
    std::fs::write(
        trace_dir.join("trace.json"),
        serde_json::json!({
            "schema": "tracer.trace.v1",
            "trace_id": "tr_livetape0001",
            "created_ms": 1,
            "user_id": "user-a",
            "status": "complete",
            "order_source": "signals",
            "tape_source": "live",
            "inputs": [{"role": "signals", "file": "signals__x.csv"}],
            "grace_ms": 300000,
            "cross_window_ms": 60000,
            "preflight": null
        })
        .to_string(),
    )
    .unwrap();
    w.state.traces = Some(rt);

    for (source, first_field) in [
        ("orderbook", "orderbook_ts_us"),
        ("trades", "source_trade_ts_us"),
    ] {
        let (status, schema) = get_json(
            &w,
            &format!("/api/tracer/traces/tr_livetape0001/schema?source={source}"),
            Some(OWNER),
        )
        .await;
        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "live-tape trace must serve {source} schema"
        );
        assert_eq!(schema.unwrap()["fields"][0]["name"], first_field);
    }
}

fn live_world(name: &str, day_dir: bool) -> (World, PathBuf, PathBuf) {
    let mut w = world(name);
    let books_binding = w.dir.join("bnd_books");
    let trades_binding = w.dir.join("bnd_trades");
    let mut books_dir = books_binding.join("hyperliquid/perpetual/orderbooks");
    let mut trades_dir = trades_binding.join("hyperliquid/perpetual/trades");
    if day_dir {
        books_dir = books_dir.join("2026-10-13");
        trades_dir = trades_dir.join("2026-10-13");
    }
    std::fs::create_dir_all(&books_dir).unwrap();
    std::fs::create_dir_all(&trades_dir).unwrap();
    std::fs::copy(&w.ob_parquet, books_dir.join("ob_sync_1.parquet")).unwrap();
    std::fs::copy(&w.trades_parquet, trades_dir.join("trades_sync_1.parquet")).unwrap();
    let rt = traces::TraceRuntime::start(
        w.dir.join("traces_live"),
        repo_root().join("params/tracer_params.v1.json"),
        300_000,
        60_000,
        Some(trades_binding),
        Some(books_binding),
        Some("xyz_TSLA".into()),
        traces::DEFAULT_MAX_TRACES_PER_USER,
        traces::DEFAULT_TAPE_RETENTION_MS,
        w.state.metrics.clone(),
    )
    .unwrap();
    w.state.traces = Some(rt);
    (w, trades_dir, books_dir)
}

fn write_live_signals_record(w: &World, trace_id: &str, evicted: bool) {
    let trace_dir = w.dir.join("traces_live").join(trace_id);
    std::fs::create_dir_all(&trace_dir).unwrap();
    let inputs: Vec<serde_json::Value> = if evicted {
        Vec::new()
    } else {
        vec![serde_json::json!({"role": "signals", "file": "signals__x.csv"})]
    };
    std::fs::write(
        trace_dir.join("trace.json"),
        serde_json::json!({
            "schema": "tracer.trace.v1",
            "trace_id": trace_id,
            "created_ms": 1,
            "user_id": OWNER,
            "status": "complete",
            "tape_evicted": evicted,
            "order_source": "signals",
            "tape_source": "live",
            "inputs": inputs,
            "grace_ms": 300000,
            "cross_window_ms": 60000,
            "preflight": null
        })
        .to_string(),
    )
    .unwrap();
}

async fn upload_deep_trace(w: &World, name: &str, ts_list: &[u64]) -> String {
    let deep_ob = {
        let pair = TradingPair::from_str("xyz:TSLA/USDC").unwrap();
        let mut bids = std::collections::BTreeMap::new();
        let mut asks = std::collections::BTreeMap::new();
        bids.insert(
            f64_to_decimal(390.0),
            aetelier_types::levels::Level::new(
                0,
                aetelier_types::OrderSide::Bids,
                f64_to_decimal(390.0),
                f64_to_decimal(1.0),
                vec![],
            ),
        );
        asks.insert(
            f64_to_decimal(390.1),
            aetelier_types::levels::Level::new(
                0,
                aetelier_types::OrderSide::Asks,
                f64_to_decimal(390.1),
                f64_to_decimal(2.0),
                vec![],
            ),
        );
        let snaps: Vec<Orderbook> = ts_list
            .iter()
            .map(|t| Orderbook {
                orderbook_id: 0,
                orderbook_ts_us: t * 1_000,
                source_orderbook_ts_us: t * 1_000,
                local_orderbook_ts_us: t * 1_000,
                source_orderbook_rtt_us: 0,
                pair: pair.clone(),
                exchange: "hyperliquid".into(),
                bids: bids.clone(),
                asks: asks.clone(),
            })
            .collect();
        let dir = w.dir.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        write_ob_parquet(&snaps, &dir, "sync").unwrap()
    };
    let config = serde_json::json!({
        "order_source": "strategy",
        "tape": "upload",
        "notional_usd": 1000.0 + ts_list.len() as f64,
        "grace_ms": 300000,
        "cross_window_ms": 60000,
    });
    let (status, body) = post_trace(
        w,
        &config,
        &[("orderbook", &deep_ob), ("trades", &w.trades_parquet)],
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let trace_id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(w, &trace_id, OWNER).await;
    trace_id
}

fn inputs_dir(w: &World, trace_id: &str) -> PathBuf {
    w.dir.join("traces").join(trace_id).join("inputs")
}

#[tokio::test]
async fn live_depth_and_schema_find_parquet_under_day_directories() {
    let (w, _, _) = live_world("live_day_dirs", true);

    let (status, depth) = get_json(&w, "/api/tracer/traces/live/depth", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let rows = depth.unwrap()["rows"].as_array().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ts_ms"].as_u64().unwrap(), w.span.1);

    for (source, first_field) in [
        ("orderbook", "orderbook_ts_us"),
        ("trades", "source_trade_ts_us"),
    ] {
        let (status, schema) = get_json(
            &w,
            &format!("/api/tracer/traces/live/schema?source={source}"),
            Some(OWNER),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{source}");
        assert_eq!(schema.unwrap()["fields"][0]["name"], first_field);
    }

    let (status, trades) =
        get_json(&w, "/api/tracer/traces/live/trades?limit=5", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(trades.unwrap()["total"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn an_evicted_live_tape_signals_trace_still_serves_its_tape() {
    let (w, _, _) = live_world("live_evicted_signals", false);
    write_live_signals_record(&w, "tr_liveevict001", true);

    for route in ["orderbook", "trades", "depth"] {
        let (status, body) = get_json(
            &w,
            &format!("/api/tracer/traces/tr_liveevict001/{route}"),
            Some(OWNER),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{route}: {body:?}");
    }
    let (_, trades) = get_json(&w, "/api/tracer/traces/tr_liveevict001/trades", Some(OWNER)).await;
    assert!(trades.unwrap()["total"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn depth_and_trades_are_served_from_the_cache_after_the_first_read() {
    let w = world("cache_survives_file_loss");
    let (t_min, t_max) = w.span;
    let ts_list: Vec<u64> = (0..5).map(|i| t_min + i * (t_max - t_min) / 4).collect();
    let trace_id = upload_deep_trace(&w, "deep_a", &ts_list).await;

    let (status, first) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/depth"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(first.unwrap()["rows"].as_array().unwrap().len(), 5);
    std::fs::remove_dir_all(inputs_dir(&w, &trace_id)).unwrap();

    let (status, again) = get_json(
        &w,
        &format!(
            "/api/tracer/traces/{trace_id}/depth?from={}&to={}&max=2",
            ts_list[1], ts_list[3]
        ),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let rows = again.unwrap()["rows"].as_array().unwrap().clone();
    assert_eq!(rows.first().unwrap()["ts_ms"].as_u64().unwrap(), ts_list[1]);
    assert_eq!(rows.last().unwrap()["ts_ms"].as_u64().unwrap(), ts_list[3]);
    assert!(rows.len() <= 3);

    let (status, trades) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/trades"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(trades.unwrap()["total"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn the_tape_cache_keeps_several_traces_at_once() {
    let w = world("cache_capacity");
    let (t_min, t_max) = w.span;
    let a = upload_deep_trace(&w, "deep_a", &[t_min, t_max]).await;
    let b = upload_deep_trace(&w, "deep_b", &[t_min, (t_min + t_max) / 2, t_max]).await;
    assert_ne!(a, b);

    for id in [&a, &b] {
        let (status, _) =
            get_json(&w, &format!("/api/tracer/traces/{id}/depth"), Some(OWNER)).await;
        assert_eq!(status, axum::http::StatusCode::OK);
    }
    std::fs::remove_dir_all(inputs_dir(&w, &a)).unwrap();

    let (status, depth) = get_json(&w, &format!("/api/tracer/traces/{a}/depth"), Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(depth.unwrap()["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn deleting_a_trace_drops_its_cached_tape() {
    let w = world("cache_delete");
    let (t_min, t_max) = w.span;
    let a = upload_deep_trace(&w, "deep_a", &[t_min, t_max]).await;
    let (status, _) = get_json(&w, &format!("/api/tracer/traces/{a}/depth"), Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);

    assert_eq!(
        delete_trace(&w, &a, Some(OWNER)).await,
        axum::http::StatusCode::NO_CONTENT
    );

    let (status, _) = get_json(&w, &format!("/api/tracer/traces/{a}/depth"), Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_list_omits_preflight_files_and_reports_their_count() {
    let w = world("list_trim");
    let (t_min, t_max) = w.span;
    let a = upload_deep_trace(&w, "deep_a", &[t_min, t_max]).await;

    let (status, list) = get_json(&w, "/api/tracer/traces", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let list = list.unwrap();
    let listed = list["traces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["trace_id"] == a)
        .unwrap();
    assert!(listed["preflight"].get("files").is_none(), "{listed}");
    assert_eq!(listed["preflight"]["files_n"], 2);
    assert!(listed["preflight"].get("span").is_some());

    let (_, single) = get_json(&w, &format!("/api/tracer/traces/{a}"), Some(OWNER)).await;
    let single = single.unwrap();
    assert_eq!(single["preflight"]["files"].as_array().unwrap().len(), 2);
    assert!(single["preflight"].get("files_n").is_none());
}

#[tokio::test]
async fn live_trades_grow_when_a_new_tape_file_appears() {
    let (w, trades_dir, _) = live_world("live_incremental", true);
    let (status, before) = get_json(&w, "/api/tracer/traces/live/trades", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let before = before.unwrap()["total"].as_u64().unwrap();

    let pair = TradingPair::from_str("xyz:TSLA/USDC").unwrap();
    let extra: Vec<Trade> = (0..3)
        .map(|i| Trade {
            source_trade_ts_us: (w.span.1 + 1_000 + i) * 1_000,
            local_trade_ts_us: (w.span.1 + 1_000 + i) * 1_000,
            source_trade_rtt_us: 0,
            pair: pair.clone(),
            side: TradeSide::Buy,
            amount: f64_to_decimal(1.0),
            price: f64_to_decimal(391.0),
            exchange: "hyperliquid".into(),
            id: format!("extra{i:08}"),
            origin: Default::default(),
        })
        .collect();
    write_trades_parquet(&extra, &trades_dir.join("trades_sync_2.parquet")).unwrap();

    let (status, after) = get_json(&w, "/api/tracer/traces/live/trades?limit=1", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let after = after.unwrap();
    assert_eq!(after["total"].as_u64().unwrap(), before + 3);
    assert_eq!(after["trades"][0]["px"], 391.0);
}

async fn get_text(w: &World, uri: &str, user: Option<&str>) -> (axum::http::StatusCode, String) {
    let mut req = axum::http::Request::builder().method("GET").uri(uri);
    if let Some(u) = user {
        req = req.header("x-aetelier-user", u);
    }
    let req = req.body(axum::body::Body::empty()).unwrap();
    let resp = app(w).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn metrics_count_trace_jobs_tape_loads_and_cache_entries() {
    let w = world("metrics_traces");
    let (t_min, t_max) = w.span;
    let a = upload_deep_trace(&w, "deep_a", &[t_min, t_max]).await;
    let (status, _) = get_json(
        &w,
        &format!("/api/tracer/traces/{a}/orderbook"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let (status, _) = get_json(&w, &format!("/api/tracer/traces/{a}/trades"), Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);

    let (status, body) = get_text(&w, "/metrics", None).await;

    assert_eq!(status, axum::http::StatusCode::OK);
    for line in [
        "tracer_trace_jobs_total{status=\"complete\"} 1",
        "tracer_tape_loads_total{result=\"ok\"} 1",
        "tracer_tape_cache_entries 1",
        "tracer_http_requests_total{route=\"/api/tracer/traces/{id}/orderbook\",status=\"200\"} 1",
    ] {
        assert!(
            body.lines().any(|l| l == line),
            "missing {line:?} in\n{body}"
        );
    }
    assert!(
        !body.contains("tracer_trace_jobs_total{status=\"failed\"}"),
        "{body}"
    );
}

#[tokio::test]
async fn the_list_pages_newest_first_with_an_opaque_cursor() {
    let w = world("list_paging");
    let (t_min, t_max) = w.span;
    let mut ids = Vec::new();
    for n in 2..5 {
        let ts: Vec<u64> = (0..n)
            .map(|i| t_min + i * (t_max - t_min) / (n - 1))
            .collect();
        ids.push(upload_deep_trace(&w, &format!("deep_{n}"), &ts).await);
    }
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3
    );

    let (status, all) = get_json(&w, "/api/tracer/traces", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let all = all.unwrap();
    assert_eq!(all["total"], 3);
    assert_eq!(all["traces"].as_array().unwrap().len(), 3);
    assert!(all["older_cursor"].is_null());
    let order: Vec<String> = all["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["trace_id"].as_str().unwrap().to_owned())
        .collect();
    let created: Vec<u64> = all["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["created_ms"].as_u64().unwrap())
        .collect();
    assert!(
        created.windows(2).all(|pair| pair[0] >= pair[1]),
        "{created:?}"
    );

    let (status, first) = get_json(&w, "/api/tracer/traces?limit=2", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let first = first.unwrap();
    assert_eq!(first["total"], 3);
    let page1: Vec<&str> = first["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["trace_id"].as_str().unwrap())
        .collect();
    assert_eq!(page1, [order[0].as_str(), order[1].as_str()]);
    let cursor = first["older_cursor"].as_str().unwrap().to_owned();

    let (status, second) = get_json(
        &w,
        &format!("/api/tracer/traces?limit=2&cursor={cursor}"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let second = second.unwrap();
    let page2: Vec<&str> = second["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["trace_id"].as_str().unwrap())
        .collect();
    assert_eq!(page2, [order[2].as_str()]);
    assert!(second["older_cursor"].is_null());

    let (status, _) = get_json(&w, "/api/tracer/traces?cursor=not-a-cursor", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    let (status, capped) = get_json(&w, "/api/tracer/traces?limit=0", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(capped.unwrap()["traces"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn params_route_serves_the_frozen_parameters_file() {
    let w = world("params_route");

    let (status, params) = get_json(&w, "/api/tracer/params", Some(OWNER)).await;

    assert_eq!(status, axum::http::StatusCode::OK);
    let params = params.unwrap();
    let file: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(repo_root().join("params/tracer_params.v1.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(params, file);
    assert_eq!(params["params_version"], 1);
}

#[tokio::test]
async fn artifacts_list_and_download_byte_exact_files() {
    let w = world("artifacts");
    let (t_min, t_max) = w.span;
    let a = upload_deep_trace(&w, "deep_a", &[t_min, t_max]).await;

    let (status, listing) = get_json(
        &w,
        &format!("/api/tracer/traces/{a}/artifacts"),
        Some(OWNER),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let listing = listing.unwrap();
    assert_eq!(listing["schema"], "tracer.traceartifacts.v1");
    assert_eq!(listing["status"], "complete");
    let names: Vec<String> = listing["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        &names[..3],
        ["trace.json", "corrected_ledger.json", "fillrates.json"]
    );
    assert!(
        names[3..]
            .iter()
            .all(|n| n.starts_with("ledger/events_") && n.ends_with(".jsonl")),
        "{names:?}"
    );
    assert!(names.len() >= 4, "{names:?}");

    for artifact in listing["artifacts"].as_array().unwrap() {
        let name = artifact["name"].as_str().unwrap();
        let (status, body) = get_text(
            &w,
            &format!("/api/tracer/traces/{a}/artifacts?name={name}"),
            Some(OWNER),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{name}");
        let on_disk = std::fs::read(w.dir.join("traces").join(&a).join(name)).unwrap();
        assert_eq!(body.as_bytes(), on_disk.as_slice(), "{name}");
        assert_eq!(
            artifact["bytes"].as_u64().unwrap() as usize,
            on_disk.len(),
            "{name}"
        );
        assert_eq!(
            artifact["sha256"],
            format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(&on_disk)),
            "{name}"
        );
    }

    for name in [
        "../trace.json",
        "ledger/../trace.json",
        "inputs/x",
        "missing.json",
        "",
    ] {
        let (status, _) = get_text(
            &w,
            &format!("/api/tracer/traces/{a}/artifacts?name={name}"),
            Some(OWNER),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::NOT_FOUND, "{name:?}");
    }
    let (status, _) = get_json(&w, "/api/tracer/traces/live/artifacts", Some(OWNER)).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    let (status, _) = get_json(
        &w,
        &format!("/api/tracer/traces/{a}/artifacts"),
        Some("user-b"),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}
