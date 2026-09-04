use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use http_body_util::BodyExt;
use tower::ServiceExt;

#[path = "../src/api.rs"]
#[allow(dead_code)]
mod api;
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

    let rt = traces::TraceRuntime::start(
        dir.join("traces"),
        repo_root().join("params/tracer_params.v1.json"),
        300_000,
        60_000,
        None,
        None,
        None,
    )
    .unwrap();
    let ledger_dir = dir.join("live_ledger");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    let state = AppState {
        status: Arc::new(RwLock::new(serde_json::json!({"mode": "serve"}))),
        ledger_live: Arc::new(RwLock::new(LedgerLive::default())),
        ready: Arc::new(RwLock::new(true)),
        ledger_dir,
        artifacts_dir: None,
        trades_binding: None,
        books_binding: None,
        symbol_stem: None,
        traces: Some(rt),
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

async fn wait_complete(w: &World, trace_id: &str) -> serde_json::Value {
    for _ in 0..300 {
        let (status, body) = get_json(w, &format!("/api/tracer/traces/{trace_id}"), None).await;
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

    let record = wait_complete(&w, &trace_id).await;
    assert_eq!(record["order_source"], "strategy");
    assert_eq!(record["user_id"], "user-a");
    assert!(record["summary"]["n_signals"].as_u64().unwrap() > 0);

    let (status, ledger) =
        get_json(&w, &format!("/api/tracer/traces/{trace_id}/ledger"), None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let ledger = ledger.unwrap();
    assert_eq!(ledger["schema"], "tracer.corrected.v1");
    assert_eq!(ledger["mode"], "trace");
    assert_eq!(ledger["trace_id"], trace_id.as_str());

    let (status, fr) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/fillrates"),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(fr.unwrap()["schema"], "tracer.fillrates.v1");

    let (status, events) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/events?limit=10"),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let events = events.unwrap();
    assert_eq!(events["events"].as_array().unwrap().len(), 10);
    assert_eq!(events["events"][0]["mode"], "trace");

    let (status, book) = get_json(&w, &format!("/api/tracer/traces/{trace_id}/book"), None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let book = book.unwrap();
    assert_eq!(book["schema"], "tracer.tracebook.v1");
    assert_eq!(book["points"].as_array().unwrap().len(), 3);
    assert_eq!(book["points"][0]["bid_px"], 390.0);

    let (status, prints) = get_json(
        &w,
        &format!("/api/tracer/traces/{trace_id}/prints?limit=5"),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let prints = prints.unwrap();
    assert_eq!(prints["prints"].as_array().unwrap().len(), 5);
    assert_eq!(
        prints["prints"][0]["ts_ms"].as_u64().unwrap(),
        w.span.1,
        "prints must page newest-first"
    );
    assert!(prints["older_cursor"].as_u64().is_some());

    for source in ["orderbook", "trades"] {
        let (status, schema) = get_json(
            &w,
            &format!("/api/tracer/traces/{trace_id}/schema?source={source}"),
            None,
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

    let (status, _) = get_json(&w, "/api/tracer/traces/tr_missing000000", None).await;
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
        None,
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

    let (status, _) = get_json(&w, "/api/tracer/traces", None).await;
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
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["status"], "preflight");
    assert!(body["preflight"]["span"]["start_ms"].as_u64().is_some());

    let (_, list) = get_json(&w, "/api/tracer/traces", None).await;
    assert_eq!(list.unwrap()["traces"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn resubmitting_the_same_config_is_idempotent() {
    let w = world("idempotent");
    let config = serde_json::json!({"order_source": "strategy", "tape": "upload"});
    let files: &[(&str, &Path)] = &[("orderbook", &w.ob_parquet), ("trades", &w.trades_parquet)];
    let (status, body) = post_trace(&w, &config, files, None).await;
    assert_eq!(status, axum::http::StatusCode::CREATED);
    let id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &id).await;
    let (status, body2) = post_trace(&w, &config, files, None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(body2["trace_id"], id.as_str());
    assert_eq!(body2["status"], "complete");
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
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    let trace_id = body["trace_id"].as_str().unwrap().to_string();
    wait_complete(&w, &trace_id).await;

    let (status, depth) = get_json(&w, &format!("/api/tracer/traces/{trace_id}/depth"), None).await;
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
        None,
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
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let capped = capped.unwrap();
    let crows = capped["rows"].as_array().unwrap();
    assert!(crows.len() <= 3, "got {}", crows.len());
    assert_eq!(crows[0]["ts_ms"].as_u64().unwrap(), ts_list[0]);
    assert_eq!(crows.last().unwrap()["ts_ms"].as_u64().unwrap(), ts_list[4]);

    let (status, _) = get_json(&w, "/api/tracer/traces/tr_missing0000/depth", None).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);

    let (status, _) = get_json(&w, "/api/tracer/traces/live/depth", None).await;
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
            None,
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
