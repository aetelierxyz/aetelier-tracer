mod golden_pin;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use sha2::{Digest, Sha256};

use aetelier_io::orderbooks::ob_parquet::write_ob_parquet;
use aetelier_io::trades::trades_parquet::write_trades_parquet;
use aetelier_types::orderbooks::core::{Orderbook, f64_to_decimal};
use aetelier_types::trades::{Trade, TradeSide};
use aetelier_types::trading_pair::TradingPair;

use tracer_engine::bars::read_fills_csv;
use tracer_engine::corrected::{
    DEFAULT_CROSS_WINDOW_MS, DEFAULT_GRACE_MS, emit_events, evaluate, write_artifacts,
};
use tracer_engine::ledger::LedgerWriter;
use tracer_engine::nyse::regime;
use tracer_engine::paper::ExecutedTrade;
use tracer_engine::params::TracerParams;
use tracer_engine::replay::load_symbol_data;
use tracer_engine::trace::{OrderSourceSpec, TapeRole, TraceRequest, execute, prepare, sniff_role};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn load_params() -> TracerParams {
    TracerParams::load(&repo_root().join("params/tracer_params.v1.json")).unwrap()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tracer_trace_gate_{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn fills_to_trades_parquet(fills_csv: &Path, coin: &str, out: &Path) {
    let rows = read_fills_csv(fills_csv).unwrap();
    rows_to_trades_parquet(&rows, coin, out, 0);
}

fn rows_to_trades_parquet(
    rows: &[tracer_engine::bars::FillRow],
    coin: &str,
    out: &Path,
    id_offset: usize,
) {
    let pair = TradingPair::from_str(&format!("{coin}/USDC")).unwrap();
    let trades: Vec<Trade> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| (i + id_offset, r))
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
            id: format!("{coin}|{i:012}"),
            origin: Default::default(),
        })
        .collect();
    write_trades_parquet(&trades, out).unwrap();
}

fn ob_snapshot(ts_us: u64, pair: &TradingPair, bid: f64, ask: f64) -> Orderbook {
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();
    bids.insert(
        f64_to_decimal(bid),
        aetelier_types::levels::Level::new(
            0,
            aetelier_types::OrderSide::Bids,
            f64_to_decimal(bid),
            f64_to_decimal(1.0),
            vec![],
        ),
    );
    asks.insert(
        f64_to_decimal(ask),
        aetelier_types::levels::Level::new(
            1,
            aetelier_types::OrderSide::Asks,
            f64_to_decimal(ask),
            f64_to_decimal(1.0),
            vec![],
        ),
    );
    Orderbook {
        orderbook_id: 0,
        orderbook_ts_us: ts_us,
        source_orderbook_ts_us: ts_us,
        local_orderbook_ts_us: ts_us,
        source_orderbook_rtt_us: 0,
        pair: pair.clone(),
        exchange: "hyperliquid".into(),
        bids,
        asks,
    }
}

fn write_ob_file(dir: &Path, coin: &str, ts_ms: &[u64]) -> PathBuf {
    let pair = TradingPair::from_str(&format!("{coin}/USDC")).unwrap();
    let snaps: Vec<Orderbook> = ts_ms
        .iter()
        .map(|t| ob_snapshot(t * 1_000, &pair, 100.0, 100.1))
        .collect();
    write_ob_parquet(&snaps, dir, "sync").unwrap()
}

fn render_signals(trades: &[ExecutedTrade]) -> String {
    let mut out = String::from(tracer_engine::trace::SIGNALS_HEADER);
    out.push('\n');
    for t in trades {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            t.symbol,
            t.side,
            t.score,
            t.decile,
            t.t_entry_ms,
            t.entry_px,
            t.t_exit_ms,
            t.t_exit_real_ms,
            t.exit_px,
            t.fee_entry_usd,
            t.fee_exit_usd,
            t.pnl_usd,
            regime(t.t_entry_ms).label()
        ));
    }
    out
}

fn strip_provenance(v: &mut serde_json::Value) {
    if let Some(obj) = v.as_object_mut() {
        obj.remove("mode");
        obj.remove("trace_id");
        obj.remove("input_sha256");
    }
}

const FROZEN_REPLAY_FILES: [&str; 2] = ["corrected_ledger.json", "fillrates.json"];

fn holds_frozen_replay(dir: &Path) -> bool {
    FROZEN_REPLAY_FILES
        .iter()
        .all(|name| dir.join(name).is_file())
}

fn frozen_replay_dir() -> Option<PathBuf> {
    match std::env::var_os("TRACER_REPLAY_DIR").filter(|v| !v.is_empty()) {
        Some(set) => {
            let dir = PathBuf::from(set);
            assert!(
                holds_frozen_replay(&dir),
                "TRACER_REPLAY_DIR={} lacks {}",
                dir.display(),
                FROZEN_REPLAY_FILES.join(" or ")
            );
            Some(dir)
        }
        None => {
            let dir = repo_root().join("out/replay");
            holds_frozen_replay(&dir).then_some(dir)
        }
    }
}

fn assert_frozen_replay_pinned(dir: &Path) {
    let pins = std::fs::read_to_string(repo_root().join("fixtures/frozen_replay.sha256")).unwrap();
    let mut pinned = Vec::new();
    for line in pins.lines() {
        let (want, name) = line
            .split_once("  ")
            .expect("frozen_replay.sha256 line must be '<sha256>  <file>'");
        let got = format!(
            "{:x}",
            Sha256::digest(std::fs::read(dir.join(name)).unwrap())
        );
        assert_eq!(
            got,
            want,
            "{} does not match its pin in fixtures/frozen_replay.sha256",
            dir.join(name).display()
        );
        pinned.push(name);
    }
    pinned.sort_unstable();
    assert_eq!(pinned, ["corrected_ledger.json", "fillrates.json"]);
}

fn artifact_norm(path: &Path) -> serde_json::Value {
    let mut v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    strip_provenance(&mut v);
    v
}

fn events_norm(ledger_dir: &Path) -> Vec<serde_json::Value> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(ledger_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for f in files {
        for line in std::fs::read_to_string(&f).unwrap().lines() {
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            strip_provenance(&mut v);
            out.push(v);
        }
    }
    out
}

struct MiniWorld {
    dir: PathBuf,
    params: TracerParams,
    replay_out: PathBuf,
    replay_trades: Vec<ExecutedTrade>,
    trades_parquet: PathBuf,
}

fn mini_world(name: &str) -> MiniWorld {
    let dir = temp_dir(name);
    let fills_dir = dir.join("fills");
    std::fs::create_dir_all(&fills_dir).unwrap();
    std::fs::copy(
        repo_root().join("fixtures/mini_fills_xyz_TSLA.csv"),
        fills_dir.join("xyz_TSLA.csv"),
    )
    .unwrap();
    let mut params = load_params();
    params.symbols.retain(|s| s.stem == "xyz_TSLA");

    let data = load_symbol_data(&fills_dir, &params).unwrap();
    let replay_trades = tracer_engine::replay::paper_trades_from(&data, &params).unwrap();
    let verdicts = evaluate(
        &replay_trades,
        &data,
        &params,
        DEFAULT_GRACE_MS,
        DEFAULT_CROSS_WINDOW_MS,
    )
    .unwrap();
    let replay_out = dir.join("replay");
    write_artifacts(
        &verdicts,
        &params,
        DEFAULT_GRACE_MS,
        DEFAULT_CROSS_WINDOW_MS,
        &replay_out,
    )
    .unwrap();
    let mut writer = LedgerWriter::new(&replay_out.join("ledger")).unwrap();
    emit_events(&verdicts, &params, DEFAULT_GRACE_MS, &mut writer).unwrap();

    let trades_parquet = dir.join("xyz_TSLA_trades.parquet");
    fills_to_trades_parquet(&fills_dir.join("xyz_TSLA.csv"), "xyz:TSLA", &trades_parquet);
    MiniWorld {
        dir,
        params,
        replay_out,
        replay_trades,
        trades_parquet,
    }
}

fn expect_reject(
    r: Result<tracer_engine::trace::TracePrepared, tracer_engine::trace::PreflightReport>,
) -> tracer_engine::trace::PreflightReport {
    match r {
        Err(report) => report,
        Ok(_) => panic!("expected strict rejection, prepare was clean"),
    }
}

fn signals_request(w: &MiniWorld, signals_path: PathBuf) -> TraceRequest {
    TraceRequest {
        tape_files: vec![(TapeRole::Trades, w.trades_parquet.clone())],
        orders: OrderSourceSpec::Signals(signals_path),
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    }
}

#[test]
fn mini_strategy_trace_reproduces_replay_paper_trades() {
    let w = mini_world("strategy");
    let req = TraceRequest {
        tape_files: vec![(TapeRole::Trades, w.trades_parquet.clone())],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let prepared = prepare(&req, &w.params).unwrap().unwrap();
    assert_eq!(prepared.trades, w.replay_trades);
}

#[test]
fn mini_signals_trace_reproduces_replay_artifacts_modulo_provenance() {
    let w = mini_world("signals");
    let signals_path = w.dir.join("signals.csv");
    std::fs::write(&signals_path, render_signals(&w.replay_trades)).unwrap();
    let req = signals_request(&w, signals_path);
    let prepared = prepare(&req, &w.params).unwrap().unwrap();
    assert_eq!(prepared.trades, w.replay_trades);

    let trace_out = w.dir.join("trace");
    let summary = execute(
        &prepared,
        DEFAULT_GRACE_MS,
        DEFAULT_CROSS_WINDOW_MS,
        &trace_out,
    )
    .unwrap();
    assert_eq!(summary.n_signals, w.replay_trades.len());

    for name in ["corrected_ledger.json", "fillrates.json"] {
        assert_eq!(
            artifact_norm(&trace_out.join(name)),
            artifact_norm(&w.replay_out.join(name)),
            "{name} diverged"
        );
    }
    assert_eq!(
        events_norm(&trace_out.join("ledger")),
        events_norm(&w.replay_out.join("ledger"))
    );
}

#[test]
fn trace_artifacts_carry_trace_stamps() {
    let w = mini_world("stamps");
    let signals_path = w.dir.join("signals.csv");
    std::fs::write(&signals_path, render_signals(&w.replay_trades)).unwrap();
    let req = signals_request(&w, signals_path);
    let prepared = prepare(&req, &w.params).unwrap().unwrap();
    let trace_out = w.dir.join("trace");
    execute(
        &prepared,
        DEFAULT_GRACE_MS,
        DEFAULT_CROSS_WINDOW_MS,
        &trace_out,
    )
    .unwrap();

    let ledger: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(trace_out.join("corrected_ledger.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(ledger["mode"], "trace");
    assert_eq!(ledger["trace_id"], prepared.trace_id.as_str());
    let sha = ledger["input_sha256"].as_object().unwrap();
    assert!(sha.keys().any(|k| k.starts_with("trades/")));
    assert!(sha.keys().any(|k| k.starts_with("signals/")));

    let events = events_norm(&trace_out.join("ledger"));
    assert!(!events.is_empty());
    let raw = std::fs::read_dir(trace_out.join("ledger"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| std::fs::read_to_string(e.path()).unwrap())
        .collect::<String>();
    assert!(raw.contains("\"mode\":\"trace\""));
    assert!(raw.contains(&format!("\"trace_id\":\"{}\"", prepared.trace_id)));
}

#[test]
fn row_error_permutations_produce_a_complete_report() {
    let w = mini_world("rowerrors");
    let t = &w.replay_trades[0];
    let hold = w.params.hold_ms;
    let span_break = t.t_entry_ms - 200 * 86_400_000;
    let mut csv = String::from(tracer_engine::trace::SIGNALS_HEADER);
    csv.push('\n');
    let good = |t: &ExecutedTrade| {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            t.symbol,
            t.side,
            t.score,
            t.decile,
            t.t_entry_ms,
            t.entry_px,
            t.t_exit_ms,
            t.t_exit_real_ms,
            t.exit_px,
            t.fee_entry_usd,
            t.fee_exit_usd,
            t.pnl_usd,
            regime(t.t_entry_ms).label()
        )
    };
    csv.push_str(&good(t));
    csv.push_str("xyz_TSLA,1,0.5\n");
    csv.push_str(&format!(
        "xyz_GME,1,0.5,10,{},100.0,{},{},100.0,-0.1,-0.1,0.0,{}\n",
        t.t_entry_ms,
        t.t_entry_ms + hold,
        t.t_entry_ms + hold,
        regime(t.t_entry_ms).label()
    ));
    csv.push_str(&format!(
        "xyz_TSLA,2,0.5,11,{},-4.0,{},{},100.0,-0.1,-0.1,0.0,{}\n",
        t.t_entry_ms,
        t.t_entry_ms + hold + 1,
        t.t_entry_ms + hold + 2,
        regime(t.t_entry_ms).label()
    ));
    csv.push_str(&format!(
        "xyz_TSLA,-1,NaN,5,{span_break},100.0,{},{},100.0,-0.1,-0.1,0.0,{}\n",
        span_break + hold,
        span_break,
        if regime(span_break).label() == "RTH" {
            "EXT"
        } else {
            "RTH"
        }
    ));
    csv.push_str(&good(t));
    let signals_path = w.dir.join("bad_signals.csv");
    std::fs::write(&signals_path, csv).unwrap();

    let req = signals_request(&w, signals_path);
    let report = expect_reject(prepare(&req, &w.params).unwrap());
    assert_eq!(report.signal_rows, 6);
    let has = |row: usize, field: &str| {
        assert!(
            report
                .row_errors
                .iter()
                .any(|e| e.row == row && e.field == field),
            "missing error row {row} field {field:?} in {:?}",
            report.row_errors
        );
    };
    has(2, "");
    has(3, "symbol");
    has(4, "side");
    has(4, "decile");
    has(4, "entry_px");
    has(4, "t_exit_ms");
    has(4, "t_exit_real_ms");
    has(5, "score");
    has(5, "t_entry_ms");
    has(5, "regime");
    has(6, "");
    assert!(!report.row_errors.iter().any(|e| e.row == 1));
}

#[test]
fn shared_span_is_the_pair_intersection_and_empty_span_rejects() {
    let w = mini_world("span");
    let trades = read_fills_csv(&w.dir.join("fills").join("xyz_TSLA.csv")).unwrap();
    let t_min = trades.first().unwrap().time_ms;
    let t_max = trades.last().unwrap().time_ms;

    let ob_dir = w.dir.join("ob_overlap");
    std::fs::create_dir_all(&ob_dir).unwrap();
    let mid = t_min + (t_max - t_min) / 2;
    let ob_path = write_ob_file(&ob_dir, "xyz:TSLA", &[mid, t_max + 60_000]);
    let req = TraceRequest {
        tape_files: vec![
            (TapeRole::Trades, w.trades_parquet.clone()),
            (TapeRole::Orderbook, ob_path),
        ],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    match prepare(&req, &w.params).unwrap() {
        Ok(p) => {
            let span = p.report.span.unwrap();
            assert_eq!(span.start_ms, mid);
            assert_eq!(span.end_ms, t_max);
            let trades_file = p
                .report
                .files
                .iter()
                .find(|f| f.role == TapeRole::Trades)
                .unwrap();
            assert_eq!(trades_file.min_ts_ms, t_min);
            assert_eq!(trades_file.max_ts_ms, t_max);
            assert_eq!(trades_file.events, trades.len());
        }
        Err(r) => panic!("expected clean prepare, got {:?}", r.row_errors),
    }

    let ob_dir2 = w.dir.join("ob_disjoint");
    std::fs::create_dir_all(&ob_dir2).unwrap();
    let ob_path2 = write_ob_file(
        &ob_dir2,
        "xyz:TSLA",
        &[t_max + 3_600_000, t_max + 7_200_000],
    );
    let req2 = TraceRequest {
        tape_files: vec![
            (TapeRole::Trades, w.trades_parquet.clone()),
            (TapeRole::Orderbook, ob_path2),
        ],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let report = expect_reject(prepare(&req2, &w.params).unwrap());
    assert!(
        report
            .row_errors
            .iter()
            .any(|e| e.reason.contains("shared span is empty"))
    );
}

#[test]
fn tape_symbol_mismatch_and_schema_mismatch_reject() {
    let w = mini_world("schema");
    let ob_dir = w.dir.join("ob_other");
    std::fs::create_dir_all(&ob_dir).unwrap();
    let trades = read_fills_csv(&w.dir.join("fills").join("xyz_TSLA.csv")).unwrap();
    let ob_path = write_ob_file(
        &ob_dir,
        "xyz:AAPL",
        &[
            trades.first().unwrap().time_ms,
            trades.last().unwrap().time_ms,
        ],
    );
    let req = TraceRequest {
        tape_files: vec![
            (TapeRole::Trades, w.trades_parquet.clone()),
            (TapeRole::Orderbook, ob_path.clone()),
        ],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let report = expect_reject(prepare(&req, &w.params).unwrap());
    assert!(
        report
            .row_errors
            .iter()
            .any(|e| e.reason.contains("symbol mismatch"))
    );

    assert_eq!(sniff_role(&ob_path).unwrap(), TapeRole::Orderbook);
    assert_eq!(sniff_role(&w.trades_parquet).unwrap(), TapeRole::Trades);

    let req2 = TraceRequest {
        tape_files: vec![(TapeRole::Trades, ob_path)],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let report2 = expect_reject(prepare(&req2, &w.params).unwrap());
    assert!(
        report2
            .row_errors
            .iter()
            .any(|e| e.field.starts_with("file:") || e.field == "tape"),
        "{:?}",
        report2.row_errors
    );
}

#[test]
fn signals_row_cap_rejects() {
    let w = mini_world("rowcap");
    let t = &w.replay_trades[0];
    let mut csv = String::from(tracer_engine::trace::SIGNALS_HEADER);
    csv.push('\n');
    let row = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        t.symbol,
        t.side,
        t.score,
        t.decile,
        t.t_entry_ms,
        t.entry_px,
        t.t_exit_ms,
        t.t_exit_real_ms,
        t.exit_px,
        t.fee_entry_usd,
        t.fee_exit_usd,
        t.pnl_usd,
        regime(t.t_entry_ms).label()
    );
    for _ in 0..=tracer_engine::trace::MAX_SIGNAL_ROWS {
        csv.push_str(&row);
    }
    let signals_path = w.dir.join("cap_signals.csv");
    std::fs::write(&signals_path, csv).unwrap();
    let req = signals_request(&w, signals_path);
    let report = expect_reject(prepare(&req, &w.params).unwrap());
    assert!(
        report
            .row_errors
            .iter()
            .any(|e| e.reason.contains("row cap"))
    );
}

#[test]
fn live_shaped_multifile_tape_prepares_with_the_union_span() {
    let w = mini_world("multifile");
    let rows = read_fills_csv(&w.dir.join("fills").join("xyz_TSLA.csv")).unwrap();
    let mid_ms = rows[rows.len() / 2].time_ms;
    let split = rows.partition_point(|r| r.time_ms <= mid_ms);
    let t_min = rows.first().unwrap().time_ms;
    let t_max = rows.last().unwrap().time_ms;

    let trades_a = w.dir.join("trades_a.parquet");
    let trades_b = w.dir.join("trades_b.parquet");
    rows_to_trades_parquet(&rows[..split], "xyz:TSLA", &trades_a, 0);
    rows_to_trades_parquet(&rows[split..], "xyz:TSLA", &trades_b, split);

    let ob_dir_a = w.dir.join("ob_a");
    let ob_dir_b = w.dir.join("ob_b");
    std::fs::create_dir_all(&ob_dir_a).unwrap();
    std::fs::create_dir_all(&ob_dir_b).unwrap();
    let ob_a = write_ob_file(&ob_dir_a, "xyz:TSLA", &[t_min, mid_ms]);
    let ob_b = write_ob_file(&ob_dir_b, "xyz:TSLA", &[mid_ms + 60_000, t_max]);

    let req = TraceRequest {
        tape_files: vec![
            (TapeRole::Trades, trades_a),
            (TapeRole::Trades, trades_b),
            (TapeRole::Orderbook, ob_a),
            (TapeRole::Orderbook, ob_b),
        ],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let prepared = match prepare(&req, &w.params).unwrap() {
        Ok(p) => p,
        Err(r) => panic!("multi-file tape rejected: {:?}", r.row_errors),
    };
    let span = prepared.report.span.unwrap();
    assert_eq!(span.start_ms, t_min);
    assert_eq!(span.end_ms, t_max);
    assert_eq!(prepared.report.files.len(), 4);
    assert_eq!(prepared.trades, w.replay_trades);
    assert_eq!(
        prepared.books.first().unwrap().ts_ms,
        t_min,
        "merged book ticks must span both files"
    );
    assert_eq!(prepared.books.last().unwrap().ts_ms, t_max);
}

#[test]
fn a_fully_duplicated_live_trades_file_is_skipped_not_rejected() {
    let w = mini_world("dupfile");
    let rows = read_fills_csv(&w.dir.join("fills").join("xyz_TSLA.csv")).unwrap();
    let t_min = rows.first().unwrap().time_ms;
    let t_max = rows.last().unwrap().time_ms;

    let trades_a = w.dir.join("trades_a.parquet");
    let trades_dup = w.dir.join("trades_dup.parquet");
    rows_to_trades_parquet(&rows, "xyz:TSLA", &trades_a, 0);
    rows_to_trades_parquet(&rows, "xyz:TSLA", &trades_dup, 0);

    let ob_dir = w.dir.join("ob");
    std::fs::create_dir_all(&ob_dir).unwrap();
    let ob = write_ob_file(&ob_dir, "xyz:TSLA", &[t_min, t_max]);

    let req = TraceRequest {
        tape_files: vec![
            (TapeRole::Trades, trades_a),
            (TapeRole::Trades, trades_dup),
            (TapeRole::Orderbook, ob),
        ],
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let prepared = match prepare(&req, &w.params).unwrap() {
        Ok(p) => p,
        Err(r) => panic!("duplicated live file rejected: {:?}", r.row_errors),
    };
    let span = prepared.report.span.unwrap();
    assert_eq!(span.start_ms, t_min);
    assert_eq!(span.end_ms, t_max);
    assert_eq!(
        prepared.report.files.len(),
        2,
        "the all-duplicate file must not produce a summary"
    );
    assert_eq!(prepared.trades, w.replay_trades);
}

#[test]
fn full_golden_signals_trace_reproduces_frozen_replay_artifacts() {
    let Some(inputs) = golden_pin::golden_inputs("full signals parity gate") else {
        return;
    };
    let fills_dir = inputs.fills_dir;
    let Some(frozen_replay) = frozen_replay_dir() else {
        eprintln!(
            "TRACER_REPLAY_DIR unset and out/replay absent; full signals parity gate skipped"
        );
        return;
    };
    assert_frozen_replay_pinned(&frozen_replay);
    let params = load_params();
    let dir = temp_dir("full_signals");
    let mut tape_files: Vec<(TapeRole, PathBuf)> = Vec::new();
    for sym in &params.symbols {
        let coin = sym.stem.replacen('_', ":", 1);
        let out = dir.join(format!("{}_trades.parquet", sym.stem));
        fills_to_trades_parquet(
            &Path::new(&fills_dir).join(format!("{}.csv", sym.stem)),
            &coin,
            &out,
        );
        tape_files.push((TapeRole::Trades, out));
    }
    let req = TraceRequest {
        tape_files,
        orders: OrderSourceSpec::Signals(inputs.golden_csv),
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let prepared = match prepare(&req, &params).unwrap() {
        Ok(p) => p,
        Err(r) => panic!(
            "golden signals rejected: {} errors, first {:?}",
            r.row_errors.len(),
            r.row_errors.first()
        ),
    };
    assert_eq!(prepared.trades.len(), 14_433);
    let trace_out = dir.join("trace");
    execute(
        &prepared,
        DEFAULT_GRACE_MS,
        DEFAULT_CROSS_WINDOW_MS,
        &trace_out,
    )
    .unwrap();
    for name in ["corrected_ledger.json", "fillrates.json"] {
        assert_eq!(
            artifact_norm(&trace_out.join(name)),
            artifact_norm(&frozen_replay.join(name)),
            "{name} diverged from frozen replay"
        );
    }

    let data = load_symbol_data(Path::new(&fills_dir), &params).unwrap();
    let replay_trades = tracer_engine::replay::paper_trades_from(&data, &params).unwrap();
    let verdicts = evaluate(
        &replay_trades,
        &data,
        &params,
        DEFAULT_GRACE_MS,
        DEFAULT_CROSS_WINDOW_MS,
    )
    .unwrap();
    let replay_ledger = dir.join("replay_ledger");
    let mut writer = LedgerWriter::new(&replay_ledger).unwrap();
    emit_events(&verdicts, &params, DEFAULT_GRACE_MS, &mut writer).unwrap();
    assert_eq!(
        events_norm(&trace_out.join("ledger")),
        events_norm(&replay_ledger)
    );
}

#[test]
fn full_strategy_trace_reproduces_14433_paper_trades() {
    let Ok(fills_dir) = std::env::var("TRACER_FILLS_DIR") else {
        eprintln!("TRACER_FILLS_DIR unset; full strategy parity gate skipped");
        return;
    };
    let params = load_params();
    let dir = temp_dir("full_strategy");
    let mut tape_files: Vec<(TapeRole, PathBuf)> = Vec::new();
    for sym in &params.symbols {
        let coin = sym.stem.replacen('_', ":", 1);
        let out = dir.join(format!("{}_trades.parquet", sym.stem));
        fills_to_trades_parquet(
            &Path::new(&fills_dir).join(format!("{}.csv", sym.stem)),
            &coin,
            &out,
        );
        tape_files.push((TapeRole::Trades, out));
    }
    let req = TraceRequest {
        tape_files,
        orders: OrderSourceSpec::Strategy,
        overrides: None,
        grace_ms: DEFAULT_GRACE_MS,
        cross_window_ms: DEFAULT_CROSS_WINDOW_MS,
        gap_spans: Vec::new(),
    };
    let prepared = prepare(&req, &params).unwrap().unwrap();
    let golden = tracer_engine::replay::paper_trades(Path::new(&fills_dir), &params).unwrap();
    assert_eq!(prepared.trades.len(), 14_433);
    assert_eq!(prepared.trades, golden);
}

#[test]
fn books_tape_reads_event_time_not_grid_ticks() {
    let dir = temp_dir("event_book_time");
    let pair = TradingPair::from_str("xyz:TSLA/USDC").unwrap();
    let venue_ts_us = 1_791_321_096_929_000;
    let snaps: Vec<Orderbook> = [
        1_791_321_097_000_000u64,
        1_791_321_097_100_000,
        1_791_321_097_200_000,
    ]
    .iter()
    .map(|grid_ts_us| Orderbook {
        orderbook_ts_us: *grid_ts_us,
        ..ob_snapshot(venue_ts_us, &pair, 100.0, 100.1)
    })
    .collect();
    let path = write_ob_parquet(&snaps, &dir, "sync").unwrap();

    let (ticks, _) = tracer_engine::trace::load_books_files(&[path.as_path()]).unwrap();

    let stamps: Vec<u64> = ticks.iter().map(|t| t.ts_ms).collect();
    assert_eq!(stamps, vec![venue_ts_us / 1_000]);
}
