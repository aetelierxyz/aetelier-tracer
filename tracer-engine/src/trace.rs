use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rust_decimal::prelude::ToPrimitive;
use sha2::Digest;

use aetelier_io::orderbooks::ob_parquet::read_ob_parquet;
use aetelier_io::trades::trades_parquet::read_trades_parquet;
use aetelier_types::orderbooks::{OrderbookTarget, OrderbookTargetData};
use aetelier_types::trades::TradeSide;

use crate::bars::{FillRow, auto_threshold, build_bars, quantize_bars};
use crate::corrected::{emit_events_stamped, evaluate, write_artifacts_stamped};
use crate::ledger::{LedgerWriter, Mode};
use crate::market_engine::{GapSpan, datatype_dir, gap_overlaps};
use crate::nyse::regime;
use crate::paper::{ExecutedTrade, run_symbol};
use crate::params::{SymbolParams, TracerParams};
use crate::replay::{MIN_BARS_PER_SYMBOL, SymbolData, paper_trades_from};

pub const SIGNALS_HEADER: &str = "symbol,side,score,decile,t_entry_ms,entry_px,t_exit_ms,t_exit_real_ms,exit_px,fee_entry_usd,fee_exit_usd,pnl_usd,regime";
pub const PREFLIGHT_SCHEMA: &str = "tracer.preflight.v1";
pub const MAX_SIGNAL_ROWS: usize = 50_000;
pub const MAX_SIGNALS_BYTES: u64 = 10 * 1024 * 1024;
pub const MAX_TAPE_BYTES: u64 = 200 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RowError {
    pub row: usize,
    pub field: String,
    pub reason: String,
}

fn file_error(name: &str, reason: String) -> RowError {
    RowError {
        row: 0,
        field: format!("file:{name}"),
        reason,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SharedSpan {
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TapeRole {
    Orderbook,
    Trades,
}

impl TapeRole {
    pub fn label(&self) -> &'static str {
        match self {
            TapeRole::Orderbook => "orderbook",
            TapeRole::Trades => "trades",
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TapeFileSummary {
    pub role: TapeRole,
    pub file: String,
    pub symbol_stem: String,
    pub events: usize,
    pub min_ts_ms: u64,
    pub max_ts_ms: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct BookTick {
    pub ts_ms: u64,
    pub bid_px: Option<f64>,
    pub ask_px: Option<f64>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SchemaFieldOut {
    pub name: String,
    pub dtype: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PreflightReport {
    pub schema: &'static str,
    pub span: Option<SharedSpan>,
    pub files: Vec<TapeFileSummary>,
    pub signal_rows: usize,
    pub row_errors: Vec<RowError>,
}

impl PreflightReport {
    pub fn clean(&self) -> bool {
        self.row_errors.is_empty()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind", content = "path")]
pub enum OrderSourceSpec {
    Strategy,
    Signals(PathBuf),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TraceOverrides {
    pub notional_usd: Option<f64>,
    pub hold_ms: Option<u64>,
    pub maker_fee_leg_bps: Option<f64>,
    pub taker_fee_leg_bps: Option<f64>,
}

pub struct TraceRequest {
    pub tape_files: Vec<(TapeRole, PathBuf)>,
    pub orders: OrderSourceSpec,
    pub overrides: Option<TraceOverrides>,
    pub grace_ms: u64,
    pub cross_window_ms: u64,
    pub gap_spans: Vec<GapSpan>,
}

pub struct TracePrepared {
    pub report: PreflightReport,
    pub data: Vec<SymbolData>,
    pub books: Vec<BookTick>,
    pub trades: Vec<ExecutedTrade>,
    pub params: TracerParams,
    pub trace_id: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub n_signals: usize,
    pub entry_through: usize,
    pub corrected_kept: usize,
    pub events_written: u64,
}

pub fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let bytes = std::fs::read(path)?;
    let mut h = sha2::Sha256::new();
    h.update(&bytes);
    Ok(format!("{:x}", h.finalize()))
}

pub fn parquet_schema_fields(path: &Path) -> anyhow::Result<Vec<SchemaFieldOut>> {
    let file = std::fs::File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder
        .schema()
        .fields()
        .iter()
        .map(|f| SchemaFieldOut {
            name: f.name().clone(),
            dtype: format!("{}", f.data_type()),
        })
        .collect())
}

const TRADES_SCHEMA_PREFIX: [(&str, &str); 7] = [
    ("source_trade_ts_us", "UInt64"),
    ("symbol", "Utf8"),
    ("side", "Utf8"),
    ("price", "Float64"),
    ("amount", "Float64"),
    ("exchange", "Utf8"),
    ("id", "Utf8"),
];

const OB_SCHEMA_PREFIX: [(&str, &str); 7] = [
    ("orderbook_ts_us", "UInt64"),
    ("symbol", "Utf8"),
    ("exchange", "Utf8"),
    ("side", "Utf8"),
    ("level", "UInt64"),
    ("price", "Float64"),
    ("size", "Float64"),
];

fn schema_matches(fields: &[SchemaFieldOut], want: &[(&str, &str)]) -> bool {
    fields.len() >= want.len()
        && want
            .iter()
            .zip(fields)
            .all(|((name, dtype), f)| f.name == *name && f.dtype == *dtype)
}

pub fn expect_schema(path: &Path, role: TapeRole) -> anyhow::Result<()> {
    let fields = parquet_schema_fields(path)?;
    let want: &[(&str, &str)] = match role {
        TapeRole::Trades => &TRADES_SCHEMA_PREFIX,
        TapeRole::Orderbook => &OB_SCHEMA_PREFIX,
    };
    anyhow::ensure!(
        schema_matches(&fields, want),
        "{}: not a collector {} parquet (schema {})",
        path.display(),
        role.label(),
        fields
            .iter()
            .map(|f| format!("{}:{}", f.name, f.dtype))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

pub fn sniff_role(path: &Path) -> anyhow::Result<TapeRole> {
    let fields = parquet_schema_fields(path)?;
    if schema_matches(&fields, &TRADES_SCHEMA_PREFIX) {
        return Ok(TapeRole::Trades);
    }
    if schema_matches(&fields, &OB_SCHEMA_PREFIX) {
        return Ok(TapeRole::Orderbook);
    }
    anyhow::bail!(
        "{}: unrecognized parquet schema (want collector trades or orderbook snapshot)",
        path.display()
    )
}

fn stem_of_symbol(symbol: &str) -> String {
    symbol.split('/').next().unwrap_or(symbol).replace(':', "_")
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

fn check_size(path: &Path, errors: &mut Vec<RowError>) -> bool {
    match std::fs::metadata(path) {
        Ok(m) if m.len() > MAX_TAPE_BYTES => {
            errors.push(file_error(
                &file_name(path),
                format!("{} bytes exceeds tape cap {MAX_TAPE_BYTES}", m.len()),
            ));
            false
        }
        Ok(_) => true,
        Err(e) => {
            errors.push(file_error(&file_name(path), format!("unreadable: {e}")));
            false
        }
    }
}

pub type PrintsByStem = BTreeMap<String, Vec<FillRow>>;

pub fn load_trades_files(paths: &[&Path]) -> anyhow::Result<(PrintsByStem, Vec<TapeFileSummary>)> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut merged: Vec<(u64, String, f64, f64, bool, String)> = Vec::new();
    let mut summaries: Vec<TapeFileSummary> = Vec::new();
    for path in paths {
        expect_schema(path, TapeRole::Trades)?;
        let trades =
            read_trades_parquet(path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        anyhow::ensure!(!trades.is_empty(), "{}: no trades", path.display());
        let mut kept = 0usize;
        let mut min_ts = u64::MAX;
        let mut max_ts = 0u64;
        let mut stems: BTreeSet<String> = BTreeSet::new();
        for t in trades {
            if !seen.insert(t.id.clone()) {
                continue;
            }
            let (Some(px), Some(sz)) = (t.price.to_f64(), t.amount.to_f64()) else {
                continue;
            };
            let ts_ms = t.source_trade_ts_us / 1_000;
            let stem = stem_of_symbol(&t.pair.to_canonical());
            kept += 1;
            min_ts = min_ts.min(ts_ms);
            max_ts = max_ts.max(ts_ms);
            stems.insert(stem.clone());
            merged.push((ts_ms, t.id, px, sz, matches!(t.side, TradeSide::Buy), stem));
        }
        anyhow::ensure!(kept > 0, "{}: no new priced trades", path.display());
        summaries.push(TapeFileSummary {
            role: TapeRole::Trades,
            file: file_name(path),
            symbol_stem: stems.into_iter().collect::<Vec<_>>().join(","),
            events: kept,
            min_ts_ms: min_ts,
            max_ts_ms: max_ts,
            sha256: file_sha256(path)?,
        });
    }
    merged.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    let mut by_stem: BTreeMap<String, Vec<FillRow>> = BTreeMap::new();
    for (ts_ms, _, px, sz, is_buy, stem) in merged {
        by_stem.entry(stem).or_default().push(FillRow {
            time_ms: ts_ms,
            px,
            sz,
            is_buy,
        });
    }
    Ok((by_stem, summaries))
}

pub struct LoadedBooks {
    pub ticks: Vec<BookTick>,
    pub summary: TapeFileSummary,
}

pub fn load_tape_books(path: &Path) -> anyhow::Result<LoadedBooks> {
    expect_schema(path, TapeRole::Orderbook)?;
    let data = read_ob_parquet(path, OrderbookTarget::Snapshot)
        .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    let OrderbookTargetData::Snapshot(books) = data else {
        anyhow::bail!("{}: not an orderbook snapshot parquet", path.display());
    };
    anyhow::ensure!(!books.is_empty(), "{}: no snapshots", path.display());
    let mut stems: BTreeSet<String> = BTreeSet::new();
    let mut ticks: Vec<BookTick> = Vec::with_capacity(books.len());
    for ob in &books {
        stems.insert(stem_of_symbol(&ob.pair.to_canonical()));
        ticks.push(BookTick {
            ts_ms: ob.orderbook_ts_us / 1_000,
            bid_px: ob.bids.last_key_value().and_then(|(px, _)| px.to_f64()),
            ask_px: ob.asks.first_key_value().and_then(|(px, _)| px.to_f64()),
        });
    }
    ticks.sort_by_key(|t| t.ts_ms);
    ticks.dedup_by_key(|t| t.ts_ms);
    anyhow::ensure!(
        stems.len() == 1,
        "{}: {} symbols in one tape file, want exactly one",
        path.display(),
        stems.len()
    );
    let min_ts = ticks.first().map(|t| t.ts_ms).unwrap_or(0);
    let max_ts = ticks.last().map(|t| t.ts_ms).unwrap_or(0);
    Ok(LoadedBooks {
        summary: TapeFileSummary {
            role: TapeRole::Orderbook,
            file: file_name(path),
            symbol_stem: stems.into_iter().next().unwrap(),
            events: ticks.len(),
            min_ts_ms: min_ts,
            max_ts_ms: max_ts,
            sha256: file_sha256(path)?,
        },
        ticks,
    })
}

fn span_intersection(files: &[TapeFileSummary]) -> Option<SharedSpan> {
    let start = files.iter().map(|f| f.min_ts_ms).max()?;
    let end = files.iter().map(|f| f.max_ts_ms).min()?;
    (start < end).then_some(SharedSpan {
        start_ms: start,
        end_ms: end,
    })
}

fn last_px_at_or_before(prints: &[FillRow], ts_ms: u64) -> Option<f64> {
    let idx = prints.partition_point(|r| r.time_ms <= ts_ms);
    (idx > 0).then(|| prints[idx - 1].px)
}

pub struct SymbolCoverage {
    pub span: SharedSpan,
}

fn coverage_by_stem(
    prints_by_stem: &BTreeMap<String, Vec<FillRow>>,
    ob_span: Option<(u64, u64)>,
) -> BTreeMap<String, SymbolCoverage> {
    prints_by_stem
        .iter()
        .filter_map(|(stem, prints)| {
            let min_ts = prints.first()?.time_ms;
            let max_ts = prints.last()?.time_ms;
            let start = ob_span.map_or(min_ts, |(s, _)| s.max(min_ts));
            let end = ob_span.map_or(max_ts, |(_, e)| e.min(max_ts));
            Some((
                stem.clone(),
                SymbolCoverage {
                    span: SharedSpan {
                        start_ms: start,
                        end_ms: end,
                    },
                },
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn parse_signals_strict(
    text: &str,
    coverage: &BTreeMap<String, SymbolCoverage>,
    prints_by_stem: &BTreeMap<String, Vec<FillRow>>,
    bar_last_px: &BTreeMap<(String, u64), f64>,
    hold_ms: u64,
    gap_spans: &[GapSpan],
) -> (Vec<ExecutedTrade>, usize, Vec<RowError>) {
    let mut errors: Vec<RowError> = Vec::new();
    let mut out: Vec<ExecutedTrade> = Vec::new();
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != SIGNALS_HEADER {
        errors.push(RowError {
            row: 0,
            field: "header".into(),
            reason: format!("want {SIGNALS_HEADER:?}, got {header:?}"),
        });
        return (out, 0, errors);
    }
    let mut seen_ids: BTreeSet<(String, u64, i8)> = BTreeSet::new();
    let mut n_rows = 0usize;
    for (i, line) in lines.enumerate() {
        let row = i + 1;
        if line.is_empty() {
            continue;
        }
        n_rows += 1;
        if n_rows > MAX_SIGNAL_ROWS {
            errors.push(RowError {
                row,
                field: "".into(),
                reason: format!("row cap {MAX_SIGNAL_ROWS} exceeded"),
            });
            break;
        }
        let c: Vec<&str> = line.split(',').collect();
        if c.len() != 13 {
            errors.push(RowError {
                row,
                field: "".into(),
                reason: format!("expected 13 columns, got {}", c.len()),
            });
            continue;
        }
        let mut row_errors: Vec<RowError> = Vec::new();
        let mut err = |field: &str, reason: String| {
            row_errors.push(RowError {
                row,
                field: field.into(),
                reason,
            });
        };
        let symbol = c[0].to_string();
        let cov = coverage.get(&symbol);
        if cov.is_none() {
            err(
                "symbol",
                format!(
                    "not covered by tape (have: {})",
                    coverage.keys().cloned().collect::<Vec<_>>().join(", ")
                ),
            );
        }
        let side = match c[1].parse::<i8>() {
            Ok(s @ (1 | -1)) => Some(s),
            Ok(other) => {
                err("side", format!("want 1 or -1, got {other}"));
                None
            }
            Err(_) => {
                err("side", format!("not an integer: {:?}", c[1]));
                None
            }
        };
        let parse_f64 =
            |field: &str, raw: &str, err: &mut dyn FnMut(&str, String)| -> Option<f64> {
                match raw.parse::<f64>() {
                    Ok(v) if v.is_finite() => Some(v),
                    Ok(v) => {
                        err(field, format!("not finite: {v}"));
                        None
                    }
                    Err(_) => {
                        err(field, format!("not a number: {raw:?}"));
                        None
                    }
                }
            };
        let parse_u64 =
            |field: &str, raw: &str, err: &mut dyn FnMut(&str, String)| -> Option<u64> {
                match raw.parse::<u64>() {
                    Ok(v) => Some(v),
                    Err(_) => {
                        err(field, format!("not a timestamp: {raw:?}"));
                        None
                    }
                }
            };
        let score = parse_f64("score", c[2], &mut err);
        let decile = match c[3].parse::<usize>() {
            Ok(d @ 1..=10) => Some(d),
            Ok(other) => {
                err("decile", format!("want 1..=10, got {other}"));
                None
            }
            Err(_) => {
                err("decile", format!("not an integer: {:?}", c[3]));
                None
            }
        };
        let t_entry_ms = parse_u64("t_entry_ms", c[4], &mut err);
        let entry_px = parse_f64("entry_px", c[5], &mut err).filter(|v| {
            if *v > 0.0 {
                true
            } else {
                err("entry_px", format!("want > 0, got {v}"));
                false
            }
        });
        let t_exit_ms = parse_u64("t_exit_ms", c[6], &mut err);
        let t_exit_real_ms = parse_u64("t_exit_real_ms", c[7], &mut err);
        let exit_px = parse_f64("exit_px", c[8], &mut err).filter(|v| {
            if *v > 0.0 {
                true
            } else {
                err("exit_px", format!("want > 0, got {v}"));
                false
            }
        });
        let fee_entry_usd = parse_f64("fee_entry_usd", c[9], &mut err);
        let fee_exit_usd = parse_f64("fee_exit_usd", c[10], &mut err);
        let pnl_usd = parse_f64("pnl_usd", c[11], &mut err);
        let regime_given = c[12];

        if let Some(t0) = t_entry_ms {
            let computed = regime(t0).label();
            if regime_given != computed {
                err(
                    "regime",
                    format!("given {regime_given}, computed {computed}"),
                );
            }
            if let Some(tx) = t_exit_ms
                && tx != t0 + hold_ms
            {
                err(
                    "t_exit_ms",
                    format!("must equal t_entry_ms + hold_ms ({hold_ms}), got {tx}"),
                );
            }
            if let Some(txr) = t_exit_real_ms {
                if txr < t0 {
                    err(
                        "t_exit_real_ms",
                        format!("before t_entry_ms ({txr} < {t0})"),
                    );
                } else if let Some(tx) = t_exit_ms
                    && txr > tx
                {
                    err(
                        "t_exit_real_ms",
                        format!("after nominal exit ({txr} > {tx})"),
                    );
                }
            }
            if let Some(cov) = cov {
                let s = cov.span;
                if t0 < s.start_ms || t0 > s.end_ms {
                    err(
                        "t_entry_ms",
                        format!("outside covered span [{}, {}]", s.start_ms, s.end_ms),
                    );
                }
                if let Some(txr) = t_exit_real_ms
                    && (txr < s.start_ms || txr > s.end_ms)
                {
                    err(
                        "t_exit_real_ms",
                        format!("outside covered span [{}, {}]", s.start_ms, s.end_ms),
                    );
                }
            }
            if !gap_spans.is_empty() && gap_overlaps(gap_spans, t0, t0 + hold_ms) {
                err(
                    "t_entry_ms",
                    "entry window straddles a recorded collector gap".into(),
                );
            }
            if let Some(sd) = side
                && !seen_ids.insert((symbol.clone(), t0, sd))
            {
                err("", format!("duplicate trade identity {symbol}|{t0}|{sd}"));
            }
        }

        if row_errors.is_empty() {
            let (t0, sd) = (t_entry_ms.unwrap(), side.unwrap());
            let entry_px = entry_px.unwrap();
            let entry_last_px = bar_last_px
                .get(&(symbol.clone(), t0))
                .copied()
                .or_else(|| {
                    prints_by_stem
                        .get(&symbol)
                        .and_then(|p| last_px_at_or_before(p, t0))
                })
                .unwrap_or(entry_px);
            out.push(ExecutedTrade {
                symbol,
                side: sd,
                score: score.unwrap(),
                decile: decile.unwrap(),
                t_entry_ms: t0,
                entry_px,
                entry_last_px,
                t_exit_ms: t_exit_ms.unwrap(),
                t_exit_real_ms: t_exit_real_ms.unwrap(),
                exit_px: exit_px.unwrap(),
                fee_entry_usd: fee_entry_usd.unwrap(),
                fee_exit_usd: fee_exit_usd.unwrap(),
                pnl_usd: pnl_usd.unwrap(),
            });
        } else {
            errors.append(&mut row_errors);
        }
    }
    out.sort_by(|a, b| {
        (a.t_exit_ms, &a.symbol, a.t_entry_ms, a.side).cmp(&(
            b.t_exit_ms,
            &b.symbol,
            b.t_entry_ms,
            b.side,
        ))
    });
    (out, n_rows, errors)
}

pub fn effective_params(
    base: &TracerParams,
    overrides: Option<&TraceOverrides>,
    prints_by_stem: &BTreeMap<String, Vec<FillRow>>,
    input_sha256: BTreeMap<String, String>,
) -> anyhow::Result<TracerParams> {
    let mut p = base.clone();
    if let Some(ov) = overrides {
        if let Some(n) = ov.notional_usd {
            anyhow::ensure!(n > 0.0 && n.is_finite(), "notional_usd must be > 0");
            p.notional_usd = n;
        }
        if let Some(h) = ov.hold_ms {
            anyhow::ensure!(h > 0, "hold_ms must be > 0");
            p.hold_ms = h;
        }
    }
    for (stem, prints) in prints_by_stem {
        match p.symbols.iter_mut().find(|s| &s.stem == stem) {
            Some(sym) => {
                if let Some(ov) = overrides {
                    if let Some(m) = ov.maker_fee_leg_bps {
                        sym.maker_fee_leg_bps = m;
                    }
                    if let Some(t) = ov.taker_fee_leg_bps {
                        sym.taker_fee_leg_bps = t;
                    }
                }
            }
            None => {
                let ov = overrides.ok_or_else(|| {
                    anyhow::anyhow!(
                        "symbol {stem} absent from params artifact and no fee overrides given"
                    )
                })?;
                let (maker, taker) = match (ov.maker_fee_leg_bps, ov.taker_fee_leg_bps) {
                    (Some(m), Some(t)) => (m, t),
                    _ => {
                        anyhow::bail!("symbol {stem} needs maker_fee_leg_bps and taker_fee_leg_bps")
                    }
                };
                p.symbols.push(SymbolParams {
                    stem: stem.clone(),
                    bar_threshold_usd: auto_threshold(prints, p.tau_secs).0,
                    maker_fee_leg_bps: maker,
                    taker_fee_leg_bps: taker,
                });
            }
        }
    }
    p.input_sha256 = input_sha256;
    Ok(p)
}

pub fn trace_id_for(
    params: &TracerParams,
    orders: &OrderSourceSpec,
    grace_ms: u64,
    cross_window_ms: u64,
) -> String {
    let mut h = sha2::Sha256::new();
    for (k, v) in &params.input_sha256 {
        h.update(k.as_bytes());
        h.update(v.as_bytes());
    }
    h.update(serde_json::to_string(orders).unwrap_or_default().as_bytes());
    h.update(params.hold_ms.to_le_bytes());
    h.update(params.notional_usd.to_le_bytes());
    h.update(grace_ms.to_le_bytes());
    h.update(cross_window_ms.to_le_bytes());
    let hex = format!("{:x}", h.finalize());
    format!("tr_{}", &hex[..12])
}

pub fn prepare(
    req: &TraceRequest,
    base_params: &TracerParams,
) -> anyhow::Result<Result<TracePrepared, PreflightReport>> {
    let mut errors: Vec<RowError> = Vec::new();
    let mut files: Vec<TapeFileSummary> = Vec::new();
    let mut prints_by_stem: BTreeMap<String, Vec<FillRow>> = BTreeMap::new();
    let mut books: Vec<BookTick> = Vec::new();
    let mut input_sha256: BTreeMap<String, String> = BTreeMap::new();

    let mut trades_paths: Vec<&Path> = Vec::new();
    let mut ob_paths: Vec<&Path> = Vec::new();
    for (role, path) in &req.tape_files {
        if !check_size(path, &mut errors) {
            continue;
        }
        match role {
            TapeRole::Trades => trades_paths.push(path),
            TapeRole::Orderbook => ob_paths.push(path),
        }
    }
    anyhow::ensure!(ob_paths.len() <= 1, "more than one orderbook tape file");
    match load_trades_files(&trades_paths) {
        Ok((by_stem, summaries)) => {
            for s in &summaries {
                input_sha256.insert(format!("trades/{}", s.file), s.sha256.clone());
            }
            prints_by_stem = by_stem;
            files.extend(summaries);
        }
        Err(e) => errors.push(file_error("trades", format!("{e:#}"))),
    }
    if let Some(path) = ob_paths.first() {
        match load_tape_books(path) {
            Ok(loaded) => {
                input_sha256.insert(
                    format!("orderbook/{}", loaded.summary.file),
                    loaded.summary.sha256.clone(),
                );
                books = loaded.ticks;
                files.push(loaded.summary);
            }
            Err(e) => errors.push(file_error(&file_name(path), format!("{e:#}"))),
        }
    }

    if prints_by_stem.is_empty() && errors.is_empty() {
        errors.push(RowError {
            row: 0,
            field: "tape".into(),
            reason: "no trades tape provided".into(),
        });
    }
    let ob_file = files
        .iter()
        .find(|f| f.role == TapeRole::Orderbook)
        .cloned();
    if let Some(ob) = &ob_file {
        for stem in prints_by_stem.keys() {
            if &ob.symbol_stem != stem {
                errors.push(RowError {
                    row: 0,
                    field: "tape".into(),
                    reason: format!(
                        "symbol mismatch: orderbook {} vs trades {stem}",
                        ob.symbol_stem
                    ),
                });
            }
        }
    }
    let span = span_intersection(&files);
    if span.is_none() && !files.is_empty() {
        errors.push(RowError {
            row: 0,
            field: "tape".into(),
            reason: "shared span is empty".into(),
        });
    }

    let mut report = PreflightReport {
        schema: PREFLIGHT_SCHEMA,
        span,
        files: files.clone(),
        signal_rows: 0,
        row_errors: errors,
    };
    if !report.clean() {
        return Ok(Err(report));
    }

    let ob_span = ob_file.as_ref().map(|f| (f.min_ts_ms, f.max_ts_ms));
    let coverage = coverage_by_stem(&prints_by_stem, ob_span);

    let into_data = |map: BTreeMap<String, Vec<FillRow>>| -> Vec<SymbolData> {
        map.into_iter()
            .map(|(stem, prints)| SymbolData {
                stem,
                prints,
                bars: Vec::new(),
            })
            .collect()
    };

    let (trades, params, data) = match &req.orders {
        OrderSourceSpec::Signals(path) => {
            match std::fs::metadata(path) {
                Ok(m) if m.len() > MAX_SIGNALS_BYTES => {
                    report.row_errors.push(file_error(
                        &file_name(path),
                        format!("{} bytes exceeds signals cap {MAX_SIGNALS_BYTES}", m.len()),
                    ));
                    return Ok(Err(report));
                }
                Ok(_) => {}
                Err(e) => {
                    report
                        .row_errors
                        .push(file_error(&file_name(path), format!("unreadable: {e}")));
                    return Ok(Err(report));
                }
            }
            input_sha256.insert(format!("signals/{}", file_name(path)), file_sha256(path)?);
            let params = effective_params(
                base_params,
                req.overrides.as_ref(),
                &prints_by_stem,
                input_sha256,
            )?;
            let mut bar_last_px: BTreeMap<(String, u64), f64> = BTreeMap::new();
            for (stem, prints) in &prints_by_stem {
                let sym = params.symbol(stem)?;
                let mut bars = build_bars(prints, sym.bar_threshold_usd);
                quantize_bars(&mut bars);
                for t in run_symbol(
                    stem,
                    &bars,
                    &params.decile_thresholds,
                    params.tau_secs,
                    params.hold_ms,
                    params.exit_staleness_ms,
                    params.notional_usd,
                    sym.maker_fee_leg_bps,
                ) {
                    bar_last_px.insert((stem.clone(), t.t_entry_ms), t.entry_last_px);
                }
            }
            let text = std::fs::read_to_string(path)?;
            let (trades, n_rows, mut errs) = parse_signals_strict(
                &text,
                &coverage,
                &prints_by_stem,
                &bar_last_px,
                params.hold_ms,
                &req.gap_spans,
            );
            report.signal_rows = n_rows;
            if !errs.is_empty() {
                report.row_errors.append(&mut errs);
                return Ok(Err(report));
            }
            if trades.is_empty() {
                report.row_errors.push(RowError {
                    row: 0,
                    field: "".into(),
                    reason: "no signal rows".into(),
                });
                return Ok(Err(report));
            }
            (trades, params, into_data(prints_by_stem))
        }
        OrderSourceSpec::Strategy => {
            let params = effective_params(
                base_params,
                req.overrides.as_ref(),
                &prints_by_stem,
                input_sha256,
            )?;
            let mut data = into_data(prints_by_stem);
            for d in &mut data {
                let sym = params.symbol(&d.stem)?;
                let mut bars = build_bars(&d.prints, sym.bar_threshold_usd);
                quantize_bars(&mut bars);
                if bars.len() < MIN_BARS_PER_SYMBOL {
                    report.row_errors.push(RowError {
                        row: 0,
                        field: "tape".into(),
                        reason: format!(
                            "{}: {} bars under strategy minimum {MIN_BARS_PER_SYMBOL}",
                            d.stem,
                            bars.len()
                        ),
                    });
                }
                d.bars = bars;
            }
            if !report.clean() {
                return Ok(Err(report));
            }
            let trades = paper_trades_from(&data, &params)?;
            if trades.is_empty() {
                report.row_errors.push(RowError {
                    row: 0,
                    field: "".into(),
                    reason: "strategy generated no orders over this tape".into(),
                });
                return Ok(Err(report));
            }
            report.signal_rows = trades.len();
            (trades, params, data)
        }
    };

    let trace_id = trace_id_for(&params, &req.orders, req.grace_ms, req.cross_window_ms);
    Ok(Ok(TracePrepared {
        report,
        data,
        books,
        trades,
        params,
        trace_id,
    }))
}

pub fn execute(
    prepared: &TracePrepared,
    grace_ms: u64,
    cross_window_ms: u64,
    out_dir: &Path,
) -> anyhow::Result<TraceSummary> {
    let verdicts = evaluate(
        &prepared.trades,
        &prepared.data,
        &prepared.params,
        grace_ms,
        cross_window_ms,
    )?;
    write_artifacts_stamped(
        &verdicts,
        &prepared.params,
        grace_ms,
        cross_window_ms,
        out_dir,
        "trace",
        Some(&prepared.trace_id),
    )?;
    let mut writer = LedgerWriter::new(&out_dir.join("ledger"))?;
    emit_events_stamped(
        &verdicts,
        &prepared.params,
        grace_ms,
        &mut writer,
        Mode::Trace,
        Some(&prepared.trace_id),
    )?;
    Ok(TraceSummary {
        trace_id: prepared.trace_id.clone(),
        n_signals: prepared.trades.len(),
        entry_through: verdicts
            .iter()
            .filter(|v| v.entry[0].through.is_some())
            .count(),
        corrected_kept: verdicts.iter().filter(|v| v.corrected.is_some()).count(),
        events_written: writer.events_written,
    })
}

pub fn load_live_tape_files(
    trades_binding: &Path,
    books_binding: Option<&Path>,
) -> anyhow::Result<Vec<(TapeRole, PathBuf)>> {
    let mut out = Vec::new();
    let trades_dir = datatype_dir(trades_binding, "trades")?
        .ok_or_else(|| anyhow::anyhow!("{}: no trades binding dir", trades_binding.display()))?;
    let mut names: Vec<PathBuf> = std::fs::read_dir(&trades_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    names.sort();
    anyhow::ensure!(
        !names.is_empty(),
        "{}: no trades parquet yet",
        trades_dir.display()
    );
    out.extend(names.into_iter().map(|p| (TapeRole::Trades, p)));
    if let Some(books) = books_binding
        && let Some(dir) = datatype_dir(books, "orderbooks")?
    {
        let mut names: Vec<PathBuf> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .collect();
        names.sort();
        out.extend(names.into_iter().map(|p| (TapeRole::Orderbook, p)));
    }
    Ok(out)
}

pub fn book_series(
    ticks: &[BookTick],
    t0: Option<u64>,
    t1: Option<u64>,
    max_points: usize,
) -> Vec<BookTick> {
    let lo = t0.unwrap_or(0);
    let hi = t1.unwrap_or(u64::MAX);
    let window: Vec<BookTick> = ticks
        .iter()
        .filter(|t| t.ts_ms >= lo && t.ts_ms <= hi)
        .copied()
        .collect();
    if max_points == 0 || window.len() <= max_points {
        return window;
    }
    let stride = window.len().div_ceil(max_points);
    let mut out: Vec<BookTick> = window.iter().step_by(stride).copied().collect();
    if let (Some(last), Some(kept)) = (window.last(), out.last())
        && kept.ts_ms != last.ts_ms
    {
        out.push(*last);
    }
    out
}
