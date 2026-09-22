use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};

use axum::extract::{Multipart, Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use sha2::Digest;

use tracer_engine::market_engine::read_gap_spans;
use tracer_engine::params::TracerParams;
use tracer_engine::trace::{
    BookTick, DEPTH_ROWS_CAP, DEPTH_ROWS_DEFAULT, DepthRow, MAX_SIGNALS_BYTES, MAX_TAPE_BYTES,
    OrderSourceSpec, PreflightReport, RowError, SchemaFieldOut, TapeRole, TraceOverrides,
    TraceRequest, TraceSummary, book_series, decimate_depth, execute, load_books_files,
    load_depth_rows, load_live_tape_files, load_trades_files, parquet_schema_fields, prepare,
};

use crate::api::{AppState, EventsQuery, paged_events};

pub const TRACE_SCHEMA: &str = "tracer.trace.v1";
pub const USER_HEADER: &str = "x-aetelier-user";
pub const SERVICE_HEADER: &str = "x-aetelier-service";
pub const SERVICE_TOKEN_ENV: &str = "TRACER_SERVICE_TOKEN";
pub const SERVICE_PRINCIPAL: &str = "svc:in-network";
pub const LIVE_ID: &str = "live";
pub const DEFAULT_MAX_TRACES_PER_USER: usize = 50;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TraceInputFile {
    pub role: String,
    pub file: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TraceRecord {
    pub schema: String,
    pub trace_id: String,
    pub created_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub order_source: String,
    pub tape_source: String,
    pub inputs: Vec<TraceInputFile>,
    pub grace_ms: u64,
    pub cross_window_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overrides: Option<TraceOverrides>,
    pub preflight: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<serde_json::Value>,
}

struct CachedTape {
    prints: Vec<(u64, f64, f64, bool)>,
    books: Vec<BookTick>,
}

pub struct TraceRuntime {
    pub dir: PathBuf,
    pub params_path: PathBuf,
    pub grace_ms: u64,
    pub cross_window_ms: u64,
    pub trades_binding: Option<PathBuf>,
    pub books_binding: Option<PathBuf>,
    pub symbol_stem: Option<String>,
    pub max_traces_per_user: usize,
    tx: mpsc::Sender<String>,
    cache: Mutex<Option<(String, Arc<CachedTape>)>>,
    deleted: Mutex<std::collections::HashSet<String>>,
}

impl TraceRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        dir: PathBuf,
        params_path: PathBuf,
        grace_ms: u64,
        cross_window_ms: u64,
        trades_binding: Option<PathBuf>,
        books_binding: Option<PathBuf>,
        symbol_stem: Option<String>,
        max_traces_per_user: usize,
    ) -> anyhow::Result<Arc<Self>> {
        std::fs::create_dir_all(&dir)?;
        let (tx, rx) = mpsc::channel::<String>();
        let rt = Arc::new(Self {
            dir,
            params_path,
            grace_ms,
            cross_window_ms,
            trades_binding,
            books_binding,
            symbol_stem,
            max_traces_per_user,
            tx,
            cache: Mutex::new(None),
            deleted: Mutex::new(std::collections::HashSet::new()),
        });
        for record in rt.list_records() {
            if record.status == "queued" || record.status == "running" {
                let _ = rt.tx.send(record.trace_id.clone());
            }
        }
        let worker = rt.clone();
        std::thread::spawn(move || {
            while let Ok(trace_id) = rx.recv() {
                if let Err(e) = worker.run_one(&trace_id) {
                    eprintln!("trace {trace_id}: {e:#}");
                    let _ = worker.update_record(&trace_id, |r| {
                        r.status = "failed".into();
                        r.error = Some(format!("{e:#}"));
                    });
                }
            }
        });
        Ok(rt)
    }

    fn trace_dir(&self, trace_id: &str) -> PathBuf {
        self.dir.join(trace_id)
    }

    fn record_path(&self, trace_id: &str) -> PathBuf {
        self.trace_dir(trace_id).join("trace.json")
    }

    pub fn read_record(&self, trace_id: &str) -> Option<TraceRecord> {
        let text = std::fs::read_to_string(self.record_path(trace_id)).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn write_record(&self, record: &TraceRecord) -> anyhow::Result<()> {
        let dir = self.trace_dir(&record.trace_id);
        std::fs::create_dir_all(&dir)?;
        let tmp = dir.join("trace.json.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(record)? + "\n")?;
        std::fs::rename(&tmp, self.record_path(&record.trace_id))?;
        Ok(())
    }

    fn update_record(
        &self,
        trace_id: &str,
        f: impl FnOnce(&mut TraceRecord),
    ) -> anyhow::Result<()> {
        let mut record = self
            .read_record(trace_id)
            .ok_or_else(|| anyhow::anyhow!("{trace_id}: record missing"))?;
        f(&mut record);
        self.write_record(&record)
    }

    fn is_deleted(&self, trace_id: &str) -> bool {
        self.deleted.lock().unwrap().contains(trace_id)
    }

    fn drop_cached(&self, trace_id: &str) {
        let mut cache = self.cache.lock().unwrap();
        if cache.as_ref().is_some_and(|(id, _)| id == trace_id) {
            *cache = None;
        }
    }

    fn purge_dir(&self, trace_id: &str) {
        let dir = self.trace_dir(trace_id);
        if dir.exists() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    pub fn delete_trace(&self, trace_id: &str) -> Result<(), StatusCode> {
        if !is_trace_id(trace_id) {
            return Err(StatusCode::NOT_FOUND);
        }
        self.deleted.lock().unwrap().insert(trace_id.to_string());
        self.drop_cached(trace_id);
        self.purge_dir(trace_id);
        Ok(())
    }

    pub fn count_for(&self, owner: &str) -> usize {
        self.list_records()
            .iter()
            .filter(|r| r.user_id.as_deref() == Some(owner))
            .count()
    }

    pub fn list_records(&self) -> Vec<TraceRecord> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut out: Vec<TraceRecord> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| self.read_record(&e.file_name().to_string_lossy()))
            .collect();
        out.sort_by_key(|r| std::cmp::Reverse(r.created_ms));
        out
    }

    fn request_for(&self, record: &TraceRecord) -> anyhow::Result<TraceRequest> {
        let inputs_dir = self.trace_dir(&record.trace_id).join("inputs");
        let (tape_files, gap_spans) = if record.tape_source == "live" {
            let trades_root = self
                .trades_binding
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("live tape unavailable: no trades binding"))?;
            (
                load_live_tape_files(trades_root, self.books_binding.as_deref())?,
                read_gap_spans(trades_root, self.symbol_stem.as_deref())?,
            )
        } else {
            let files = record
                .inputs
                .iter()
                .filter(|f| f.role != "signals")
                .map(|f| {
                    let role = match f.role.as_str() {
                        "orderbook" => TapeRole::Orderbook,
                        _ => TapeRole::Trades,
                    };
                    (role, inputs_dir.join(&f.file))
                })
                .collect();
            (files, Vec::new())
        };
        let orders = if record.order_source == "strategy" {
            OrderSourceSpec::Strategy
        } else {
            let signals = record
                .inputs
                .iter()
                .find(|f| f.role == "signals")
                .ok_or_else(|| anyhow::anyhow!("signals input missing from record"))?;
            OrderSourceSpec::Signals(inputs_dir.join(&signals.file))
        };
        Ok(TraceRequest {
            tape_files,
            orders,
            overrides: record.overrides.clone(),
            grace_ms: record.grace_ms,
            cross_window_ms: record.cross_window_ms,
            gap_spans,
        })
    }

    fn run_one(&self, trace_id: &str) -> anyhow::Result<()> {
        if self.is_deleted(trace_id) {
            self.purge_dir(trace_id);
            return Ok(());
        }
        let record = self
            .read_record(trace_id)
            .ok_or_else(|| anyhow::anyhow!("{trace_id}: record missing"))?;
        if record.status == "complete" {
            return Ok(());
        }
        self.update_record(trace_id, |r| {
            r.status = "running".into();
            r.error = None;
        })?;
        let base_params = TracerParams::load(&self.params_path)?;
        let req = self.request_for(&record)?;
        let out_dir = self.trace_dir(trace_id);
        match prepare(&req, &base_params)? {
            Err(report) => self.update_record(trace_id, |r| {
                r.status = "failed".into();
                r.error = Some(format!(
                    "strict validation failed at run time: {} errors",
                    report.row_errors.len()
                ));
                r.preflight = serde_json::to_value(&report).unwrap_or_default();
            }),
            Ok(mut prepared) => {
                prepared.trace_id = trace_id.to_string();
                let effective = serde_json::json!({
                    "notional_usd": prepared.params.notional_usd,
                    "hold_ms": prepared.params.hold_ms,
                    "grace_ms": record.grace_ms,
                    "cross_window_ms": record.cross_window_ms,
                    "symbols": prepared.params.symbols.iter().map(|s| serde_json::json!({
                        "stem": s.stem,
                        "maker_fee_leg_bps": s.maker_fee_leg_bps,
                        "taker_fee_leg_bps": s.taker_fee_leg_bps,
                    })).collect::<Vec<_>>(),
                });
                let summary: TraceSummary =
                    execute(&prepared, record.grace_ms, record.cross_window_ms, &out_dir)?;
                if self.is_deleted(trace_id) {
                    self.purge_dir(trace_id);
                    return Ok(());
                }
                self.update_record(trace_id, |r| {
                    r.status = "complete".into();
                    r.summary = serde_json::to_value(&summary).ok();
                    r.effective = Some(effective);
                })
            }
        }
    }

    fn cached_tape(&self, record: &TraceRecord) -> anyhow::Result<Arc<CachedTape>> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some((id, tape)) = cache.as_ref()
                && id == &record.trace_id
            {
                return Ok(tape.clone());
            }
        }
        let req = self.request_for(record)?;
        let mut trades_paths: Vec<&Path> = Vec::new();
        let mut books_paths: Vec<&Path> = Vec::new();
        for (role, path) in &req.tape_files {
            match role {
                TapeRole::Trades => trades_paths.push(path),
                TapeRole::Orderbook => books_paths.push(path),
            }
        }
        let (by_stem, _) = load_trades_files(&trades_paths)?;
        let mut prints: Vec<(u64, f64, f64, bool)> = by_stem
            .into_values()
            .flatten()
            .map(|r| (r.time_ms, r.px, r.sz, r.is_buy))
            .collect();
        prints.sort_by_key(|p| p.0);
        let books = if books_paths.is_empty() {
            Vec::new()
        } else {
            load_books_files(&books_paths)?.0
        };
        let tape = Arc::new(CachedTape { prints, books });
        *self.cache.lock().unwrap() = Some((record.trace_id.clone(), tape.clone()));
        Ok(tape)
    }

    fn live_record(&self) -> Option<serde_json::Value> {
        let stem = self.symbol_stem.as_ref()?;
        Some(serde_json::json!({
            "schema": TRACE_SCHEMA,
            "trace_id": "live",
            "status": "live",
            "order_source": "strategy",
            "tape_source": "live",
            "symbol_stem": stem,
        }))
    }
}

fn now_from(state: &AppState) -> u64 {
    (state.now_ms)()
}

fn runtime(state: &AppState) -> Result<Arc<TraceRuntime>, StatusCode> {
    state.traces.clone().ok_or(StatusCode::NOT_FOUND)
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

fn secret_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn service_credential_matches(headers: &HeaderMap) -> bool {
    let Some(expected) = std::env::var(SERVICE_TOKEN_ENV)
        .ok()
        .filter(|s| !s.is_empty())
    else {
        return false;
    };
    header_value(headers, SERVICE_HEADER).is_some_and(|presented| secret_eq(&expected, &presented))
}

fn caller_of(headers: &HeaderMap) -> Result<String, StatusCode> {
    if service_credential_matches(headers) {
        return Ok(SERVICE_PRINCIPAL.to_string());
    }
    header_value(headers, USER_HEADER).ok_or(StatusCode::UNAUTHORIZED)
}

fn is_service(caller: &str) -> bool {
    caller == SERVICE_PRINCIPAL
}

fn is_trace_id(id: &str) -> bool {
    id.len() == 15
        && id.starts_with("tr_")
        && id
            .bytes()
            .skip(3)
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn scoped_trace_id(engine_id: &str, user: &str) -> String {
    let mut h = sha2::Sha256::new();
    h.update(engine_id.as_bytes());
    h.update(b"\n");
    h.update(user.as_bytes());
    let hex = format!("{:x}", h.finalize());
    format!("tr_{}", &hex[..12])
}

fn visible_to(record: &TraceRecord, caller: &str) -> Result<(), StatusCode> {
    if record.trace_id == LIVE_ID || is_service(caller) {
        return Ok(());
    }
    match &record.user_id {
        Some(owner) if owner == caller => Ok(()),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

fn read_owned(rt: &TraceRuntime, id: &str, headers: &HeaderMap) -> Result<TraceRecord, StatusCode> {
    let caller = caller_of(headers)?;
    let record = rt.read_record(id).ok_or(StatusCode::NOT_FOUND)?;
    visible_to(&record, &caller)?;
    Ok(record)
}

#[derive(serde::Deserialize)]
struct TraceConfigIn {
    order_source: String,
    tape: String,
    #[serde(default)]
    preflight_only: bool,
    notional_usd: Option<f64>,
    hold_ms: Option<u64>,
    maker_fee_leg_bps: Option<f64>,
    taker_fee_leg_bps: Option<f64>,
    grace_ms: Option<u64>,
    cross_window_ms: Option<u64>,
}

fn bad_request(reason: String) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": reason })),
    )
}

fn rejection(report: &PreflightReport) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(serde_json::to_value(report).unwrap_or_default()),
    )
}

struct StagedUpload {
    dir: PathBuf,
    files: Vec<(String, PathBuf)>,
}

impl Drop for StagedUpload {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn stage_upload(
    staging_root: &Path,
    stamp: u64,
    multipart: &mut Multipart,
) -> Result<(Option<TraceConfigIn>, StagedUpload), (StatusCode, Json<serde_json::Value>)> {
    let dir = staging_root.join(format!("staging_{stamp}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(|e| bad_request(format!("staging dir: {e}")))?;
    let mut staged = StagedUpload {
        dir: dir.clone(),
        files: Vec::new(),
    };
    let mut config: Option<TraceConfigIn> = None;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => return Err(bad_request(format!("multipart: {e}"))),
        };
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "config" => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("config part: {e}")))?;
                config = Some(
                    serde_json::from_str(&text)
                        .map_err(|e| bad_request(format!("config json: {e}")))?,
                );
            }
            "orderbook" | "trades" | "signals" => {
                let ext = if name == "signals" { "csv" } else { "parquet" };
                let filename = field
                    .file_name()
                    .map(|f| f.to_string())
                    .filter(|f| !f.is_empty() && !f.contains('/') && !f.contains(".."))
                    .unwrap_or_else(|| format!("{name}.{ext}"));
                let cap = if name == "signals" {
                    MAX_SIGNALS_BYTES
                } else {
                    MAX_TAPE_BYTES
                };
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| bad_request(format!("{name} part: {e}")))?;
                if bytes.len() as u64 > cap {
                    return Err(rejection(&PreflightReport {
                        schema: tracer_engine::trace::PREFLIGHT_SCHEMA,
                        span: None,
                        files: vec![],
                        signal_rows: 0,
                        row_errors: vec![RowError {
                            row: 0,
                            field: format!("file:{filename}"),
                            reason: format!("{} bytes exceeds cap {cap}", bytes.len()),
                        }],
                    }));
                }
                let path = dir.join(format!("{name}__{filename}"));
                std::fs::write(&path, &bytes)
                    .map_err(|e| bad_request(format!("{name} write: {e}")))?;
                staged.files.push((name, path));
            }
            other => return Err(bad_request(format!("unknown part {other:?}"))),
        }
    }
    Ok((config, staged))
}

pub async fn create_trace(
    State(s): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let rt = runtime(&s).map_err(|c| (c, Json(serde_json::json!({"error": "traces disabled"}))))?;
    let user = caller_of(&headers).map_err(|c| {
        (
            c,
            Json(serde_json::json!({"error": "caller identity required"})),
        )
    })?;
    if !is_service(&user) && rt.count_for(&user) >= rt.max_traces_per_user {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": "trace quota reached",
                "limit": rt.max_traces_per_user,
            })),
        ));
    }
    let now = now_from(&s);
    let (config, staged) = stage_upload(&rt.dir, now, &mut multipart).await?;
    let config = config.ok_or_else(|| bad_request("missing config part".into()))?;

    let overrides = (config.notional_usd.is_some()
        || config.hold_ms.is_some()
        || config.maker_fee_leg_bps.is_some()
        || config.taker_fee_leg_bps.is_some())
    .then_some(TraceOverrides {
        notional_usd: config.notional_usd,
        hold_ms: config.hold_ms,
        maker_fee_leg_bps: config.maker_fee_leg_bps,
        taker_fee_leg_bps: config.taker_fee_leg_bps,
    });
    let grace_ms = config.grace_ms.unwrap_or(rt.grace_ms);
    let cross_window_ms = config.cross_window_ms.unwrap_or(rt.cross_window_ms);

    let part = |role: &str| staged.files.iter().find(|(n, _)| n == role);
    let (tape_files, gap_spans, tape_source) = match config.tape.as_str() {
        "upload" => {
            let ob = part("orderbook")
                .ok_or_else(|| bad_request("upload tape needs an orderbook part".into()))?;
            let tr = part("trades")
                .ok_or_else(|| bad_request("upload tape needs a trades part".into()))?;
            (
                vec![
                    (TapeRole::Orderbook, ob.1.clone()),
                    (TapeRole::Trades, tr.1.clone()),
                ],
                Vec::new(),
                "upload",
            )
        }
        "live" => {
            let trades_root = rt.trades_binding.as_deref().ok_or_else(|| {
                bad_request("live tape unavailable: daemon has no trades binding".into())
            })?;
            let files = load_live_tape_files(trades_root, rt.books_binding.as_deref())
                .map_err(|e| bad_request(format!("live tape: {e:#}")))?;
            let gaps = read_gap_spans(trades_root, rt.symbol_stem.as_deref())
                .map_err(|e| bad_request(format!("gap ledger: {e:#}")))?;
            (files, gaps, "live")
        }
        other => return Err(bad_request(format!("unknown tape {other:?}"))),
    };
    let orders = match config.order_source.as_str() {
        "strategy" => OrderSourceSpec::Strategy,
        "signals" => {
            let sig = part("signals")
                .ok_or_else(|| bad_request("signals order source needs a signals part".into()))?;
            OrderSourceSpec::Signals(sig.1.clone())
        }
        other => return Err(bad_request(format!("unknown order_source {other:?}"))),
    };

    let base_params =
        TracerParams::load(&rt.params_path).map_err(|e| bad_request(format!("params: {e:#}")))?;
    let req = TraceRequest {
        tape_files,
        orders,
        overrides: overrides.clone(),
        grace_ms,
        cross_window_ms,
        gap_spans,
    };
    let prepared =
        match prepare(&req, &base_params).map_err(|e| bad_request(format!("prepare: {e:#}")))? {
            Err(report) => return Err(rejection(&report)),
            Ok(p) => p,
        };
    let trace_id = scoped_trace_id(&prepared.trace_id, &user);

    if config.preflight_only {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "trace_id": trace_id,
                "status": "preflight",
                "preflight": prepared.report,
            })),
        ));
    }

    if let Some(existing) = rt.read_record(&trace_id) {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "trace_id": existing.trace_id,
                "status": existing.status,
                "preflight": existing.preflight,
            })),
        ));
    }

    let trace_dir = rt.dir.join(&trace_id);
    let inputs_dir = trace_dir.join("inputs");
    std::fs::create_dir_all(&inputs_dir).map_err(|e| bad_request(format!("trace dir: {e}")))?;
    let mut inputs: Vec<TraceInputFile> = Vec::new();
    for (role, path) in &staged.files {
        if tape_source == "live" && role != "signals" {
            continue;
        }
        let file = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        std::fs::rename(path, inputs_dir.join(&file))
            .map_err(|e| bad_request(format!("stash {role}: {e}")))?;
        inputs.push(TraceInputFile {
            role: role.clone(),
            file,
        });
    }
    let record = TraceRecord {
        schema: TRACE_SCHEMA.into(),
        trace_id: trace_id.clone(),
        created_ms: now,
        user_id: Some(user),
        status: "queued".into(),
        error: None,
        order_source: config.order_source.clone(),
        tape_source: tape_source.into(),
        inputs,
        grace_ms,
        cross_window_ms,
        overrides,
        preflight: serde_json::to_value(&prepared.report).unwrap_or_default(),
        summary: None,
        effective: None,
    };
    rt.write_record(&record)
        .map_err(|e| bad_request(format!("record: {e:#}")))?;
    rt.tx
        .send(record.trace_id.clone())
        .map_err(|e| bad_request(format!("enqueue: {e}")))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "trace_id": record.trace_id,
            "status": record.status,
            "preflight": record.preflight,
        })),
    ))
}

pub async fn list_traces(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rt = runtime(&s)?;
    let caller = caller_of(&headers)?;
    let traces: Vec<serde_json::Value> = rt
        .list_records()
        .into_iter()
        .filter(|r| is_service(&caller) || r.user_id.as_deref() == Some(caller.as_str()))
        .map(|r| serde_json::to_value(&r).unwrap_or_default())
        .collect();
    Ok(Json(serde_json::json!({
        "schema": "tracer.traces.v1",
        "live": rt.live_record(),
        "traces": traces,
    })))
}

pub async fn get_trace(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rt = runtime(&s)?;
    caller_of(&headers)?;
    if id == LIVE_ID {
        return rt.live_record().map(Json).ok_or(StatusCode::NOT_FOUND);
    }
    let record = read_owned(&rt, &id, &headers)?;
    Ok(Json(serde_json::to_value(&record).unwrap_or_default()))
}

pub async fn delete_trace(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let rt = runtime(&s)?;
    let record = read_owned(&rt, &id, &headers)?;
    rt.delete_trace(&record.trace_id)?;
    Ok(StatusCode::NO_CONTENT)
}

fn trace_artifact(
    rt: &TraceRuntime,
    id: &str,
    name: &str,
    headers: &HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let record = read_owned(rt, id, headers)?;
    if record.status != "complete" {
        return Err(StatusCode::CONFLICT);
    }
    let text =
        std::fs::read_to_string(rt.dir.join(id).join(name)).map_err(|_| StatusCode::NOT_FOUND)?;
    serde_json::from_str(&text)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn trace_ledger(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    caller_of(&headers)?;
    if id == LIVE_ID {
        return Ok(crate::api::live_ledger_json(&s, None));
    }
    let rt = runtime(&s)?;
    trace_artifact(&rt, &id, "corrected_ledger.json", &headers)
}

pub async fn trace_fillrates(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    caller_of(&headers)?;
    if id == LIVE_ID {
        return crate::api::live_fillrates_json(&s, "all");
    }
    let rt = runtime(&s)?;
    trace_artifact(&rt, &id, "fillrates.json", &headers)
}

pub async fn trace_events(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<EventsQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    caller_of(&headers)?;
    let ledger_dir = if id == LIVE_ID {
        s.ledger_dir.clone()
    } else {
        let rt = runtime(&s)?;
        read_owned(&rt, &id, &headers)?;
        rt.dir.join(&id).join("ledger")
    };
    let page = paged_events(&ledger_dir, &q)?;
    Ok(Json(serde_json::to_value(page).unwrap_or_default()))
}

#[derive(serde::Deserialize)]
pub struct BookQuery {
    t0: Option<u64>,
    t1: Option<u64>,
    points: Option<usize>,
}

fn live_pseudo_record(rt: &TraceRuntime) -> TraceRecord {
    TraceRecord {
        schema: TRACE_SCHEMA.into(),
        trace_id: "live".into(),
        created_ms: 0,
        user_id: None,
        status: "live".into(),
        error: None,
        order_source: "strategy".into(),
        tape_source: "live".into(),
        inputs: Vec::new(),
        grace_ms: rt.grace_ms,
        cross_window_ms: rt.cross_window_ms,
        overrides: None,
        preflight: serde_json::Value::Null,
        summary: None,
        effective: None,
    }
}

fn record_or_live(rt: &TraceRuntime, id: &str) -> Result<TraceRecord, StatusCode> {
    if id == "live" {
        if rt.trades_binding.is_none() {
            return Err(StatusCode::NOT_FOUND);
        }
        return Ok(live_pseudo_record(rt));
    }
    rt.read_record(id).ok_or(StatusCode::NOT_FOUND)
}

fn owned_record_or_live(
    rt: &TraceRuntime,
    id: &str,
    headers: &HeaderMap,
) -> Result<TraceRecord, StatusCode> {
    let caller = caller_of(headers)?;
    let record = record_or_live(rt, id)?;
    visible_to(&record, &caller)?;
    Ok(record)
}

pub async fn trace_book(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<BookQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rt = runtime(&s)?;
    let record = owned_record_or_live(&rt, &id, &headers)?;
    let tape = rt
        .cached_tape(&record)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let points = q.points.unwrap_or(1024).min(4096);
    let series = book_series(&tape.books, q.t0, q.t1, points);
    Ok(Json(serde_json::json!({
        "schema": "tracer.tracebook.v1",
        "trace_id": id,
        "points": series,
    })))
}

#[derive(serde::Deserialize)]
pub struct DepthQuery {
    from: Option<u64>,
    to: Option<u64>,
    max: Option<usize>,
}

pub async fn trace_depth(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<DepthQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rt = runtime(&s)?;
    let max_rows = q.max.unwrap_or(DEPTH_ROWS_DEFAULT).clamp(1, DEPTH_ROWS_CAP);
    let rows: Vec<DepthRow> = if id == "live" {
        let root = rt.books_binding.as_deref().ok_or(StatusCode::NOT_FOUND)?;
        let dir = tracer_engine::market_engine::datatype_dir(root, "orderbooks")
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?;
        let path = latest_parquet_in(&dir).ok_or(StatusCode::NOT_FOUND)?;
        let rows =
            load_depth_rows(&[&path], None, None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        rows.last().cloned().into_iter().collect()
    } else {
        let record = read_owned(&rt, &id, &headers)?;
        let req = rt
            .request_for(&record)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let paths: Vec<&Path> = req
            .tape_files
            .iter()
            .filter(|(role, _)| *role == TapeRole::Orderbook)
            .map(|(_, p)| p.as_path())
            .collect();
        if paths.is_empty() {
            return Err(StatusCode::NOT_FOUND);
        }
        let rows =
            load_depth_rows(&paths, q.from, q.to).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        decimate_depth(rows, max_rows)
    };
    Ok(Json(serde_json::json!({
        "schema": "tracer.tracedepth.v1",
        "trace_id": id,
        "rows": rows,
    })))
}

#[derive(serde::Deserialize)]
pub struct PrintsQuery {
    cursor: Option<usize>,
    limit: Option<usize>,
}

pub async fn trace_prints(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<PrintsQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rt = runtime(&s)?;
    let record = owned_record_or_live(&rt, &id, &headers)?;
    let tape = rt
        .cached_tape(&record)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit = q.limit.unwrap_or(100).min(1000);
    let offset = q.cursor.unwrap_or(0);
    let total = tape.prints.len();
    let page: Vec<serde_json::Value> = tape
        .prints
        .iter()
        .rev()
        .skip(offset)
        .take(limit)
        .map(|(ts_ms, px, sz, is_buy)| {
            serde_json::json!({
                "ts_ms": ts_ms,
                "px": px,
                "sz": sz,
                "side": if *is_buy { "buy" } else { "sell" },
            })
        })
        .collect();
    let next = offset + page.len();
    Ok(Json(serde_json::json!({
        "schema": "tracer.traceprints.v1",
        "trace_id": id,
        "total": total,
        "prints": page,
        "older_cursor": if next < total { Some(next) } else { None },
    })))
}

#[derive(serde::Deserialize)]
pub struct SchemaQuery {
    source: String,
}

fn latest_parquet_in(dir: &Path) -> Option<PathBuf> {
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    names.sort();
    names.pop()
}

/// Newest parquet under the running daemon's binding dir for `role`.
fn live_role_parquet(rt: &TraceRuntime, role: TapeRole) -> Result<PathBuf, StatusCode> {
    let (binding, datatype) = match role {
        TapeRole::Orderbook => (rt.books_binding.as_deref(), "orderbooks"),
        TapeRole::Trades => (rt.trades_binding.as_deref(), "trades"),
    };
    let root = binding.ok_or(StatusCode::NOT_FOUND)?;
    let dir = tracer_engine::market_engine::datatype_dir(root, datatype)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    latest_parquet_in(&dir).ok_or(StatusCode::NOT_FOUND)
}

pub async fn trace_schema(
    State(s): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<SchemaQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rt = runtime(&s)?;
    let role = match q.source.as_str() {
        "orderbook" => TapeRole::Orderbook,
        "trades" => TapeRole::Trades,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let path: PathBuf = if id == "live" {
        live_role_parquet(&rt, role)?
    } else {
        let record = read_owned(&rt, &id, &headers)?;
        match record.inputs.iter().find(|f| f.role == role.label()) {
            Some(file) => rt.dir.join(&id).join("inputs").join(&file.file),
            // A trace that ran on the LIVE tape uploaded no parquet, so it has
            // no input to describe: its tape was — and still is — the daemon's
            // binding dirs. Describe those. Any other trace genuinely lacks
            // this role and keeps its 404.
            None if record.tape_source == "live" => live_role_parquet(&rt, role)?,
            None => return Err(StatusCode::NOT_FOUND),
        }
    };
    let fields: Vec<SchemaFieldOut> =
        parquet_schema_fields(&path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({
        "schema": "tracer.traceschema.v1",
        "trace_id": id,
        "source": role.label(),
        "fields": fields,
    })))
}
