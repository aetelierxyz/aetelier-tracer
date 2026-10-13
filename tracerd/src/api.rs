use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::extract::{MatchedPath, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;

use tracer_engine::ledger::{Event, EventKind, Leg};
use tracer_engine::market_engine::{GapIncident, read_gap_incidents};

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct LedgerLive {
    pub since_ts_ms: Option<u64>,
    pub n_kept: usize,
    pub total_usd: f64,
    pub fees_usd: f64,
    pub equity: Vec<(u64, f64)>,
    pub paper_n: usize,
    pub paper_total_usd: f64,
    pub paper_fees_usd: f64,
    pub paper_equity: Vec<(u64, f64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Readiness {
    pub ready: bool,
    pub reason: String,
}

impl Readiness {
    pub fn ready() -> Self {
        Self {
            ready: true,
            reason: String::new(),
        }
    }

    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            reason: reason.into(),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub status: Arc<RwLock<serde_json::Value>>,
    pub ledger_live: Arc<RwLock<LedgerLive>>,
    pub ready: Arc<RwLock<Readiness>>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub ledger_dir: PathBuf,
    pub artifacts_dir: Option<PathBuf>,
    pub trades_binding: Option<PathBuf>,
    pub books_binding: Option<PathBuf>,
    pub symbol_stem: Option<String>,
    pub traces: Option<Arc<crate::traces::TraceRuntime>>,
    pub now_ms: fn() -> u64,
}

pub fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

const TRACE_UPLOAD_BODY_CAP: usize = 450 * 1024 * 1024;

pub fn router(state: AppState, allowed_origins: &[String]) -> axum::Router {
    let cors = cors_layer(allowed_origins);
    let traces = axum::Router::new()
        .route(
            "/api/tracer/traces",
            get(crate::traces::list_traces).post(crate::traces::create_trace),
        )
        .route(
            "/api/tracer/traces/{id}",
            get(crate::traces::get_trace).delete(crate::traces::delete_trace),
        )
        .route(
            "/api/tracer/traces/{id}/ledger",
            get(crate::traces::trace_ledger),
        )
        .route(
            "/api/tracer/traces/{id}/fillrates",
            get(crate::traces::trace_fillrates),
        )
        .route(
            "/api/tracer/traces/{id}/events",
            get(crate::traces::trace_events),
        )
        .route(
            "/api/tracer/traces/{id}/book",
            get(crate::traces::trace_book),
        )
        .route(
            "/api/tracer/traces/{id}/depth",
            get(crate::traces::trace_depth),
        )
        .route(
            "/api/tracer/traces/{id}/prints",
            get(crate::traces::trace_prints),
        )
        .route(
            "/api/tracer/traces/{id}/schema",
            get(crate::traces::trace_schema),
        )
        .layer(axum::extract::DefaultBodyLimit::max(TRACE_UPLOAD_BODY_CAP));
    axum::Router::new()
        .route("/health", get(health))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_text))
        .route("/api/tracer/status", get(status))
        .route("/api/tracer/events", get(events))
        .route("/api/tracer/gaps", get(gaps))
        .route("/api/tracer/fillrates", get(fillrates))
        .route("/api/tracer/ledger", get(ledger))
        .route("/api/tracer/replay/ledger", get(replay_ledger))
        .route("/api/tracer/replay/fillrates", get(replay_fillrates))
        .merge(traces)
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.metrics.clone(),
            record_http,
        ))
        .with_state(state)
}

async fn record_http(
    State(metrics): State<Arc<crate::metrics::Metrics>>,
    matched: Option<MatchedPath>,
    request: Request,
    next: Next,
) -> Response {
    let route = matched
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    metrics.inc(
        "tracer_http_requests_total",
        &[("route", &route), ("status", &status)],
    );
    metrics.add(
        "tracer_http_request_ms_sum",
        &[("route", &route)],
        elapsed_ms,
    );
    metrics.inc("tracer_http_request_ms_count", &[("route", &route)]);
    response
}

async fn metrics_text(State(s): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            crate::metrics::CONTENT_TYPE,
        )],
        s.metrics.render(),
    )
}

fn cors_layer(origins: &[String]) -> tower_http::cors::CorsLayer {
    let parsed: Vec<axum::http::HeaderValue> =
        origins.iter().filter_map(|o| o.parse().ok()).collect();
    tower_http::cors::CorsLayer::new()
        .allow_origin(parsed)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::DELETE,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderName::from_static(crate::traces::USER_HEADER),
        ])
}

pub fn origins_from_env() -> Vec<String> {
    std::env::var("AETELIER_ALLOWED_ORIGINS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|o| o.trim().to_string())
                .filter(|o| !o.is_empty())
                .collect()
        })
        .filter(|v: &Vec<String>| !v.is_empty())
        .unwrap_or_else(|| {
            vec![
                "http://localhost:3000".into(),
                "http://127.0.0.1:3000".into(),
            ]
        })
}

async fn health() -> &'static str {
    "ok"
}

async fn readyz(State(s): State<AppState>) -> impl IntoResponse {
    let readiness = s.ready.read().unwrap().clone();
    if readiness.ready {
        (StatusCode::OK, "ready".to_owned())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {}", readiness.reason),
        )
    }
}

async fn status(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(s.status.read().unwrap().clone())
}

fn ledger_files_sorted(dir: &PathBuf) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .is_some_and(|n| n.to_string_lossy().ends_with(".jsonl"))
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

fn read_event_lines(path: &PathBuf) -> Vec<Event> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|l| serde_json::from_str::<Event>(l).ok())
        .collect()
}

#[derive(serde::Deserialize)]
pub struct EventsQuery {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub trade_id: Option<String>,
    pub kind: Option<String>,
    pub leg: Option<String>,
    pub regime: Option<String>,
    pub symbol: Option<String>,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
}

fn parse_kind(s: &str) -> Option<EventKind> {
    Some(match s {
        "POSTED" => EventKind::Posted,
        "OBSERVED" => EventKind::Observed,
        "FILLED" => EventKind::Filled,
        "EXPIRED" => EventKind::Expired,
        "TAKER_CROSS" => EventKind::TakerCross,
        "EVIDENCE" => EventKind::Evidence,
        _ => return None,
    })
}

fn parse_leg(s: &str) -> Option<Leg> {
    Some(match s {
        "entry" => Leg::Entry,
        "exit" => Leg::Exit,
        _ => return None,
    })
}

#[derive(serde::Serialize)]
pub struct EventsPage {
    events: Vec<Event>,
    older_cursor: Option<String>,
}

fn decode_cursor(cursor: &str) -> Option<(String, usize)> {
    let raw = B64.decode(cursor).ok()?;
    let s = String::from_utf8(raw).ok()?;
    let (file, line) = s.rsplit_once('|')?;
    Some((file.to_string(), line.parse().ok()?))
}

fn encode_cursor(file: &str, line: usize) -> String {
    B64.encode(format!("{file}|{line}"))
}

fn deduped_events(ledger_dir: &PathBuf) -> Vec<(String, usize, Event)> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut all: Vec<(String, usize, Event)> = Vec::new();
    for path in ledger_files_sorted(ledger_dir) {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        for (idx, e) in read_event_lines(&path).into_iter().enumerate() {
            if seen.insert(e.event_id.clone()) {
                all.push((name.clone(), idx, e));
            }
        }
    }
    all
}

pub fn paged_events(ledger_dir: &PathBuf, q: &EventsQuery) -> Result<EventsPage, StatusCode> {
    let limit = q.limit.unwrap_or(200).min(1000);
    let start = match &q.cursor {
        None => None,
        Some(c) => Some(decode_cursor(c).ok_or(StatusCode::BAD_REQUEST)?),
    };
    let kind = match &q.kind {
        None => None,
        Some(k) => Some(parse_kind(k).ok_or(StatusCode::BAD_REQUEST)?),
    };
    let leg = match &q.leg {
        None => None,
        Some(l) => Some(parse_leg(l).ok_or(StatusCode::BAD_REQUEST)?),
    };
    let mut all = deduped_events(ledger_dir);
    if let Some((cf, cl)) = &start {
        all.retain(|(name, idx, _)| name < cf || (name == cf && idx < cl));
    }
    all.retain(|(_, _, e)| {
        q.trade_id.as_ref().is_none_or(|t| &e.trade_id == t)
            && kind.is_none_or(|k| e.kind == k)
            && leg.is_none_or(|l| e.leg == l)
            && q.regime.as_ref().is_none_or(|r| &e.regime == r)
            && q.symbol.as_ref().is_none_or(|sym| &e.symbol == sym)
            && q.since_ms.is_none_or(|t| e.ts_ms >= t)
            && q.until_ms.is_none_or(|t| e.ts_ms <= t)
    });
    let cut = all.len().saturating_sub(limit);
    let older = (cut > 0).then(|| encode_cursor(&all[cut].0, all[cut].1));
    let events = all.split_off(cut).into_iter().map(|(_, _, e)| e).collect();
    Ok(EventsPage {
        events,
        older_cursor: older,
    })
}

async fn events(
    State(s): State<AppState>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<EventsPage>, StatusCode> {
    paged_events(&s.ledger_dir, &q).map(Json)
}

#[derive(serde::Deserialize)]
struct GapsQuery {
    window: Option<String>,
}

#[derive(serde::Serialize)]
struct GapIncidentOut {
    start_ms: u64,
    end_ms: u64,
    window_us: u64,
    cause: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    possible_dropped_trades: Option<u64>,
    exchange: String,
    symbol: String,
}

#[derive(serde::Serialize)]
struct GapsResponse {
    schema: &'static str,
    mode: &'static str,
    window: String,
    coverage_pct: f64,
    incident_count: usize,
    total_gap_ms: u64,
    est_dropped_trades_sum: u64,
    incidents: Vec<GapIncidentOut>,
}

fn merged_overlap_ms(spans: &mut [(u64, u64)], since: u64, now: u64) -> u64 {
    spans.sort_unstable();
    let mut total = 0u64;
    let mut cur: Option<(u64, u64)> = None;
    for &(s0, e0) in spans.iter() {
        let s = s0.max(since);
        let e = e0.min(now);
        if e <= s {
            continue;
        }
        match cur {
            Some((cs, ce)) if s <= ce => cur = Some((cs, ce.max(e))),
            Some((cs, ce)) => {
                total += ce - cs;
                cur = Some((s, e));
                let _ = cs;
            }
            None => cur = Some((s, e)),
        }
    }
    if let Some((cs, ce)) = cur {
        total += ce - cs;
    }
    total
}

async fn gaps(
    State(s): State<AppState>,
    Query(q): Query<GapsQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let trades_root = s.trades_binding.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let window = q.window.unwrap_or_else(|| "24h".into());
    let now = (s.now_ms)();
    let cutoff = match window.as_str() {
        "24h" => now.saturating_sub(24 * 3_600_000),
        "7d" => now.saturating_sub(7 * 24 * 3_600_000),
        "all" => 0,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let stem = s.symbol_stem.as_deref();
    let mut incidents: Vec<GapIncident> =
        read_gap_incidents(trades_root, stem).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(root) = &s.books_binding {
        incidents
            .extend(read_gap_incidents(root, stem).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?);
    }
    incidents.sort_by_key(|i| i.opened_epoch_us);
    let in_window: Vec<&GapIncident> = incidents
        .iter()
        .filter(|i| i.closed_epoch_us / 1_000 > cutoff)
        .collect();
    let since = if window == "all" {
        in_window
            .first()
            .map(|i| i.opened_epoch_us / 1_000)
            .unwrap_or(now)
    } else {
        cutoff
    };
    let mut spans: Vec<(u64, u64)> = in_window
        .iter()
        .map(|i| (i.opened_epoch_us / 1_000, i.closed_epoch_us / 1_000))
        .collect();
    let total_gap_ms = merged_overlap_ms(&mut spans, since, now);
    let elapsed = now.saturating_sub(since);
    let coverage_pct = if elapsed == 0 {
        100.0
    } else {
        100.0 * (1.0 - total_gap_ms as f64 / elapsed as f64)
    };
    let body = GapsResponse {
        schema: "tracer.gaps.v1",
        mode: "live",
        window,
        coverage_pct,
        incident_count: in_window.len(),
        total_gap_ms,
        est_dropped_trades_sum: in_window
            .iter()
            .filter_map(|i| i.possible_dropped_trades)
            .sum(),
        incidents: in_window
            .iter()
            .map(|i| GapIncidentOut {
                start_ms: i.opened_epoch_us / 1_000,
                end_ms: i.closed_epoch_us / 1_000,
                window_us: i.window_us,
                cause: i.cause.clone(),
                possible_dropped_trades: i.possible_dropped_trades,
                exchange: i.exchange.clone(),
                symbol: i.symbol.clone(),
            })
            .collect(),
    };
    Ok(Json(serde_json::to_value(body).unwrap()))
}

#[derive(serde::Deserialize)]
struct FillratesQuery {
    window: Option<String>,
}

#[derive(serde::Serialize)]
struct Pct {
    p50: u64,
    p90: u64,
    n: usize,
}

fn pct_json(mut v: Vec<u64>) -> Option<Pct> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let at = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize];
    Some(Pct {
        p50: at(0.50),
        p90: at(0.90),
        n: v.len(),
    })
}

#[derive(Default)]
struct LiveEntryAgg {
    posted: u64,
    already_through: u64,
    filled_touch: u64,
    filled_through: u64,
    filled_queue: u64,
    expired_unfilled: u64,
    gapped: u64,
    book_degraded_stale: u64,
    ttf_touch: Vec<u64>,
    ttf_through: Vec<u64>,
}

#[derive(Default)]
struct LiveExitAgg {
    posted: u64,
    filled_through: u64,
    taker_cross: u64,
    ttf_through: Vec<u64>,
}

#[derive(serde::Serialize)]
struct LiveEntryCell {
    posted: u64,
    already_through: u64,
    filled_touch: u64,
    filled_through: u64,
    filled_queue: u64,
    expired_unfilled: u64,
    gapped: u64,
    book_degraded_stale: u64,
    time_to_fill_ms: BTreeMap<&'static str, Option<Pct>>,
}

#[derive(serde::Serialize)]
struct LiveExitCell {
    posted: u64,
    filled_through: u64,
    taker_cross: u64,
    exit_unpriced: u64,
    time_to_fill_ms: BTreeMap<&'static str, Option<Pct>>,
}

#[derive(serde::Serialize)]
struct LiveFillrates {
    schema: &'static str,
    mode: &'static str,
    window: String,
    denominator: &'static str,
    entry_cells: BTreeMap<String, LiveEntryCell>,
    exit_cells: BTreeMap<String, LiveExitCell>,
}

pub fn live_fillrates_json(
    s: &AppState,
    window: &str,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let window = window.to_string();
    let cutoff = match window.as_str() {
        "24h" => (s.now_ms)().saturating_sub(24 * 3_600_000),
        "7d" => (s.now_ms)().saturating_sub(7 * 24 * 3_600_000),
        "all" => 0,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let mut entry: BTreeMap<String, LiveEntryAgg> = BTreeMap::new();
    let mut exit: BTreeMap<String, LiveExitAgg> = BTreeMap::new();
    let mut posted_ts: HashMap<String, u64> = HashMap::new();
    use tracer_engine::ledger::{EventKind, Leg, PostState, Verdict};
    for (_, _, e) in deduped_events(&s.ledger_dir) {
        if e.kind == EventKind::Posted {
            let leg = match e.leg {
                Leg::Entry => "entry",
                Leg::Exit => "exit",
            };
            posted_ts.insert(format!("{}|{leg}", e.trade_id), e.ts_ms);
        }
        if e.ts_ms < cutoff {
            continue;
        }
        let key = format!("{}|{}", e.symbol, e.regime);
        match (e.leg, e.kind) {
            (Leg::Entry, EventKind::Posted) => {
                let c = entry.entry(key).or_default();
                c.posted += 1;
                if e.post_state == Some(PostState::AlreadyThrough) {
                    c.already_through += 1;
                }
                if e.book_staleness_ms.is_some() && e.depth_ahead_usd.is_none() {
                    c.book_degraded_stale += 1;
                }
            }
            (Leg::Entry, EventKind::Filled | EventKind::Expired) => {
                let c = entry.entry(key).or_default();
                if e.filled_touch_ts_ms.is_some() {
                    c.filled_touch += 1;
                }
                let posted = posted_ts.get(&format!("{}|entry", e.trade_id)).copied();
                if let (Some(t0), Some(t)) = (posted, e.filled_touch_ts_ms) {
                    c.ttf_touch.push(t.saturating_sub(t0));
                }
                if let (Some(t0), Some(t)) = (posted, e.filled_through_ts_ms) {
                    c.ttf_through.push(t.saturating_sub(t0));
                }
                match e.verdict {
                    Some(Verdict::FilledThrough) => c.filled_through += 1,
                    Some(Verdict::FilledQueue) => c.filled_queue += 1,
                    Some(Verdict::UnfilledGapped) => c.gapped += 1,
                    Some(Verdict::Unfilled) => c.expired_unfilled += 1,
                    _ => {}
                }
            }
            (Leg::Exit, EventKind::Posted) => {
                exit.entry(key).or_default().posted += 1;
            }
            (Leg::Exit, EventKind::Filled) => {
                let c = exit.entry(key).or_default();
                c.filled_through += 1;
                let posted = posted_ts.get(&format!("{}|exit", e.trade_id)).copied();
                if let (Some(t0), Some(t)) = (posted, e.filled_through_ts_ms) {
                    c.ttf_through.push(t.saturating_sub(t0));
                }
            }
            (Leg::Exit, EventKind::TakerCross) => {
                exit.entry(key).or_default().taker_cross += 1;
            }
            (Leg::Exit, EventKind::Expired) => {}
            _ => {}
        }
    }
    let entry_cells: BTreeMap<String, LiveEntryCell> = entry
        .into_iter()
        .map(|(k, a)| {
            (
                k,
                LiveEntryCell {
                    posted: a.posted,
                    already_through: a.already_through,
                    filled_touch: a.filled_touch,
                    filled_through: a.filled_through,
                    filled_queue: a.filled_queue,
                    expired_unfilled: a.expired_unfilled,
                    gapped: a.gapped,
                    book_degraded_stale: a.book_degraded_stale,
                    time_to_fill_ms: BTreeMap::from([
                        ("touch", pct_json(a.ttf_touch)),
                        ("through", pct_json(a.ttf_through)),
                    ]),
                },
            )
        })
        .collect();
    let exit_cells: BTreeMap<String, LiveExitCell> = exit
        .into_iter()
        .map(|(k, a)| {
            (
                k,
                LiveExitCell {
                    posted: a.posted,
                    filled_through: a.filled_through,
                    taker_cross: a.taker_cross,
                    exit_unpriced: a.posted.saturating_sub(a.filled_through + a.taker_cross),
                    time_to_fill_ms: BTreeMap::from([("through", pct_json(a.ttf_through))]),
                },
            )
        })
        .collect();
    let body = LiveFillrates {
        schema: "tracer.fillrates.live.v1",
        mode: "live",
        window,
        denominator: "posted_signals",
        entry_cells,
        exit_cells,
    };
    Ok(Json(serde_json::to_value(body).unwrap()))
}

async fn fillrates(
    State(s): State<AppState>,
    Query(q): Query<FillratesQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    live_fillrates_json(&s, &q.window.unwrap_or_else(|| "24h".into()))
}

#[derive(serde::Deserialize)]
struct LedgerQuery {
    points: Option<usize>,
}

pub fn live_ledger_json(s: &AppState, points: Option<usize>) -> Json<serde_json::Value> {
    let c = s.ledger_live.read().unwrap().clone();
    let points = points.unwrap_or(500);
    let tail = |v: &[(u64, f64)]| -> Vec<(u64, f64)> {
        v.iter().rev().take(points).rev().copied().collect()
    };
    Json(serde_json::json!({
        "schema": "tracer.ledger.live.v1",
        "mode": "live",
        "since_ts_ms": c.since_ts_ms,
        "n_kept": c.n_kept,
        "total_usd": c.total_usd,
        "fees_usd": c.fees_usd,
        "equity": tail(&c.equity),
        "paper": {
            "denominator": "every_posted_signal",
            "n": c.paper_n,
            "total_usd": c.paper_total_usd,
            "fees_usd": c.paper_fees_usd,
            "equity": tail(&c.paper_equity),
        },
        "delta_total_usd": c.total_usd - c.paper_total_usd,
    }))
}

async fn ledger(
    State(s): State<AppState>,
    Query(q): Query<LedgerQuery>,
) -> Json<serde_json::Value> {
    live_ledger_json(&s, q.points)
}

fn artifact_json(s: &AppState, name: &str) -> Result<Json<serde_json::Value>, StatusCode> {
    let dir = s.artifacts_dir.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let text = std::fs::read_to_string(dir.join(name)).map_err(|_| StatusCode::NOT_FOUND)?;
    serde_json::from_str(&text)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn replay_ledger(State(s): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    artifact_json(&s, "corrected_ledger.json")
}

async fn replay_fillrates(
    State(s): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    artifact_json(&s, "fillrates.json")
}
