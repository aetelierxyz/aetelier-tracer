use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;

use tracer_engine::ledger::Event;

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

#[derive(Clone)]
pub struct AppState {
    pub status: Arc<RwLock<serde_json::Value>>,
    pub ledger_live: Arc<RwLock<LedgerLive>>,
    pub ready: Arc<RwLock<bool>>,
    pub ledger_dir: PathBuf,
    pub artifacts_dir: Option<PathBuf>,
    pub now_ms: fn() -> u64,
}

pub fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn router(state: AppState, allowed_origins: &[String]) -> axum::Router {
    let cors = cors_layer(allowed_origins);
    axum::Router::new()
        .route("/health", get(health))
        .route("/readyz", get(readyz))
        .route("/api/tracer/status", get(status))
        .route("/api/tracer/events", get(events))
        .route("/api/tracer/fillrates", get(fillrates))
        .route("/api/tracer/ledger", get(ledger))
        .route("/api/tracer/replay/ledger", get(replay_ledger))
        .route("/api/tracer/replay/fillrates", get(replay_fillrates))
        .layer(cors)
        .with_state(state)
}

fn cors_layer(origins: &[String]) -> tower_http::cors::CorsLayer {
    let parsed: Vec<axum::http::HeaderValue> =
        origins.iter().filter_map(|o| o.parse().ok()).collect();
    tower_http::cors::CorsLayer::new()
        .allow_origin(parsed)
        .allow_methods([axum::http::Method::GET])
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
    if *s.ready.read().unwrap() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
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
struct EventsQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(serde::Serialize)]
struct EventsPage {
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

async fn events(
    State(s): State<AppState>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<EventsPage>, StatusCode> {
    let limit = q.limit.unwrap_or(200).min(1000);
    let start = match &q.cursor {
        None => None,
        Some(c) => Some(decode_cursor(c).ok_or(StatusCode::BAD_REQUEST)?),
    };
    let mut all = deduped_events(&s.ledger_dir);
    if let Some((cf, cl)) = &start {
        all.retain(|(name, idx, _)| name < cf || (name == cf && idx < cl));
    }
    let cut = all.len().saturating_sub(limit);
    let older = (cut > 0).then(|| encode_cursor(&all[cut].0, all[cut].1));
    let events = all.split_off(cut).into_iter().map(|(_, _, e)| e).collect();
    Ok(Json(EventsPage {
        events,
        older_cursor: older,
    }))
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

async fn fillrates(
    State(s): State<AppState>,
    Query(q): Query<FillratesQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let window = q.window.unwrap_or_else(|| "24h".into());
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

#[derive(serde::Deserialize)]
struct LedgerQuery {
    points: Option<usize>,
}

async fn ledger(
    State(s): State<AppState>,
    Query(q): Query<LedgerQuery>,
) -> Json<serde_json::Value> {
    let c = s.ledger_live.read().unwrap().clone();
    let points = q.points.unwrap_or(500);
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
