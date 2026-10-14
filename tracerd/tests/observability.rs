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
use api::{AppState, LedgerLive, Readiness, router, wall_now_ms};

fn state(name: &str) -> AppState {
    let dir = std::env::temp_dir().join(format!("tracerd_observability_{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    AppState {
        status: Arc::new(RwLock::new(serde_json::json!({"mode": "live"}))),
        ledger_live: Arc::new(RwLock::new(LedgerLive::default())),
        ready: Arc::new(RwLock::new(Readiness::not_ready("first tick pending"))),
        metrics: metrics::Metrics::shared(),
        ledger_dir: dir,
        artifacts_dir: None,
        trades_binding: None,
        books_binding: None,
        symbol_stem: None,
        traces: None,
        params_path: None,
        now_ms: wall_now_ms,
    }
}

fn app(state: &AppState) -> axum::Router {
    router(state.clone(), &["http://localhost:3000".to_string()])
}

async fn get(state: &AppState, uri: &str) -> (u16, Option<String>, String) {
    let response = app(state)
        .oneshot(
            axum::http::Request::builder()
                .uri(uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let code = response.status().as_u16();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .map(|value| value.to_str().unwrap().to_owned());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        code,
        content_type,
        String::from_utf8(bytes.to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn readyz_reports_the_reason_until_the_runtime_marks_it_ready() {
    let state = state("readyz");

    let (code, _, body) = get(&state, "/readyz").await;
    assert_eq!(code, 503);
    assert_eq!(body, "not ready: first tick pending");

    *state.ready.write().unwrap() = Readiness::ready();
    let (code, _, body) = get(&state, "/readyz").await;
    assert_eq!(code, 200);
    assert_eq!(body, "ready");

    *state.ready.write().unwrap() = Readiness::not_ready("tick failed 3 time(s) in a row: boom");
    let (code, _, body) = get(&state, "/readyz").await;
    assert_eq!(code, 503);
    assert_eq!(body, "not ready: tick failed 3 time(s) in a row: boom");
}

#[tokio::test]
async fn metrics_count_requests_by_matched_route_and_status() {
    let state = state("http_counters");

    assert_eq!(get(&state, "/health").await.0, 200);
    assert_eq!(get(&state, "/health").await.0, 200);
    assert_eq!(get(&state, "/readyz").await.0, 503);
    assert_eq!(get(&state, "/nowhere").await.0, 404);

    let (code, content_type, body) = get(&state, "/metrics").await;
    assert_eq!(code, 200);
    assert_eq!(
        content_type.as_deref(),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    for line in [
        "# TYPE tracer_http_requests_total counter",
        "tracer_http_requests_total{route=\"/health\",status=\"200\"} 2",
        "tracer_http_requests_total{route=\"/readyz\",status=\"503\"} 1",
        "tracer_http_requests_total{route=\"unmatched\",status=\"404\"} 1",
        "tracer_http_request_ms_count{route=\"/health\"} 2",
    ] {
        assert!(
            body.lines().any(|l| l == line),
            "missing {line:?} in\n{body}"
        );
    }
    assert!(
        body.lines()
            .any(|l| l.starts_with("tracer_http_request_ms_sum{route=\"/health\"} ")),
        "{body}"
    );
}

#[tokio::test]
async fn metrics_render_gauges_the_runtime_sets() {
    let state = state("gauges");
    state.metrics.set("tracer_ready", &[], 1.0);
    state.metrics.set(
        "tracer_stream_advance_age_ms",
        &[("stream", "trades")],
        2500.0,
    );
    state.metrics.set("tracer_ledger_bytes", &[], 123_456.0);

    let (_, _, body) = get(&state, "/metrics").await;

    for line in [
        "# TYPE tracer_ledger_bytes gauge",
        "tracer_ledger_bytes 123456",
        "tracer_ready 1",
        "tracer_stream_advance_age_ms{stream=\"trades\"} 2500",
    ] {
        assert!(
            body.lines().any(|l| l == line),
            "missing {line:?} in\n{body}"
        );
    }
}
