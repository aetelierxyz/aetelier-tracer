mod api;
mod metrics;
mod tape_cache;
mod traces;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use clap::{Parser, Subcommand};
use tracing::{error, info};

use api::{AppState, LedgerLive, Readiness, origins_from_env, router, wall_now_ms};
use metrics::Metrics;
use tracer_engine::corrected::{
    DEFAULT_CROSS_WINDOW_MS, DEFAULT_GRACE_MS, emit_events, evaluate, write_artifacts,
};
use tracer_engine::engine::EngineStatus;
use tracer_engine::engine::{DEFAULT_IDLE_AFTER_MS, LiveConfig, LiveEngine};
use tracer_engine::ledger::LedgerWriter;
use tracer_engine::nyse::regime;
use tracer_engine::params::TracerParams;
use tracer_engine::replay::{load_symbol_data, paper_trades_from};

#[derive(Parser)]
#[command(
    name = "tracerd",
    version = tracer_engine::ENGINE_VERSION,
    about = "Tracer fill-probability instrument: replay, live daemon, API"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Run {
        #[arg(long, env = "TRACER_SYMBOL")]
        symbol: String,
        #[arg(long, env = "TRACER_TRADES_BINDING")]
        trades_binding: PathBuf,
        #[arg(long, env = "TRACER_BOOKS_BINDING")]
        books_binding: Option<PathBuf>,
        #[arg(
            long,
            env = "TRACER_PARAMS",
            default_value = "./params/tracer_params.v1.json"
        )]
        params: PathBuf,
        #[arg(long, env = "TRACER_LEDGER_DIR", default_value = "./out/live/ledger")]
        ledger_dir: PathBuf,
        #[arg(long, env = "TRACER_ARTIFACTS_DIR")]
        artifacts: Option<PathBuf>,
        #[arg(long, env = "TRACER_QUEUE_VERDICTS", default_value_t = false)]
        queue_verdicts: bool,
        #[arg(long, env = "TRACER_POLL_MS", default_value_t = 2_000)]
        poll_ms: u64,
        #[arg(long, env = "TRACER_PORT", default_value_t = 8330)]
        port: u16,
        #[arg(long, env = "TRACER_EXIT_GRACE_MS", default_value_t = DEFAULT_GRACE_MS)]
        grace_ms: u64,
        #[arg(long, env = "TRACER_CROSS_WINDOW_MS", default_value_t = DEFAULT_CROSS_WINDOW_MS)]
        cross_window_ms: u64,
        #[arg(long, env = "TRACER_IDLE_AFTER_MS", default_value_t = DEFAULT_IDLE_AFTER_MS)]
        idle_after_ms: u64,
        #[arg(long, env = "TRACER_TRACES_DIR")]
        traces_dir: Option<PathBuf>,
        #[arg(long, env = "TRACER_MAX_TRACES_PER_USER", default_value_t = traces::DEFAULT_MAX_TRACES_PER_USER)]
        max_traces_per_user: usize,
        #[arg(long, env = "TRACER_TAPE_RETENTION_MS", default_value_t = traces::DEFAULT_TAPE_RETENTION_MS)]
        tape_retention_ms: u64,
        #[arg(long, env = "KAFKA_BROKERS")]
        kafka_brokers: Option<String>,
    },
    Replay {
        #[arg(long, env = "TRACER_FILLS_DIR")]
        fills: PathBuf,
        #[arg(long, default_value = "./params/tracer_params.v1.json")]
        params: PathBuf,
        #[arg(long, default_value = "./out/replay")]
        out: PathBuf,
        #[arg(long, env = "TRACER_EXIT_GRACE_MS", default_value_t = DEFAULT_GRACE_MS)]
        grace_ms: u64,
        #[arg(long, env = "TRACER_CROSS_WINDOW_MS", default_value_t = DEFAULT_CROSS_WINDOW_MS)]
        cross_window_ms: u64,
    },
    Serve {
        #[arg(long, env = "TRACER_LEDGER_DIR")]
        ledger_dir: PathBuf,
        #[arg(long)]
        artifacts: Option<PathBuf>,
        #[arg(long, env = "TRACER_PORT", default_value_t = 8330)]
        port: u16,
        #[arg(long, env = "TRACER_TRACES_DIR")]
        traces_dir: Option<PathBuf>,
        #[arg(long, env = "TRACER_MAX_TRACES_PER_USER", default_value_t = traces::DEFAULT_MAX_TRACES_PER_USER)]
        max_traces_per_user: usize,
        #[arg(long, env = "TRACER_TAPE_RETENTION_MS", default_value_t = traces::DEFAULT_TAPE_RETENTION_MS)]
        tape_retention_ms: u64,
        #[arg(
            long,
            env = "TRACER_PARAMS",
            default_value = "./params/tracer_params.v1.json"
        )]
        params: PathBuf,
    },
    ExportGolden {
        #[arg(long, env = "TRACER_FILLS_DIR")]
        fills: PathBuf,
        #[arg(long, default_value = "./params/tracer_params.v1.json")]
        params: PathBuf,
        #[arg(long, default_value = "./out/paper_trades_golden.csv")]
        out: PathBuf,
    },
    Evaluate {
        #[arg(long, env = "TRACER_TAPE")]
        tape: String,
        #[arg(long, env = "TRACER_ORDERS")]
        orders: String,
        #[arg(
            long,
            env = "TRACER_PARAMS",
            default_value = "./params/tracer_params.v1.json"
        )]
        params: PathBuf,
        #[arg(long, default_value = "./out/trace")]
        out: PathBuf,
        #[arg(long, env = "TRACER_EXIT_GRACE_MS", default_value_t = DEFAULT_GRACE_MS)]
        grace_ms: u64,
        #[arg(long, env = "TRACER_CROSS_WINDOW_MS", default_value_t = DEFAULT_CROSS_WINDOW_MS)]
        cross_window_ms: u64,
        #[arg(long)]
        notional_usd: Option<f64>,
        #[arg(long)]
        hold_ms: Option<u64>,
        #[arg(long)]
        maker_fee_bps: Option<f64>,
        #[arg(long)]
        taker_fee_bps: Option<f64>,
        #[arg(long, env = "TRACER_TRADES_BINDING")]
        trades_binding: Option<PathBuf>,
        #[arg(long, env = "TRACER_BOOKS_BINDING")]
        books_binding: Option<PathBuf>,
    },
}

async fn serve_api(state: AppState, port: u16) -> anyhow::Result<()> {
    let origins = origins_from_env();
    let app = router(state, &origins);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!(port, "api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_live(
    symbol: String,
    trades_binding: PathBuf,
    books_binding: Option<PathBuf>,
    params_path: PathBuf,
    ledger_dir: PathBuf,
    artifacts_dir: Option<PathBuf>,
    queue_verdicts: bool,
    poll_ms: u64,
    port: u16,
    grace_ms: u64,
    cross_window_ms: u64,
    idle_after_ms: u64,
    traces_dir: Option<PathBuf>,
    max_traces_per_user: usize,
    tape_retention_ms: u64,
    kafka_brokers: Option<String>,
) -> anyhow::Result<()> {
    let params = TracerParams::load(&params_path)?;
    let stem = symbol.replace(':', "_");
    let _ = kafka_brokers;
    let metrics = Metrics::shared();
    let traces_rt = traces_dir
        .map(|dir| {
            traces::TraceRuntime::start(
                dir,
                params_path.clone(),
                grace_ms,
                cross_window_ms,
                Some(trades_binding.clone()),
                books_binding.clone(),
                Some(stem.clone()),
                max_traces_per_user,
                tape_retention_ms,
                metrics.clone(),
            )
        })
        .transpose()?;
    let cfg = LiveConfig {
        symbol_stem: stem.clone(),
        grace_ms,
        cross_window_ms,
        queue_verdicts,
        idle_after_ms,
    };
    let mut engine = LiveEngine::new(
        params,
        cfg,
        &trades_binding,
        books_binding.as_deref(),
        &ledger_dir,
    )?;
    let state = AppState {
        status: Arc::new(RwLock::new(serde_json::json!({"mode": "starting"}))),
        ledger_live: Arc::new(RwLock::new(LedgerLive::default())),
        ready: Arc::new(RwLock::new(Readiness::not_ready("first tick pending"))),
        metrics: metrics.clone(),
        ledger_dir: ledger_dir.clone(),
        artifacts_dir,
        trades_binding: Some(trades_binding.clone()),
        books_binding: books_binding.clone(),
        symbol_stem: Some(stem),
        traces: traces_rt,
        params_path: Some(params_path),
        now_ms: wall_now_ms,
    };
    let publish = state.clone();
    std::thread::spawn(move || {
        let mut consecutive_errors: u64 = 0;
        let mut ticks: u64 = 0;
        loop {
            let wall = wall_now_ms();
            let started = std::time::Instant::now();
            let outcome = engine.tick(wall);
            let tick_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            ticks += 1;
            match &outcome {
                Ok(()) => consecutive_errors = 0,
                Err(e) => {
                    consecutive_errors += 1;
                    error!(error = %format!("{e:#}"), consecutive = consecutive_errors, "tick failed");
                }
            }
            let readiness = readiness_after_tick(outcome.as_ref().err(), consecutive_errors);
            record_tick(&metrics, &readiness, consecutive_errors, tick_ms);
            let status = engine.status(wall_now_ms());
            record_engine_status(&metrics, &status);
            if ticks % LEDGER_SIZE_EVERY_TICKS == 1 {
                metrics.set(
                    "tracer_ledger_bytes",
                    &[],
                    metrics::dir_bytes(&ledger_dir) as f64,
                );
            }
            *publish.status.write().unwrap() =
                serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
            *publish.ledger_live.write().unwrap() = LedgerLive {
                since_ts_ms: status.first_post_ts_ms,
                n_kept: engine.corrected_points.len(),
                total_usd: engine
                    .corrected_points
                    .last()
                    .map(|(_, c)| *c)
                    .unwrap_or(0.0),
                fees_usd: status.corrected_fees_usd,
                equity: engine.corrected_points.clone(),
                paper_n: engine.paper_points.len(),
                paper_total_usd: engine.paper_points.last().map(|(_, c)| *c).unwrap_or(0.0),
                paper_fees_usd: status.paper_fees_usd,
                paper_equity: engine.paper_points.clone(),
            };
            *publish.ready.write().unwrap() = readiness;
            std::thread::sleep(std::time::Duration::from_millis(poll_ms));
        }
    });
    serve_api(state, port).await
}

const LEDGER_SIZE_EVERY_TICKS: u64 = 15;

fn readiness_after_tick(error: Option<&anyhow::Error>, consecutive_errors: u64) -> Readiness {
    match error {
        None => Readiness::ready(),
        Some(e) => Readiness::not_ready(format!(
            "tick failed {consecutive_errors} time(s) in a row: {e:#}"
        )),
    }
}

fn record_tick(metrics: &Metrics, readiness: &Readiness, consecutive_errors: u64, tick_ms: u64) {
    let result = if readiness.ready { "ok" } else { "error" };
    metrics.inc("tracer_tick_total", &[("result", result)]);
    metrics.set(
        "tracer_tick_consecutive_errors",
        &[],
        consecutive_errors as f64,
    );
    metrics.set("tracer_tick_last_duration_ms", &[], tick_ms as f64);
    metrics.set_flag("tracer_ready", &[], readiness.ready);
}

fn record_engine_status(metrics: &Metrics, status: &EngineStatus) {
    let streams = [
        ("trades", Some(&status.trades)),
        ("books", status.books.as_ref()),
    ];
    for (name, stream) in streams {
        let Some(stream) = stream else {
            continue;
        };
        let labels = [("stream", name)];
        metrics.set(
            "tracer_stream_watermark_ms",
            &labels,
            stream.watermark_ms as f64,
        );
        metrics.set(
            "tracer_stream_advance_age_ms",
            &labels,
            stream.advance_age_ms as f64,
        );
        metrics.set(
            "tracer_stream_files_read",
            &labels,
            stream.files_read as f64,
        );
        metrics.set_flag("tracer_stream_stalled", &labels, stream.stalled);
    }
    metrics.set(
        "tracer_merged_watermark_ms",
        &[],
        status.merged_watermark_ms as f64,
    );
    metrics.set(
        "tracer_verdict_trail_ms",
        &[],
        status.verdict_trail_ms as f64,
    );
    metrics.set("tracer_open_trades", &[], status.open_trades as f64);
    metrics.set("tracer_events_written", &[], status.events_written as f64);
    metrics.set(
        "tracer_corrected_n_kept",
        &[],
        status.corrected_n_kept as f64,
    );
    metrics.set("tracer_paper_n", &[], status.paper_n as f64);
    metrics.set_flag("tracer_idle", &[], status.idle);
    metrics.set_flag("tracer_binding_pending", &[], status.binding_pending);
    metrics.set_flag("tracer_calendar_expired", &[], status.calendar_expired);
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let json = std::env::var("TRACER_LOG_FORMAT").is_ok_and(|value| value == "json");
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn run_replay(
    fills: &std::path::Path,
    params_path: &std::path::Path,
    out: &std::path::Path,
    grace_ms: u64,
    cross_window_ms: u64,
) -> anyhow::Result<()> {
    let params = TracerParams::load(params_path)?;
    let data = load_symbol_data(fills, &params)?;
    let trades = paper_trades_from(&data, &params)?;
    println!("paper trades: {}", trades.len());
    let verdicts = evaluate(&trades, &data, &params, grace_ms, cross_window_ms)?;
    write_artifacts(&verdicts, &params, grace_ms, cross_window_ms, out)?;
    let mut writer = LedgerWriter::new(&out.join("ledger"))?;
    emit_events(&verdicts, &params, grace_ms, &mut writer)?;
    let entry_through = verdicts
        .iter()
        .filter(|v| v.entry[0].through.is_some())
        .count();
    let kept = verdicts.iter().filter(|v| v.corrected.is_some()).count();
    println!(
        "entry FILLED_THROUGH {entry_through}/{} ({:.1}%), corrected kept {kept}, events {}",
        verdicts.len(),
        100.0 * entry_through as f64 / verdicts.len() as f64,
        writer.events_written
    );
    println!(
        "artifacts: {}/corrected_ledger.json, fillrates.json, ledger/",
        out.display()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_evaluate(
    tape: &str,
    orders: &str,
    params_path: &std::path::Path,
    out: &std::path::Path,
    grace_ms: u64,
    cross_window_ms: u64,
    overrides: Option<tracer_engine::trace::TraceOverrides>,
    trades_binding: Option<&std::path::Path>,
    books_binding: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    use tracer_engine::market_engine::read_gap_spans;
    use tracer_engine::trace::{
        OrderSourceSpec, TapeRole, TraceRequest, execute, load_live_tape_files, prepare, sniff_role,
    };
    let (tape_files, gap_spans) = if tape == "live" {
        let trades_root =
            trades_binding.ok_or_else(|| anyhow::anyhow!("--tape live needs --trades-binding"))?;
        let files = load_live_tape_files(trades_root, books_binding)?;
        (files, read_gap_spans(trades_root, None)?)
    } else {
        let mut files: Vec<(TapeRole, PathBuf)> = Vec::new();
        for part in tape.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let path = PathBuf::from(part);
            files.push((sniff_role(&path)?, path));
        }
        anyhow::ensure!(!files.is_empty(), "--tape lists no files");
        (files, Vec::new())
    };
    let order_source = if orders == "strategy" {
        OrderSourceSpec::Strategy
    } else {
        OrderSourceSpec::Signals(PathBuf::from(orders))
    };
    let base_params = TracerParams::load(params_path)?;
    let req = TraceRequest {
        tape_files,
        orders: order_source,
        overrides,
        grace_ms,
        cross_window_ms,
        gap_spans,
    };
    match prepare(&req, &base_params)? {
        Err(report) => {
            println!("{}", serde_json::to_string_pretty(&report)?);
            anyhow::bail!(
                "strict validation failed: {} errors",
                report.row_errors.len()
            );
        }
        Ok(prepared) => {
            let summary = execute(&prepared, grace_ms, cross_window_ms, out)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            println!(
                "artifacts: {}/corrected_ledger.json, fillrates.json, ledger/",
                out.display()
            );
            Ok(())
        }
    }
}

fn export_golden(
    fills: &std::path::Path,
    params_path: &std::path::Path,
    out: &std::path::Path,
) -> anyhow::Result<()> {
    use std::io::Write;
    let params = TracerParams::load(params_path)?;
    let data = load_symbol_data(fills, &params)?;
    let trades = paper_trades_from(&data, &params)?;
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut w = std::io::BufWriter::new(std::fs::File::create(out)?);
    writeln!(
        w,
        "symbol,side,score,decile,t_entry_ms,entry_px,t_exit_ms,t_exit_real_ms,exit_px,fee_entry_usd,fee_exit_usd,pnl_usd,regime"
    )?;
    for t in &trades {
        writeln!(
            w,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
        )?;
    }
    w.flush()?;
    println!("{} trades -> {}", trades.len(), out.display());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    match Cli::parse().command {
        Command::Run {
            symbol,
            trades_binding,
            books_binding,
            params,
            ledger_dir,
            artifacts,
            queue_verdicts,
            poll_ms,
            port,
            grace_ms,
            cross_window_ms,
            idle_after_ms,
            traces_dir,
            max_traces_per_user,
            tape_retention_ms,
            kafka_brokers,
        } => {
            run_live(
                symbol,
                trades_binding,
                books_binding,
                params,
                ledger_dir,
                artifacts,
                queue_verdicts,
                poll_ms,
                port,
                grace_ms,
                cross_window_ms,
                idle_after_ms,
                traces_dir,
                max_traces_per_user,
                tape_retention_ms,
                kafka_brokers,
            )
            .await
        }
        Command::Serve {
            ledger_dir,
            artifacts,
            port,
            traces_dir,
            max_traces_per_user,
            tape_retention_ms,
            params,
        } => {
            let metrics = Metrics::shared();
            let traces_rt = traces_dir
                .map(|dir| {
                    traces::TraceRuntime::start(
                        dir,
                        params.clone(),
                        DEFAULT_GRACE_MS,
                        DEFAULT_CROSS_WINDOW_MS,
                        None,
                        None,
                        None,
                        max_traces_per_user,
                        tape_retention_ms,
                        metrics.clone(),
                    )
                })
                .transpose()?;
            metrics.set(
                "tracer_ledger_bytes",
                &[],
                metrics::dir_bytes(&ledger_dir) as f64,
            );
            metrics.set_flag("tracer_ready", &[], true);
            let state = AppState {
                status: Arc::new(RwLock::new(serde_json::json!({
                    "schema": "tracer.status.v1",
                    "mode": "serve",
                    "engine_version": tracer_engine::ENGINE_VERSION,
                }))),
                ledger_live: Arc::new(RwLock::new(LedgerLive::default())),
                ready: Arc::new(RwLock::new(Readiness::ready())),
                metrics,
                ledger_dir,
                artifacts_dir: artifacts,
                trades_binding: None,
                books_binding: None,
                symbol_stem: None,
                traces: traces_rt,
                params_path: Some(params),
                now_ms: wall_now_ms,
            };
            serve_api(state, port).await
        }
        Command::Replay {
            fills,
            params,
            out,
            grace_ms,
            cross_window_ms,
        } => run_replay(&fills, &params, &out, grace_ms, cross_window_ms),
        Command::ExportGolden { fills, params, out } => export_golden(&fills, &params, &out),
        Command::Evaluate {
            tape,
            orders,
            params,
            out,
            grace_ms,
            cross_window_ms,
            notional_usd,
            hold_ms,
            maker_fee_bps,
            taker_fee_bps,
            trades_binding,
            books_binding,
        } => {
            let overrides = (notional_usd.is_some()
                || hold_ms.is_some()
                || maker_fee_bps.is_some()
                || taker_fee_bps.is_some())
            .then_some(tracer_engine::trace::TraceOverrides {
                notional_usd,
                hold_ms,
                maker_fee_leg_bps: maker_fee_bps,
                taker_fee_leg_bps: taker_fee_bps,
            });
            run_evaluate(
                &tape,
                &orders,
                &params,
                &out,
                grace_ms,
                cross_window_ms,
                overrides,
                trades_binding.as_deref(),
                books_binding.as_deref(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;
    use tracer_engine::engine::StreamStatus;

    #[test]
    fn cli_definition_is_consistent() {
        super::Cli::command().debug_assert();
    }

    #[test]
    fn readiness_follows_the_last_tick_outcome() {
        let failure = anyhow::anyhow!("binding dir vanished");

        assert_eq!(readiness_after_tick(None, 0), Readiness::ready());
        let not_ready = readiness_after_tick(Some(&failure), 3);
        assert!(!not_ready.ready);
        assert_eq!(
            not_ready.reason,
            "tick failed 3 time(s) in a row: binding dir vanished"
        );
        assert_eq!(readiness_after_tick(None, 0), Readiness::ready());
    }

    #[test]
    fn record_tick_counts_results_and_mirrors_readiness() {
        let metrics = Metrics::shared();

        record_tick(&metrics, &Readiness::ready(), 0, 12);
        record_tick(&metrics, &Readiness::not_ready("x"), 1, 40);
        record_tick(&metrics, &Readiness::not_ready("x"), 2, 41);

        assert_eq!(metrics.counter("tracer_tick_total", &[("result", "ok")]), 1);
        assert_eq!(
            metrics.counter("tracer_tick_total", &[("result", "error")]),
            2
        );
        assert_eq!(
            metrics.gauge("tracer_tick_consecutive_errors", &[]),
            Some(2.0)
        );
        assert_eq!(
            metrics.gauge("tracer_tick_last_duration_ms", &[]),
            Some(41.0)
        );
        assert_eq!(metrics.gauge("tracer_ready", &[]), Some(0.0));
    }

    #[test]
    fn record_engine_status_exports_each_stream_and_the_engine_flags() {
        let metrics = Metrics::shared();
        let status = EngineStatus {
            trades: StreamStatus {
                watermark_ms: 1_000,
                files_read: 7,
                advance_age_ms: 2_500,
                stalled: true,
                ..StreamStatus::default()
            },
            books: None,
            merged_watermark_ms: 900,
            verdict_trail_ms: 100,
            open_trades: 2,
            events_written: 55,
            idle: false,
            binding_pending: true,
            ..EngineStatus::default()
        };

        record_engine_status(&metrics, &status);

        let trades = [("stream", "trades")];
        assert_eq!(
            metrics.gauge("tracer_stream_watermark_ms", &trades),
            Some(1_000.0)
        );
        assert_eq!(
            metrics.gauge("tracer_stream_files_read", &trades),
            Some(7.0)
        );
        assert_eq!(
            metrics.gauge("tracer_stream_advance_age_ms", &trades),
            Some(2_500.0)
        );
        assert_eq!(metrics.gauge("tracer_stream_stalled", &trades), Some(1.0));
        assert_eq!(
            metrics.gauge("tracer_stream_stalled", &[("stream", "books")]),
            None
        );
        assert_eq!(
            metrics.gauge("tracer_merged_watermark_ms", &[]),
            Some(900.0)
        );
        assert_eq!(metrics.gauge("tracer_verdict_trail_ms", &[]), Some(100.0));
        assert_eq!(metrics.gauge("tracer_open_trades", &[]), Some(2.0));
        assert_eq!(metrics.gauge("tracer_events_written", &[]), Some(55.0));
        assert_eq!(metrics.gauge("tracer_idle", &[]), Some(0.0));
        assert_eq!(metrics.gauge("tracer_binding_pending", &[]), Some(1.0));
    }
}
