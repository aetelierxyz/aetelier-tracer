mod api;
mod claims;
mod traces;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use clap::{Parser, Subcommand};

use api::{AppState, LedgerLive, origins_from_env, router, wall_now_ms};
use tracer_engine::corrected::{
    DEFAULT_CROSS_WINDOW_MS, DEFAULT_GRACE_MS, emit_events, evaluate, write_artifacts,
};
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
    println!("tracerd api listening on :{port}");
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
    kafka_brokers: Option<String>,
) -> anyhow::Result<()> {
    let params = TracerParams::load(&params_path)?;
    let stem = symbol.replace(':', "_");
    let claim_publisher = claims::ClaimPublisher::start(
        kafka_brokers.as_deref(),
        &trades_binding,
        books_binding.as_deref(),
        &stem,
        poll_ms,
    );
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
        ready: Arc::new(RwLock::new(false)),
        ledger_dir,
        artifacts_dir,
        trades_binding: Some(trades_binding.clone()),
        books_binding: books_binding.clone(),
        symbol_stem: Some(stem),
        traces: traces_rt,
        now_ms: wall_now_ms,
    };
    let publish = state.clone();
    std::thread::spawn(move || {
        loop {
            let wall = wall_now_ms();
            if let Err(e) = engine.tick(wall) {
                eprintln!("tick error: {e:#}");
            }
            let status = engine.status(wall_now_ms());
            // claim the collector bindings ONLY while a live
            // trace is actively running on a healthy pair. An idle / paused /
            // half-deleted pair announces nothing, its overseer claim ages
            // out, and the collector becomes deletable — the delete guard
            // then blocks only a genuinely active trace.
            if let Some(publisher) = &claim_publisher {
                publisher.announce_if_active(wall, &status);
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
            *publish.ready.write().unwrap() = true;
            std::thread::sleep(std::time::Duration::from_millis(poll_ms));
        }
    });
    serve_api(state, port).await
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
                kafka_brokers,
            )
            .await
        }
        Command::Serve {
            ledger_dir,
            artifacts,
            port,
            traces_dir,
            params,
        } => {
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
                    )
                })
                .transpose()?;
            let state = AppState {
                status: Arc::new(RwLock::new(serde_json::json!({
                    "schema": "tracer.status.v1",
                    "mode": "serve",
                    "engine_version": tracer_engine::ENGINE_VERSION,
                }))),
                ledger_live: Arc::new(RwLock::new(LedgerLive::default())),
                ready: Arc::new(RwLock::new(true)),
                ledger_dir,
                artifacts_dir: artifacts,
                trades_binding: None,
                books_binding: None,
                symbol_stem: None,
                traces: traces_rt,
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

    #[test]
    fn cli_definition_is_consistent() {
        super::Cli::command().debug_assert();
    }
}
