mod api;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use clap::{Parser, Subcommand};

use api::{AppState, CorrectedLive, origins_from_env, router, wall_now_ms};
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
    },
    ExportGolden {
        #[arg(long, env = "TRACER_FILLS_DIR")]
        fills: PathBuf,
        #[arg(long, default_value = "./params/tracer_params.v1.json")]
        params: PathBuf,
        #[arg(long, default_value = "./out/paper_trades_golden.csv")]
        out: PathBuf,
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
    queue_verdicts: bool,
    poll_ms: u64,
    port: u16,
    grace_ms: u64,
    cross_window_ms: u64,
    idle_after_ms: u64,
) -> anyhow::Result<()> {
    let params = TracerParams::load(&params_path)?;
    let stem = symbol.replace(':', "_");
    let cfg = LiveConfig {
        symbol_stem: stem,
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
        corrected: Arc::new(RwLock::new(CorrectedLive::default())),
        ready: Arc::new(RwLock::new(false)),
        ledger_dir,
        artifacts_dir: None,
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
            *publish.status.write().unwrap() =
                serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
            *publish.corrected.write().unwrap() = CorrectedLive {
                n_kept: engine.corrected_points.len(),
                total_usd: engine
                    .corrected_points
                    .last()
                    .map(|(_, c)| *c)
                    .unwrap_or(0.0),
                equity: engine.corrected_points.clone(),
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
            queue_verdicts,
            poll_ms,
            port,
            grace_ms,
            cross_window_ms,
            idle_after_ms,
        } => {
            run_live(
                symbol,
                trades_binding,
                books_binding,
                params,
                ledger_dir,
                queue_verdicts,
                poll_ms,
                port,
                grace_ms,
                cross_window_ms,
                idle_after_ms,
            )
            .await
        }
        Command::Serve {
            ledger_dir,
            artifacts,
            port,
        } => {
            let state = AppState {
                status: Arc::new(RwLock::new(serde_json::json!({
                    "schema": "tracer.status.v1",
                    "mode": "serve",
                    "engine_version": tracer_engine::ENGINE_VERSION,
                }))),
                corrected: Arc::new(RwLock::new(CorrectedLive::default())),
                ready: Arc::new(RwLock::new(true)),
                ledger_dir,
                artifacts_dir: artifacts,
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
