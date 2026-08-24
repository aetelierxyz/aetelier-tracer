use std::path::PathBuf;

use clap::{Parser, Subcommand};

use tracer_engine::corrected::{
    DEFAULT_CROSS_WINDOW_MS, DEFAULT_GRACE_MS, emit_events, evaluate, write_artifacts,
};
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
    Run,
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
    Serve,
    ExportGolden {
        #[arg(long, env = "TRACER_FILLS_DIR")]
        fills: PathBuf,
        #[arg(long, default_value = "./params/tracer_params.v1.json")]
        params: PathBuf,
        #[arg(long, default_value = "./out/paper_trades_golden.csv")]
        out: PathBuf,
    },
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

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Run => anyhow::bail!("run is not implemented"),
        Command::Serve => anyhow::bail!("serve is not implemented"),
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
