use std::path::Path;

use crate::bars::{Bar, FillRow, build_bars, quantize_bars, read_fills_csv};
use crate::paper::{ExecutedTrade, run_symbol};
use crate::params::TracerParams;

pub const MIN_BARS_PER_SYMBOL: usize = 200;

pub struct SymbolData {
    pub stem: String,
    pub trades: Vec<FillRow>,
    pub bars: Vec<Bar>,
}

pub fn load_symbol_data(
    fills_dir: &Path,
    params: &TracerParams,
) -> anyhow::Result<Vec<SymbolData>> {
    let mut out = Vec::new();
    let mut symbols = params.symbols.clone();
    symbols.sort_by(|a, b| a.stem.cmp(&b.stem));
    for sym in &symbols {
        let path = fills_dir.join(format!("{}.csv", sym.stem));
        let trades = read_fills_csv(&path)?;
        anyhow::ensure!(!trades.is_empty(), "{}: no fills", path.display());
        let mut bars = build_bars(&trades, sym.bar_threshold_usd);
        quantize_bars(&mut bars);
        if bars.len() < MIN_BARS_PER_SYMBOL {
            continue;
        }
        out.push(SymbolData {
            stem: sym.stem.clone(),
            trades,
            bars,
        });
    }
    anyhow::ensure!(
        !out.is_empty(),
        "no symbol reached {MIN_BARS_PER_SYMBOL} bars"
    );
    Ok(out)
}

pub fn paper_trades_from(
    data: &[SymbolData],
    params: &TracerParams,
) -> anyhow::Result<Vec<ExecutedTrade>> {
    let mut all: Vec<ExecutedTrade> = Vec::new();
    for d in data {
        let sym = params.symbol(&d.stem)?;
        all.extend(run_symbol(
            &d.stem,
            &d.bars,
            &params.decile_thresholds,
            params.tau_secs,
            params.hold_ms,
            params.exit_staleness_ms,
            params.notional_usd,
            sym.maker_fee_leg_bps,
        ));
    }
    all.sort_by(|a, b| {
        (a.t_exit_ms, &a.symbol, a.t_entry_ms, a.side).cmp(&(
            b.t_exit_ms,
            &b.symbol,
            b.t_entry_ms,
            b.side,
        ))
    });
    Ok(all)
}

pub fn paper_trades(fills_dir: &Path, params: &TracerParams) -> anyhow::Result<Vec<ExecutedTrade>> {
    let data = load_symbol_data(fills_dir, params)?;
    paper_trades_from(&data, params)
}
