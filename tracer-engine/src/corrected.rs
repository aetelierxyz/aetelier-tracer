use std::collections::BTreeMap;
use std::path::Path;

use crate::ledger::{
    EVENT_SCHEMA, Event, EventKind, LedgerWriter, Leg, Mode, PostState, Verdict, event_id, trade_id,
};
use crate::nyse::regime;
use crate::paper::ExecutedTrade;
use crate::params::TracerParams;
use crate::replay::SymbolData;
use crate::stats::{StatsBlock, block};
use crate::verdict::{
    DELTAS_MS, Fill, MARKOUT_WINDOWS_MS, MakerScan, MakerSide, first_opposite_print, markout_bps,
    scan_maker,
};

pub const DEFAULT_GRACE_MS: u64 = 300_000;
pub const DEFAULT_CROSS_WINDOW_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ExitOutcome {
    Maker(Fill),
    TakerCross(Fill),
    Unpriced,
}

pub struct TradeVerdicts {
    pub trade: ExecutedTrade,
    pub entry_regime: &'static str,
    pub entry: [MakerScan; 3],
    pub exit: Option<MakerScan>,
    pub exit_outcome: Option<ExitOutcome>,
    pub markout: [Option<f64>; 2],
    pub corrected: Option<ExecutedTrade>,
}

pub fn evaluate(
    trades: &[ExecutedTrade],
    data: &[SymbolData],
    params: &TracerParams,
    grace_ms: u64,
    cross_window_ms: u64,
) -> anyhow::Result<Vec<TradeVerdicts>> {
    let prints: BTreeMap<&str, &[crate::bars::FillRow]> = data
        .iter()
        .map(|d| (d.stem.as_str(), d.prints.as_slice()))
        .collect();
    let mut out = Vec::with_capacity(trades.len());
    for t in trades {
        let prints = *prints
            .get(t.symbol.as_str())
            .ok_or_else(|| anyhow::anyhow!("{}: no prints loaded", t.symbol))?;
        let entry_side = MakerSide::entry_for_signal_side(t.side);
        let window_end = t.t_entry_ms + params.hold_ms;
        let entry: [MakerScan; 3] = DELTAS_MS.map(|delta| {
            scan_maker(
                prints,
                entry_side,
                t.entry_px,
                t.t_entry_ms,
                delta,
                window_end,
                Some(t.entry_last_px),
            )
        });

        let mut exit = None;
        let mut exit_outcome = None;
        let mut markout = [None, None];
        let mut corrected = None;
        if let Some(entry_fill) = entry[0].through {
            let posted_fill = Fill {
                ts_ms: entry_fill.ts_ms,
                px: t.entry_px,
            };
            markout = MARKOUT_WINDOWS_MS.map(|w| markout_bps(prints, posted_fill, w, t.side));
            let exit_side = MakerSide::exit_for_signal_side(t.side);
            let grace_end = t.t_exit_ms + grace_ms;
            let scan = scan_maker(
                prints,
                exit_side,
                t.exit_px,
                t.t_exit_ms,
                0,
                grace_end,
                None,
            );
            let outcome = if let Some(f) = scan.through {
                ExitOutcome::Maker(Fill {
                    ts_ms: f.ts_ms,
                    px: t.exit_px,
                })
            } else if let Some(f) =
                first_opposite_print(prints, exit_side, grace_end, grace_end + cross_window_ms)
            {
                ExitOutcome::TakerCross(f)
            } else {
                ExitOutcome::Unpriced
            };
            let sym = params.symbol(&t.symbol)?;
            corrected = match outcome {
                ExitOutcome::Maker(f) => Some(corrected_trade(t, params, f, t.fee_exit_usd)),
                ExitOutcome::TakerCross(f) => Some(corrected_trade(
                    t,
                    params,
                    f,
                    params.notional_usd * sym.taker_fee_leg_bps / 1e4,
                )),
                ExitOutcome::Unpriced => None,
            };
            exit = Some(scan);
            exit_outcome = Some(outcome);
        }
        out.push(TradeVerdicts {
            entry_regime: regime(t.t_entry_ms).label(),
            trade: t.clone(),
            entry,
            exit,
            exit_outcome,
            markout,
            corrected,
        });
    }
    Ok(out)
}

fn corrected_trade(
    t: &ExecutedTrade,
    params: &TracerParams,
    exit_fill: Fill,
    fee_exit_usd: f64,
) -> ExecutedTrade {
    let gross = t.side as f64 * (exit_fill.px / t.entry_px - 1.0) * params.notional_usd;
    ExecutedTrade {
        t_exit_ms: exit_fill.ts_ms,
        t_exit_real_ms: exit_fill.ts_ms,
        exit_px: exit_fill.px,
        fee_exit_usd,
        pnl_usd: gross - t.fee_entry_usd - fee_exit_usd,
        ..t.clone()
    }
}

fn finite(x: f64) -> Option<f64> {
    x.is_finite().then_some(x)
}

#[derive(serde::Serialize)]
pub struct StatsJson {
    n: usize,
    longs: usize,
    total: f64,
    win_rate: f64,
    profit_factor: Option<f64>,
    payoff: Option<f64>,
    mean_usd: f64,
    t_stat: Option<f64>,
    p05: f64,
    p50: f64,
    p95: f64,
    max_profit: f64,
    max_loss: f64,
    max_dd: f64,
    underwater_days: f64,
    trades_per_hour: f64,
    max_consec_losses: usize,
    fees_total: f64,
    sharpe: Option<f64>,
    sortino: Option<f64>,
    calmar: Option<f64>,
}

impl From<&StatsBlock> for StatsJson {
    fn from(b: &StatsBlock) -> Self {
        StatsJson {
            n: b.n,
            longs: b.longs,
            total: b.total,
            win_rate: b.win_rate,
            profit_factor: finite(b.profit_factor),
            payoff: finite(b.payoff),
            mean_usd: b.mean_usd,
            t_stat: finite(b.t_stat),
            p05: b.p05,
            p50: b.p50,
            p95: b.p95,
            max_profit: b.max_profit,
            max_loss: b.max_loss,
            max_dd: b.max_dd,
            underwater_days: b.underwater_days,
            trades_per_hour: b.trades_per_hour,
            max_consec_losses: b.max_consec_losses,
            fees_total: b.fees_total,
            sharpe: finite(b.sharpe),
            sortino: finite(b.sortino),
            calmar: finite(b.calmar),
        }
    }
}

fn stats_of(trades: &[&ExecutedTrade]) -> Option<StatsJson> {
    block(trades).map(|b| StatsJson::from(&b))
}

fn equity(trades: &[&ExecutedTrade]) -> Vec<(u64, f64)> {
    let mut sorted: Vec<&ExecutedTrade> = trades.to_vec();
    sorted.sort_by(|a, b| {
        (a.t_exit_ms, &a.symbol, a.t_entry_ms, a.side).cmp(&(
            b.t_exit_ms,
            &b.symbol,
            b.t_entry_ms,
            b.side,
        ))
    });
    let mut cum = 0.0;
    sorted
        .iter()
        .map(|t| {
            cum += t.pnl_usd;
            (t.t_exit_ms, cum)
        })
        .collect()
}

#[derive(serde::Serialize)]
struct LedgerSide {
    n: usize,
    stats: Option<StatsJson>,
    equity: Vec<(u64, f64)>,
}

#[derive(serde::Serialize)]
struct CorrectedSide {
    n_kept: usize,
    dropped_entry_unfilled: usize,
    exit_maker: usize,
    exit_taker_cross: usize,
    exit_unpriced: usize,
    stats: Option<StatsJson>,
    equity: Vec<(u64, f64)>,
}

#[derive(serde::Serialize)]
struct RegimeSplit {
    regime: &'static str,
    paper: Option<StatsJson>,
    corrected: Option<StatsJson>,
}

#[derive(serde::Serialize)]
struct CorrectedLedgerJson<'a> {
    schema: &'static str,
    mode: &'static str,
    params_version: u32,
    engine_version: &'static str,
    input_sha256: &'a BTreeMap<String, String>,
    grace_ms: u64,
    cross_window_ms: u64,
    paper: LedgerSide,
    corrected: CorrectedSide,
    per_regime: Vec<RegimeSplit>,
}

#[derive(serde::Serialize)]
struct PctJson {
    p50: u64,
    p90: u64,
    n: usize,
}

#[derive(serde::Serialize)]
struct MeanJson {
    mean: f64,
    n: usize,
}

#[derive(serde::Serialize)]
struct DeltaCell {
    delta_ms: u64,
    already_through: usize,
    filled_touch: usize,
    filled_through: usize,
}

#[derive(serde::Serialize)]
struct EntryCell {
    leg: &'static str,
    symbol: String,
    regime: &'static str,
    posted: usize,
    gapped: usize,
    per_delta: Vec<DeltaCell>,
    time_to_fill_ms: BTreeMap<&'static str, Option<PctJson>>,
    markout_bps: BTreeMap<&'static str, Option<MeanJson>>,
}

#[derive(serde::Serialize)]
struct ExitCell {
    leg: &'static str,
    symbol: String,
    regime: &'static str,
    posted: usize,
    gapped: usize,
    filled_touch: usize,
    filled_through: usize,
    taker_cross: usize,
    exit_unpriced: usize,
    time_to_fill_ms: BTreeMap<&'static str, Option<PctJson>>,
}

#[derive(serde::Serialize)]
struct FillratesJson<'a> {
    schema: &'static str,
    mode: &'static str,
    params_version: u32,
    engine_version: &'static str,
    input_sha256: &'a BTreeMap<String, String>,
    denominator: &'static str,
    deltas_ms: [u64; 3],
    entry_cells: Vec<EntryCell>,
    exit_cells: Vec<ExitCell>,
}

fn pct_u64(sorted: &[u64], q: f64) -> u64 {
    sorted[((sorted.len() as f64 - 1.0) * q).round() as usize]
}

fn pct_json(mut v: Vec<u64>) -> Option<PctJson> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    Some(PctJson {
        p50: pct_u64(&v, 0.50),
        p90: pct_u64(&v, 0.90),
        n: v.len(),
    })
}

fn mean_json(v: &[f64]) -> Option<MeanJson> {
    if v.is_empty() {
        return None;
    }
    Some(MeanJson {
        mean: v.iter().sum::<f64>() / v.len() as f64,
        n: v.len(),
    })
}

#[derive(Default)]
struct EntryAgg {
    posted: usize,
    per_delta: [(usize, usize, usize); 3],
    ttf_touch: Vec<u64>,
    ttf_through: Vec<u64>,
    m1: Vec<f64>,
    m5: Vec<f64>,
}

#[derive(Default)]
struct ExitAgg {
    posted: usize,
    filled_touch: usize,
    filled_through: usize,
    taker_cross: usize,
    exit_unpriced: usize,
    ttf_through: Vec<u64>,
}

pub fn write_artifacts(
    verdicts: &[TradeVerdicts],
    params: &TracerParams,
    grace_ms: u64,
    cross_window_ms: u64,
    out_dir: &Path,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(out_dir)?;

    let paper_refs: Vec<&ExecutedTrade> = verdicts.iter().map(|v| &v.trade).collect();
    let kept: Vec<&ExecutedTrade> = verdicts
        .iter()
        .filter_map(|v| v.corrected.as_ref())
        .collect();
    let n_entry_filled = verdicts.iter().filter(|v| v.exit_outcome.is_some()).count();
    let count_outcome = |f: fn(&ExitOutcome) -> bool| {
        verdicts
            .iter()
            .filter(|v| v.exit_outcome.as_ref().is_some_and(f))
            .count()
    };

    let mut per_regime = Vec::new();
    for reg in ["RTH", "EXT", "WKND"] {
        let p: Vec<&ExecutedTrade> = verdicts
            .iter()
            .filter(|v| v.entry_regime == reg)
            .map(|v| &v.trade)
            .collect();
        let c: Vec<&ExecutedTrade> = verdicts
            .iter()
            .filter(|v| v.entry_regime == reg)
            .filter_map(|v| v.corrected.as_ref())
            .collect();
        per_regime.push(RegimeSplit {
            regime: match reg {
                "RTH" => "RTH",
                "EXT" => "EXT",
                _ => "WKND",
            },
            paper: stats_of(&p),
            corrected: stats_of(&c),
        });
    }

    let ledger = CorrectedLedgerJson {
        schema: "tracer.corrected.v1",
        mode: "replay",
        params_version: params.params_version,
        engine_version: crate::ENGINE_VERSION,
        input_sha256: &params.input_sha256,
        grace_ms,
        cross_window_ms,
        paper: LedgerSide {
            n: paper_refs.len(),
            stats: stats_of(&paper_refs),
            equity: equity(&paper_refs),
        },
        corrected: CorrectedSide {
            n_kept: kept.len(),
            dropped_entry_unfilled: verdicts.len() - n_entry_filled,
            exit_maker: count_outcome(|o| matches!(o, ExitOutcome::Maker(_))),
            exit_taker_cross: count_outcome(|o| matches!(o, ExitOutcome::TakerCross(_))),
            exit_unpriced: count_outcome(|o| matches!(o, ExitOutcome::Unpriced)),
            stats: stats_of(&kept),
            equity: equity(&kept),
        },
        per_regime,
    };
    std::fs::write(
        out_dir.join("corrected_ledger.json"),
        serde_json::to_string_pretty(&ledger)? + "\n",
    )?;

    let mut entry_aggs: BTreeMap<(String, &'static str), EntryAgg> = BTreeMap::new();
    let mut exit_aggs: BTreeMap<(String, &'static str), ExitAgg> = BTreeMap::new();
    for v in verdicts {
        let key = (v.trade.symbol.clone(), v.entry_regime);
        let e = entry_aggs.entry(key.clone()).or_default();
        e.posted += 1;
        for (i, scan) in v.entry.iter().enumerate() {
            let slot = &mut e.per_delta[i];
            slot.0 += scan.already_through as usize;
            slot.1 += scan.touch.is_some() as usize;
            slot.2 += scan.through.is_some() as usize;
        }
        if let Some(f) = v.entry[0].touch {
            e.ttf_touch.push(f.ts_ms - v.trade.t_entry_ms);
        }
        if let Some(f) = v.entry[0].through {
            e.ttf_through.push(f.ts_ms - v.trade.t_entry_ms);
        }
        if let Some(m) = v.markout[0] {
            e.m1.push(m);
        }
        if let Some(m) = v.markout[1] {
            e.m5.push(m);
        }
        if let (Some(scan), Some(outcome)) = (&v.exit, &v.exit_outcome) {
            let x = exit_aggs.entry(key).or_default();
            x.posted += 1;
            x.filled_touch += scan.touch.is_some() as usize;
            x.filled_through += scan.through.is_some() as usize;
            x.taker_cross += matches!(outcome, ExitOutcome::TakerCross(_)) as usize;
            x.exit_unpriced += matches!(outcome, ExitOutcome::Unpriced) as usize;
            if let Some(f) = scan.through {
                x.ttf_through.push(f.ts_ms - v.trade.t_exit_ms);
            }
        }
    }

    let entry_cells = entry_aggs
        .into_iter()
        .map(|((symbol, regime), a)| EntryCell {
            leg: "entry",
            symbol,
            regime,
            posted: a.posted,
            gapped: 0,
            per_delta: DELTAS_MS
                .iter()
                .zip(a.per_delta.iter())
                .map(|(d, (at, ft, fth))| DeltaCell {
                    delta_ms: *d,
                    already_through: *at,
                    filled_touch: *ft,
                    filled_through: *fth,
                })
                .collect(),
            time_to_fill_ms: BTreeMap::from([
                ("touch", pct_json(a.ttf_touch)),
                ("through", pct_json(a.ttf_through)),
            ]),
            markout_bps: BTreeMap::from([("m1", mean_json(&a.m1)), ("m5", mean_json(&a.m5))]),
        })
        .collect();
    let exit_cells = exit_aggs
        .into_iter()
        .map(|((symbol, regime), a)| ExitCell {
            leg: "exit",
            symbol,
            regime,
            posted: a.posted,
            gapped: 0,
            filled_touch: a.filled_touch,
            filled_through: a.filled_through,
            taker_cross: a.taker_cross,
            exit_unpriced: a.exit_unpriced,
            time_to_fill_ms: BTreeMap::from([("through", pct_json(a.ttf_through))]),
        })
        .collect();

    let fillrates = FillratesJson {
        schema: "tracer.fillrates.v1",
        mode: "replay",
        params_version: params.params_version,
        engine_version: crate::ENGINE_VERSION,
        input_sha256: &params.input_sha256,
        denominator: "paper_trades",
        deltas_ms: DELTAS_MS,
        entry_cells,
        exit_cells,
    };
    std::fs::write(
        out_dir.join("fillrates.json"),
        serde_json::to_string_pretty(&fillrates)? + "\n",
    )?;
    Ok(())
}

pub fn emit_events(
    verdicts: &[TradeVerdicts],
    params: &TracerParams,
    grace_ms: u64,
    writer: &mut LedgerWriter,
) -> anyhow::Result<()> {
    for v in verdicts {
        let t = &v.trade;
        let tid = trade_id(&t.symbol, t.t_entry_ms, t.side);
        let mut seq: u32 = 0;
        let base = |seq: u32, kind, leg, ts_ms, posted_px, window_end_ms| Event {
            schema: EVENT_SCHEMA.into(),
            event_id: event_id(&tid, seq),
            trade_id: tid.clone(),
            seq,
            kind,
            leg,
            mode: Mode::Replay,
            ts_ms,
            wall_ts_ms: ts_ms,
            engine_version: crate::ENGINE_VERSION.into(),
            params_version: params.params_version,
            symbol: t.symbol.clone(),
            signal_side: t.side,
            maker_side: match leg {
                Leg::Entry => MakerSide::entry_for_signal_side(t.side).label().into(),
                Leg::Exit => MakerSide::exit_for_signal_side(t.side).label().into(),
            },
            decile: t.decile,
            score: t.score,
            regime: regime(ts_ms).label().into(),
            posted_px,
            qty_usd: params.notional_usd,
            window_end_ms,
            post_state: None,
            depth_ahead_usd: None,
            book_ts_ms: None,
            book_staleness_ms: None,
            exit_price_stale: None,
            milestone: None,
            evidence_px: None,
            cum_opp_usd: None,
            filled_touch_ts_ms: None,
            filled_through_ts_ms: None,
            filled_queue_ts_ms: None,
            verdict: None,
            cross_px: None,
            cross_kind: None,
            taker_fee_usd: None,
            gap_flags: vec![],
        };

        let entry_window = t.t_entry_ms + params.hold_ms;
        let scan = &v.entry[0];
        let mut posted = base(
            seq,
            EventKind::Posted,
            Leg::Entry,
            t.t_entry_ms,
            t.entry_px,
            entry_window,
        );
        posted.post_state = Some(if scan.already_through {
            PostState::AlreadyThrough
        } else {
            PostState::Passive
        });
        writer.append(&posted)?;
        seq += 1;
        if let Some(f) = scan.touch {
            let mut obs = base(
                seq,
                EventKind::Observed,
                Leg::Entry,
                f.ts_ms,
                t.entry_px,
                entry_window,
            );
            obs.milestone = Some("touch".into());
            obs.evidence_px = Some(f.px);
            writer.append(&obs)?;
            seq += 1;
        }
        let mut terminal = if let Some(f) = scan.through {
            let mut e = base(
                seq,
                EventKind::Filled,
                Leg::Entry,
                f.ts_ms,
                t.entry_px,
                entry_window,
            );
            e.milestone = Some("through".into());
            e.evidence_px = Some(f.px);
            e.verdict = Some(Verdict::FilledThrough);
            e
        } else {
            let mut e = base(
                seq,
                EventKind::Expired,
                Leg::Entry,
                entry_window,
                t.entry_px,
                entry_window,
            );
            e.verdict = Some(Verdict::Unfilled);
            e
        };
        terminal.filled_touch_ts_ms = scan.touch.map(|f| f.ts_ms);
        terminal.filled_through_ts_ms = scan.through.map(|f| f.ts_ms);
        writer.append(&terminal)?;
        seq += 1;

        if let (Some(scan), Some(outcome)) = (&v.exit, &v.exit_outcome) {
            let grace_end = t.t_exit_ms + grace_ms;
            let mut posted = base(
                seq,
                EventKind::Posted,
                Leg::Exit,
                t.t_exit_ms,
                t.exit_px,
                grace_end,
            );
            posted.post_state = Some(if scan.already_through {
                PostState::AlreadyThrough
            } else {
                PostState::Passive
            });
            posted.exit_price_stale = Some(t.t_exit_real_ms < t.t_exit_ms);
            writer.append(&posted)?;
            seq += 1;
            if let Some(f) = scan.touch {
                let mut obs = base(
                    seq,
                    EventKind::Observed,
                    Leg::Exit,
                    f.ts_ms,
                    t.exit_px,
                    grace_end,
                );
                obs.milestone = Some("touch".into());
                obs.evidence_px = Some(f.px);
                writer.append(&obs)?;
                seq += 1;
            }
            let mut terminal = if let Some(f) = scan.through {
                let mut e = base(
                    seq,
                    EventKind::Filled,
                    Leg::Exit,
                    f.ts_ms,
                    t.exit_px,
                    grace_end,
                );
                e.milestone = Some("through".into());
                e.evidence_px = Some(f.px);
                e.verdict = Some(Verdict::FilledThrough);
                e
            } else {
                let mut e = base(
                    seq,
                    EventKind::Expired,
                    Leg::Exit,
                    grace_end,
                    t.exit_px,
                    grace_end,
                );
                e.verdict = Some(Verdict::Unfilled);
                e
            };
            terminal.filled_touch_ts_ms = scan.touch.map(|f| f.ts_ms);
            terminal.filled_through_ts_ms = scan.through.map(|f| f.ts_ms);
            writer.append(&terminal)?;
            seq += 1;
            if let ExitOutcome::TakerCross(f) = outcome {
                let sym = params.symbol(&t.symbol)?;
                let mut e = base(
                    seq,
                    EventKind::TakerCross,
                    Leg::Exit,
                    f.ts_ms,
                    t.exit_px,
                    grace_end,
                );
                e.cross_px = Some(f.px);
                e.cross_kind = Some("opposite_print".into());
                e.taker_fee_usd = Some(params.notional_usd * sym.taker_fee_leg_bps / 1e4);
                writer.append(&e)?;
            }
        }
    }
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::FillRow;
    use crate::replay::SymbolData;

    fn print(ts: u64, px: f64, is_buy: bool) -> FillRow {
        FillRow {
            time_ms: ts,
            px,
            sz: 1.0,
            is_buy,
        }
    }

    fn trade(t_entry_ms: u64, side: i8, entry_px: f64, exit_px: f64) -> ExecutedTrade {
        ExecutedTrade {
            symbol: "xyz_TSLA".into(),
            side,
            score: 0.9 * side as f64,
            decile: if side > 0 { 10 } else { 1 },
            t_entry_ms,
            entry_px,
            entry_last_px: entry_px + 0.1 * side as f64,
            t_exit_ms: t_entry_ms + 900_000,
            t_exit_real_ms: t_entry_ms + 900_000,
            exit_px,
            fee_entry_usd: -0.0015,
            fee_exit_usd: -0.0015,
            pnl_usd: 0.0,
        }
    }

    fn fixture() -> (Vec<ExecutedTrade>, Vec<SymbolData>, TracerParams) {
        let params = TracerParams::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../params/tracer_params.v1.json"),
        )
        .unwrap();
        let prints = vec![
            print(1_060_000, 99.9, false),
            print(1_100_000, 100.5, true),
            print(2_000_000, 101.2, true),
            print(10_050_000, 100.0, false),
            print(20_100_000, 100.2, true),
            print(21_210_000, 99.4, false),
            print(30_050_000, 99.0, false),
        ];
        let trades = vec![
            trade(1_000_000, 1, 100.0, 101.0),
            trade(10_000_000, 1, 100.0, 101.0),
            trade(20_000_000, -1, 100.0, 99.0),
            trade(30_000_000, 1, 100.0, 100.5),
        ];
        let data = vec![SymbolData {
            stem: "xyz_TSLA".into(),
            prints,
            bars: vec![],
        }];
        (trades, data, params)
    }

    #[test]
    fn exit_outcome_permutations_and_corrected_pnl() {
        let (trades, data, params) = fixture();
        let v = evaluate(
            &trades,
            &data,
            &params,
            DEFAULT_GRACE_MS,
            DEFAULT_CROSS_WINDOW_MS,
        )
        .unwrap();

        assert!(matches!(v[0].exit_outcome, Some(ExitOutcome::Maker(_))));
        let a = v[0].corrected.as_ref().unwrap();
        assert_eq!(a.t_exit_ms, 2_000_000);
        assert_eq!(a.exit_px, 101.0);
        assert!((a.pnl_usd - 10.003).abs() < 1e-9);
        assert!((v[0].markout[0].unwrap() - 50.0).abs() < 1e-6);

        assert!(v[1].exit_outcome.is_none());
        assert!(v[1].corrected.is_none());
        assert!(v[1].entry[0].touch.is_some());
        assert!(v[1].entry[0].through.is_none());

        assert!(matches!(
            v[2].exit_outcome,
            Some(ExitOutcome::TakerCross(_))
        ));
        let c = v[2].corrected.as_ref().unwrap();
        assert_eq!(c.t_exit_ms, 21_210_000);
        assert_eq!(c.exit_px, 99.4);
        assert!((c.fee_exit_usd - 0.1044).abs() < 1e-12);
        assert!((c.pnl_usd - (6.0 + 0.0015 - 0.1044)).abs() < 1e-9);

        assert!(matches!(v[3].exit_outcome, Some(ExitOutcome::Unpriced)));
        assert!(v[3].corrected.is_none());
    }

    #[test]
    fn artifacts_serialize_with_nulls_for_non_finite() {
        let (trades, data, params) = fixture();
        let v = evaluate(
            &trades,
            &data,
            &params,
            DEFAULT_GRACE_MS,
            DEFAULT_CROSS_WINDOW_MS,
        )
        .unwrap();
        let out = std::env::temp_dir().join("tracer_corrected_artifacts_test");
        let _ = std::fs::remove_dir_all(&out);
        write_artifacts(&v, &params, DEFAULT_GRACE_MS, DEFAULT_CROSS_WINDOW_MS, &out).unwrap();

        let ledger: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(out.join("corrected_ledger.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(ledger["schema"], "tracer.corrected.v1");
        assert_eq!(ledger["paper"]["n"], 4);
        assert_eq!(ledger["corrected"]["n_kept"], 2);
        assert_eq!(ledger["corrected"]["dropped_entry_unfilled"], 1);
        assert_eq!(ledger["corrected"]["exit_maker"], 1);
        assert_eq!(ledger["corrected"]["exit_taker_cross"], 1);
        assert_eq!(ledger["corrected"]["exit_unpriced"], 1);
        assert!(ledger["corrected"]["stats"]["profit_factor"].is_null());

        let fillrates: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("fillrates.json")).unwrap())
                .unwrap();
        assert_eq!(fillrates["schema"], "tracer.fillrates.v1");
        let posted: u64 = fillrates["entry_cells"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["posted"].as_u64().unwrap())
            .sum();
        assert_eq!(posted, 4);

        let mut writer = LedgerWriter::new(&out.join("ledger")).unwrap();
        emit_events(&v, &params, DEFAULT_GRACE_MS, &mut writer).unwrap();
        assert!(writer.events_written >= 4 * 2);
        std::fs::remove_dir_all(&out).unwrap();
    }
}
