use crate::bars::Bar;
use crate::deciles::{decile_of, decile_thresholds};
use crate::signal::ewma_imbalance;

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutedTrade {
    pub symbol: String,
    pub side: i8,
    pub score: f64,
    pub decile: usize,
    pub t_entry_ms: u64,
    pub entry_px: f64,
    pub entry_last_px: f64,
    pub t_exit_ms: u64,
    pub t_exit_real_ms: u64,
    pub exit_px: f64,
    pub fee_entry_usd: f64,
    pub fee_exit_usd: f64,
    pub pnl_usd: f64,
}

pub fn pooled_thresholds(all: &[(String, Vec<Bar>)], t_split_ms: u64, tau_secs: f64) -> Vec<f64> {
    let mut fit_scores = Vec::new();
    for (_, bars) in all {
        let scores = ewma_imbalance(bars, tau_secs);
        for (b, s) in bars.iter().zip(scores.iter()) {
            if b.close_ms < t_split_ms {
                fit_scores.push(s.score);
            }
        }
    }
    decile_thresholds(&mut fit_scores)
}

#[allow(clippy::too_many_arguments)]
pub fn run_symbol(
    symbol: &str,
    bars: &[Bar],
    thresholds: &[f64],
    tau_secs: f64,
    hold_ms: u64,
    exit_staleness_ms: u64,
    notional_usd: f64,
    maker_fee_leg_bps: f64,
) -> Vec<ExecutedTrade> {
    let scores = ewma_imbalance(bars, tau_secs);
    let prices: Vec<(u64, f64)> = bars.iter().map(|b| (b.close_ms, b.vwap)).collect();
    let mut out = Vec::new();
    let mut prev_extreme = false;
    for (i, b) in bars.iter().enumerate() {
        let d = decile_of(thresholds, scores[i].score);
        let extreme = d == 0 || d == 9;
        if extreme && !prev_extreme {
            let side: i8 = if d == 9 { 1 } else { -1 };
            let t_exit = b.close_ms + hold_ms;
            let idx = prices.partition_point(|(t, _)| *t <= t_exit);
            if idx > 0 && b.vwap > 0.0 {
                let (exit_ts, exit_px) = prices[idx - 1];
                if t_exit.saturating_sub(exit_ts) <= exit_staleness_ms {
                    let gross = side as f64 * (exit_px / b.vwap - 1.0) * notional_usd;
                    let fee_entry = notional_usd * maker_fee_leg_bps / 1e4;
                    let fee_exit = notional_usd * maker_fee_leg_bps / 1e4;
                    out.push(ExecutedTrade {
                        symbol: symbol.to_string(),
                        side,
                        score: scores[i].score,
                        decile: d + 1,
                        t_entry_ms: b.close_ms,
                        entry_px: b.vwap,
                        entry_last_px: b.last_px,
                        t_exit_ms: t_exit,
                        t_exit_real_ms: exit_ts,
                        exit_px,
                        fee_entry_usd: fee_entry,
                        fee_exit_usd: fee_exit,
                        pnl_usd: gross - fee_entry - fee_exit,
                    });
                }
            }
        }
        prev_extreme = extreme;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Bar;

    const TAU: f64 = 60.0;
    const HOLD_MS: u64 = 15 * 60_000;
    const NOTIONAL: f64 = 1_000.0;

    fn bar(close_ms: u64, vwap: f64, buy: f64, sell: f64) -> Bar {
        Bar {
            open_ms: close_ms,
            close_ms,
            n: 1,
            buy_usd: buy,
            sell_usd: sell,
            vwap,
            last_px: vwap,
        }
    }

    #[test]
    fn long_on_top_decile_profits_when_price_rises_net_of_fees() {
        let mut bars = vec![bar(0, 100.0, 500.0, 500.0)];
        for k in 1..40u64 {
            let tilt = 20.0 * if k % 2 == 0 { 1.0 } else { -1.0 };
            bars.push(bar(k * 10_000, 100.0, 500.0 + tilt, 500.0 - tilt));
        }
        bars.push(bar(400_000, 100.0, 50_000.0, 0.0));
        bars.push(bar(400_000 + HOLD_MS, 101.0, 500.0, 500.0));
        let all = vec![("xyz_TSLA".to_string(), bars.clone())];
        let thresholds = pooled_thresholds(&all, 399_000, TAU);
        let trades = run_symbol(
            "xyz_TSLA",
            &bars,
            &thresholds,
            TAU,
            HOLD_MS,
            HOLD_MS / 4,
            NOTIONAL,
            -0.015,
        );
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.side, 1);
        assert!(
            t.pnl_usd > 9.9,
            "1% move on $1000 minus rebates, got {}",
            t.pnl_usd
        );
        assert!(t.fee_entry_usd < 0.0);
        assert_eq!(t.t_exit_ms, 400_000 + HOLD_MS);
        assert_eq!(t.t_exit_real_ms, 400_000 + HOLD_MS);
    }

    #[test]
    fn stale_exit_voids_the_crossing_and_real_exit_ts_is_kept() {
        let thresholds = vec![-0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8];
        let bars = vec![
            bar(0, 100.0, 500.0, 500.0),
            bar(10_000, 100.0, 500.0, 500.0),
            bar(400_000, 100.0, 50_000.0, 0.0),
            bar(400_000 + HOLD_MS - 210_000, 100.5, 500.0, 500.0),
        ];
        let trades = run_symbol(
            "xyz_TSLA",
            &bars,
            &thresholds,
            TAU,
            HOLD_MS,
            HOLD_MS / 4,
            NOTIONAL,
            -0.015,
        );
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].t_exit_real_ms, 400_000 + HOLD_MS - 210_000);
        assert_eq!(trades[0].t_exit_ms, 400_000 + HOLD_MS);
        assert_eq!(trades[0].exit_px, 100.5);

        let trades_strict = run_symbol(
            "xyz_TSLA",
            &bars,
            &thresholds,
            TAU,
            HOLD_MS,
            200_000,
            NOTIONAL,
            -0.015,
        );
        assert!(trades_strict.is_empty());
    }
}
