use crate::paper::ExecutedTrade;

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.iter().sum::<f64>() / v.len() as f64
}

fn sd(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return f64::NAN;
    }
    let m = mean(v);
    (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * q).round() as usize]
}

#[derive(Clone, Debug)]
pub struct StatsBlock {
    pub n: usize,
    pub longs: usize,
    pub total: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub payoff: f64,
    pub mean_usd: f64,
    pub t_stat: f64,
    pub p05: f64,
    pub p50: f64,
    pub p95: f64,
    pub max_profit: f64,
    pub max_loss: f64,
    pub max_dd: f64,
    pub underwater_days: f64,
    pub trades_per_hour: f64,
    pub max_consec_losses: usize,
    pub fees_total: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
}

pub fn block(trades: &[&ExecutedTrade]) -> Option<StatsBlock> {
    if trades.is_empty() {
        return None;
    }
    let mut sorted: Vec<&ExecutedTrade> = trades.to_vec();
    sorted.sort_by_key(|t| t.t_exit_ms);
    let pnls: Vec<f64> = sorted.iter().map(|t| t.pnl_usd).collect();
    let wins: Vec<f64> = pnls.iter().copied().filter(|p| *p > 0.0).collect();
    let losses: Vec<f64> = pnls.iter().copied().filter(|p| *p < 0.0).collect();
    let mut cum = 0.0f64;
    let mut peak = 0.0f64;
    let mut dd = 0.0f64;
    let mut peak_ts = sorted[0].t_exit_ms;
    let mut underwater_ms = 0u64;
    for t in &sorted {
        cum += t.pnl_usd;
        if cum >= peak {
            peak = cum;
            peak_ts = t.t_exit_ms;
        } else {
            underwater_ms = underwater_ms.max(t.t_exit_ms - peak_ts);
            dd = dd.max(peak - cum);
        }
    }
    let mut consec = 0usize;
    let mut max_consec = 0usize;
    for p in &pnls {
        if *p < 0.0 {
            consec += 1;
            max_consec = max_consec.max(consec);
        } else {
            consec = 0;
        }
    }
    let span_h = (sorted.last().unwrap().t_exit_ms - sorted[0].t_entry_ms) as f64 / 3_600_000.0;
    let m = mean(&pnls);
    let s = sd(&pnls);
    let downside = {
        let sq_below: f64 = pnls.iter().map(|p| p.min(0.0).powi(2)).sum();
        (sq_below / pnls.len() as f64).sqrt()
    };
    let mut sortable = pnls.clone();
    Some(StatsBlock {
        n: pnls.len(),
        longs: sorted.iter().filter(|t| t.side > 0).count(),
        total: pnls.iter().sum(),
        win_rate: wins.len() as f64 / pnls.len() as f64,
        profit_factor: if losses.is_empty() {
            f64::INFINITY
        } else {
            wins.iter().sum::<f64>() / losses.iter().sum::<f64>().abs()
        },
        payoff: mean(&wins) / mean(&losses).abs(),
        mean_usd: m,
        t_stat: if s > 0.0 {
            m / (s / (pnls.len() as f64).sqrt())
        } else {
            f64::NAN
        },
        p05: pct(&mut sortable, 0.05),
        p50: pct(&mut sortable, 0.50),
        p95: pct(&mut sortable, 0.95),
        max_profit: pnls.iter().cloned().fold(f64::MIN, f64::max),
        max_loss: pnls.iter().cloned().fold(f64::MAX, f64::min),
        max_dd: dd,
        underwater_days: underwater_ms as f64 / 86_400_000.0,
        trades_per_hour: pnls.len() as f64 / span_h.max(0.01),
        max_consec_losses: max_consec,
        fees_total: sorted
            .iter()
            .map(|t| t.fee_entry_usd + t.fee_exit_usd)
            .sum(),
        sharpe: if s > 0.0 { m / s } else { f64::NAN },
        sortino: if downside > 0.0 {
            m / downside
        } else {
            f64::NAN
        },
        calmar: if dd > 0.0 {
            pnls.iter().sum::<f64>() / dd
        } else {
            f64::INFINITY
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(t_entry_ms: u64, t_exit_ms: u64, side: i8, pnl_usd: f64) -> ExecutedTrade {
        ExecutedTrade {
            symbol: "xyz_TSLA".into(),
            side,
            score: 0.9,
            decile: 10,
            t_entry_ms,
            entry_px: 100.0,
            entry_last_px: 100.0,
            t_exit_ms,
            t_exit_real_ms: t_exit_ms,
            exit_px: 100.0,
            fee_entry_usd: -0.0015,
            fee_exit_usd: -0.0015,
            pnl_usd,
        }
    }

    #[test]
    fn block_matches_hand_computed_values() {
        let trades = [
            trade(0, 900_000, 1, 2.0),
            trade(1_000_000, 1_900_000, 1, -1.0),
            trade(2_000_000, 2_900_000, -1, -1.0),
            trade(3_000_000, 3_900_000, 1, 4.0),
        ];
        let refs: Vec<&ExecutedTrade> = trades.iter().collect();
        let b = block(&refs).unwrap();
        assert_eq!(b.n, 4);
        assert_eq!(b.longs, 3);
        assert_eq!(b.total, 4.0);
        assert_eq!(b.win_rate, 0.5);
        assert_eq!(b.profit_factor, 3.0);
        assert_eq!(b.mean_usd, 1.0);
        assert_eq!(b.max_dd, 2.0);
        assert_eq!(b.max_consec_losses, 2);
        assert_eq!(b.max_profit, 4.0);
        assert_eq!(b.max_loss, -1.0);
        assert_eq!(b.calmar, 2.0);
        let sd_expected = (((2.0f64 - 1.0).powi(2)
            + (-1.0f64 - 1.0).powi(2)
            + (-1.0f64 - 1.0).powi(2)
            + (4.0f64 - 1.0).powi(2))
            / 3.0)
            .sqrt();
        assert!((b.sharpe - 1.0 / sd_expected).abs() < 1e-12);
        let downside = ((1.0f64 + 1.0) / 4.0).sqrt();
        assert!((b.sortino - 1.0 / downside).abs() < 1e-12);
        assert!((b.t_stat - 1.0 / (sd_expected / 2.0)).abs() < 1e-12);
    }

    #[test]
    fn empty_and_lossless_blocks() {
        assert!(block(&[]).is_none());
        let trades = [
            trade(0, 900_000, 1, 1.0),
            trade(1_000_000, 1_900_000, 1, 2.0),
        ];
        let refs: Vec<&ExecutedTrade> = trades.iter().collect();
        let b = block(&refs).unwrap();
        assert_eq!(b.profit_factor, f64::INFINITY);
        assert_eq!(b.max_dd, 0.0);
        assert_eq!(b.calmar, f64::INFINITY);
        assert!(b.sortino.is_nan());
    }
}
