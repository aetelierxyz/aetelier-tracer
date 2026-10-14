use crate::bars::Bar;

pub struct SignalBar {
    pub close_ms: u64,
    pub score: f64,
}

pub fn ewma_imbalance(bars: &[Bar], tau_secs: f64) -> Vec<SignalBar> {
    let mut buy = 0.0f64;
    let mut sell = 0.0f64;
    let mut last_ms: Option<u64> = None;
    let mut out = Vec::with_capacity(bars.len());
    for b in bars {
        let decay = match last_ms {
            Some(prev) if b.close_ms > prev => {
                let dt = (b.close_ms - prev) as f64 / 1000.0;
                libm::exp(-dt / tau_secs)
            }
            _ => 1.0,
        };
        buy = buy * decay + b.buy_usd;
        sell = sell * decay + b.sell_usd;
        last_ms = Some(b.close_ms);
        let total = buy + sell;
        let score = if total > 0.0 {
            (buy - sell) / total
        } else {
            0.0
        };
        out.push(SignalBar {
            close_ms: b.close_ms,
            score,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Bar;

    fn bar(close_ms: u64, buy: f64, sell: f64) -> Bar {
        Bar {
            open_ms: close_ms,
            close_ms,
            n: 1,
            buy_usd: buy,
            sell_usd: sell,
            vwap: 100.0,
            last_px: 100.0,
        }
    }

    #[test]
    fn imbalance_saturates_with_one_sided_flow_and_decays() {
        let bars = vec![
            bar(0, 1000.0, 0.0),
            bar(1_000, 1000.0, 0.0),
            bar(2_000, 0.0, 1000.0),
            bar(600_000, 0.0, 1000.0),
        ];
        let s = ewma_imbalance(&bars, 60.0);
        assert!(s[0].score > 0.99);
        assert!(s[1].score > 0.99);
        assert!(s[2].score < s[1].score);
        assert!(s[3].score < -0.9);
    }
}
