use std::io::Write;
use std::path::Path;

use crate::nyse::{Regime, regime};

pub const FILLS_HEADER: &str = "coin,time_ms,px,sz,side,crossed,tid,block_number,block_time";
pub const BARS_HEADER: &str = "open_ms,close_ms,n,buy_usd,sell_usd,vwap,last_px,regime";

pub struct FillRow {
    pub time_ms: u64,
    pub px: f64,
    pub sz: f64,
    pub is_buy: bool,
}

#[derive(Clone)]
pub struct Bar {
    pub open_ms: u64,
    pub close_ms: u64,
    pub n: u32,
    pub buy_usd: f64,
    pub sell_usd: f64,
    pub vwap: f64,
    pub last_px: f64,
}

fn check_header(path: &Path, got: Option<&str>, want: &str) -> anyhow::Result<()> {
    let got = got.ok_or_else(|| anyhow::anyhow!("{}: empty file", path.display()))?;
    anyhow::ensure!(
        got == want,
        "{}: header mismatch\n  want: {want}\n  got:  {got}",
        path.display()
    );
    Ok(())
}

pub fn read_fills_csv(path: &Path) -> anyhow::Result<Vec<FillRow>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    check_header(path, lines.next(), FILLS_HEADER)?;
    let mut rows = Vec::new();
    for line in lines {
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 9 {
            continue;
        }
        let (Ok(time_ms), Ok(px), Ok(sz)) = (
            cols[1].parse::<u64>(),
            cols[2].parse::<f64>(),
            cols[3].parse::<f64>(),
        ) else {
            continue;
        };
        rows.push(FillRow {
            time_ms,
            px,
            sz,
            is_buy: cols[4] == "B",
        });
    }
    rows.sort_by_key(|r| r.time_ms);
    Ok(rows)
}

pub fn read_bars_csv(path: &Path) -> anyhow::Result<Vec<Bar>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    check_header(path, lines.next(), BARS_HEADER)?;
    let mut out = Vec::new();
    for line in lines {
        let c: Vec<&str> = line.split(',').collect();
        if c.len() < 8 {
            continue;
        }
        out.push(Bar {
            open_ms: c[0].parse()?,
            close_ms: c[1].parse()?,
            n: c[2].parse()?,
            buy_usd: c[3].parse()?,
            sell_usd: c[4].parse()?,
            vwap: c[5].parse()?,
            last_px: c[6].parse()?,
        });
    }
    Ok(out)
}

pub fn auto_threshold(rows: &[FillRow], target_secs: f64) -> (f64, &'static str) {
    let mut rth_usd = 0.0;
    let mut rth_span_ms: (u64, u64) = (u64::MAX, 0);
    let mut all_usd = 0.0;
    let mut all_span: (u64, u64) = (u64::MAX, 0);
    for r in rows {
        let usd = r.px * r.sz;
        all_usd += usd;
        all_span.0 = all_span.0.min(r.time_ms);
        all_span.1 = all_span.1.max(r.time_ms);
        if regime(r.time_ms) == Regime::Rth {
            rth_usd += usd;
            rth_span_ms.0 = rth_span_ms.0.min(r.time_ms);
            rth_span_ms.1 = rth_span_ms.1.max(r.time_ms);
        }
    }
    if rth_usd > 0.0 && rth_span_ms.1 > rth_span_ms.0 {
        let rth_days = ((rth_span_ms.1 - rth_span_ms.0) as f64 / 86_400_000.0).max(1.0);
        let rth_secs = rth_days * 6.5 * 3600.0;
        (rth_usd / (rth_secs / target_secs), "rth-calibrated")
    } else {
        let span_secs = ((all_span.1.saturating_sub(all_span.0)) as f64 / 1000.0).max(target_secs);
        (all_usd / (span_secs / target_secs), "fallback-all-data")
    }
}

pub fn build_bars(rows: &[FillRow], threshold_usd: f64) -> Vec<Bar> {
    let mut bars = Vec::new();
    let mut cur: Option<Bar> = None;
    let mut dollars = 0.0;
    let mut px_x_usd = 0.0;
    for r in rows {
        let usd = r.px * r.sz;
        let bar = cur.get_or_insert(Bar {
            open_ms: r.time_ms,
            close_ms: r.time_ms,
            n: 0,
            buy_usd: 0.0,
            sell_usd: 0.0,
            vwap: 0.0,
            last_px: r.px,
        });
        bar.close_ms = r.time_ms;
        bar.n += 1;
        bar.last_px = r.px;
        if r.is_buy {
            bar.buy_usd += usd;
        } else {
            bar.sell_usd += usd;
        }
        dollars += usd;
        px_x_usd += r.px * usd;
        if dollars >= threshold_usd {
            let mut done = cur.take().unwrap();
            done.vwap = px_x_usd / dollars;
            bars.push(done);
            dollars = 0.0;
            px_x_usd = 0.0;
        }
    }
    bars
}

fn q2(x: f64) -> f64 {
    format!("{x:.2}").parse().unwrap()
}

fn q6(x: f64) -> f64 {
    format!("{x:.6}").parse().unwrap()
}

pub fn quantize_bars(bars: &mut [Bar]) {
    for b in bars {
        b.buy_usd = q2(b.buy_usd);
        b.sell_usd = q2(b.sell_usd);
        b.vwap = q6(b.vwap);
        b.last_px = q6(b.last_px);
    }
}

pub fn write_bars_csv(bars: &[Bar], path: &Path) -> anyhow::Result<()> {
    let mut w = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(w, "{BARS_HEADER}")?;
    for b in bars {
        writeln!(
            w,
            "{},{},{},{:.2},{:.2},{:.6},{:.6},{}",
            b.open_ms,
            b.close_ms,
            b.n,
            b.buy_usd,
            b.sell_usd,
            b.vwap,
            b.last_px,
            regime(b.close_ms).label()
        )?;
    }
    w.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bars_close_on_dollar_threshold_and_split_flow_by_side() {
        let rows = vec![
            FillRow {
                time_ms: 1_000,
                px: 100.0,
                sz: 3.0,
                is_buy: true,
            },
            FillRow {
                time_ms: 2_000,
                px: 100.0,
                sz: 3.0,
                is_buy: false,
            },
            FillRow {
                time_ms: 3_000,
                px: 100.0,
                sz: 5.0,
                is_buy: true,
            },
            FillRow {
                time_ms: 4_000,
                px: 100.0,
                sz: 1.0,
                is_buy: true,
            },
        ];
        let bars = build_bars(&rows, 1_000.0);
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].n, 3);
        assert_eq!(bars[0].buy_usd, 800.0);
        assert_eq!(bars[0].sell_usd, 300.0);
        assert_eq!(bars[0].close_ms, 3_000);
    }

    #[test]
    fn quantize_matches_csv_round_trip() {
        let dir = std::env::temp_dir().join("tracer_bars_quantize_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("q.bars.csv");
        let raw = vec![
            Bar {
                open_ms: 1_755_000_000_000,
                close_ms: 1_755_000_030_000,
                n: 7,
                buy_usd: 10_666.397_501_123_809,
                sell_usd: 0.004_999,
                vwap: 292.520_138_9,
                last_px: 292.635_299_999_7,
            },
            Bar {
                open_ms: 1_755_000_030_001,
                close_ms: 1_755_000_060_000,
                n: 1,
                buy_usd: 0.0,
                sell_usd: 17_464.465,
                vwap: 330.394_444_444_4,
                last_px: 330.39,
            },
        ];
        write_bars_csv(&raw, &path).unwrap();
        let round_tripped = read_bars_csv(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let mut quantized = raw.clone();
        quantize_bars(&mut quantized);
        for (q, r) in quantized.iter().zip(round_tripped.iter()) {
            assert_eq!(q.buy_usd, r.buy_usd);
            assert_eq!(q.sell_usd, r.sell_usd);
            assert_eq!(q.vwap, r.vwap);
            assert_eq!(q.last_px, r.last_px);
        }
    }

    #[test]
    fn header_mismatch_fails_loudly() {
        let dir = std::env::temp_dir().join("tracer_bars_header_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.csv");
        std::fs::write(&path, "time,price,size\n1,2,3\n").unwrap();
        assert!(read_fills_csv(&path).is_err());
        assert!(read_bars_csv(&path).is_err());
        std::fs::remove_file(&path).unwrap();
    }
}
