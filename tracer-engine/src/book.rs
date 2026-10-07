use std::collections::BTreeMap;
use std::path::Path;

use rust_decimal::prelude::ToPrimitive;

use aetelier_io::orderbooks::ob_parquet::read_ob_parquet;
use aetelier_types::orderbooks::{OrderbookTarget, OrderbookTargetData};

use crate::bars::FillRow;
use crate::market_engine::{Print, StreamTail, datatype_dir};
use crate::verdict::{Fill, MakerSide};

pub const BOOK_STALENESS_CAP_MS: u64 = 5_000;

fn eps(px: f64) -> f64 {
    1e-9 * px.abs()
}

#[derive(Clone, Debug)]
pub struct BookSnapshot {
    pub ts_ms: u64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

pub fn depth_ahead_usd(snap: &BookSnapshot, maker_side: MakerSide, posted_px: f64) -> f64 {
    let levels = match maker_side {
        MakerSide::Buy => &snap.bids,
        MakerSide::Sell => &snap.asks,
    };
    levels
        .iter()
        .filter(|(px, _)| match maker_side {
            MakerSide::Buy => *px >= posted_px - eps(posted_px),
            MakerSide::Sell => *px <= posted_px + eps(posted_px),
        })
        .map(|(px, sz)| px * sz)
        .sum()
}

pub struct BookTailer {
    binding_root: std::path::PathBuf,
    resolved: bool,
    pub tail: StreamTail,
    snapshots: BTreeMap<u64, BookSnapshot>,
}

impl BookTailer {
    pub fn new(binding_root: &Path) -> anyhow::Result<Self> {
        let mut tailer = Self {
            binding_root: binding_root.to_path_buf(),
            resolved: false,
            tail: StreamTail::new(std::path::PathBuf::new()),
            snapshots: BTreeMap::new(),
        };
        tailer.try_resolve()?;
        Ok(tailer)
    }

    pub fn try_resolve(&mut self) -> anyhow::Result<bool> {
        if self.resolved {
            return Ok(true);
        }
        match datatype_dir(&self.binding_root, "orderbooks")? {
            Some(dir) => {
                self.tail = StreamTail::new(dir);
                self.resolved = true;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn pending(&self) -> bool {
        !self.resolved
    }

    #[cfg(test)]
    pub(crate) fn inject_snapshot(&mut self, snap: BookSnapshot) {
        self.snapshots.insert(snap.ts_ms, snap);
    }

    pub fn poll(&mut self) -> anyhow::Result<()> {
        if !self.try_resolve()? {
            return Ok(());
        }
        for path in self.tail.poll_new_files()? {
            let data = read_ob_parquet(&path, OrderbookTarget::Snapshot)
                .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
            let OrderbookTargetData::Snapshot(books) = data else {
                continue;
            };
            for ob in books {
                let ts_ms = ob.effective_ts_us() / 1_000;
                let level_vec = |m: &BTreeMap<
                    rust_decimal::Decimal,
                    aetelier_types::levels::Level,
                >|
                 -> Vec<(f64, f64)> {
                    m.iter()
                        .filter_map(|(px, level)| Some((px.to_f64()?, level.volume.to_f64()?)))
                        .collect()
                };
                let snap = BookSnapshot {
                    ts_ms,
                    bids: level_vec(&ob.bids),
                    asks: level_vec(&ob.asks),
                };
                self.tail.watermark_ms = self.tail.watermark_ms.max(ts_ms);
                self.snapshots.insert(ts_ms, snap);
            }
        }
        Ok(())
    }

    pub fn nearest_at_or_before(&self, ts_ms: u64) -> Option<&BookSnapshot> {
        self.snapshots.range(..=ts_ms).next_back().map(|(_, s)| s)
    }

    pub fn prune(&mut self, before_ms: u64) {
        self.snapshots.retain(|ts, _| *ts >= before_ms);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DepthAtPost {
    pub depth_ahead_usd: Option<f64>,
    pub book_ts_ms: Option<u64>,
    pub book_staleness_ms: Option<u64>,
}

pub fn depth_at_post(
    books: Option<&BookTailer>,
    maker_side: MakerSide,
    posted_px: f64,
    t_post_ms: u64,
) -> DepthAtPost {
    let Some(books) = books else {
        return DepthAtPost::default();
    };
    let Some(snap) = books.nearest_at_or_before(t_post_ms) else {
        return DepthAtPost::default();
    };
    let staleness = t_post_ms - snap.ts_ms;
    if staleness > BOOK_STALENESS_CAP_MS {
        return DepthAtPost {
            depth_ahead_usd: None,
            book_ts_ms: Some(snap.ts_ms),
            book_staleness_ms: Some(staleness),
        };
    }
    DepthAtPost {
        depth_ahead_usd: Some(depth_ahead_usd(snap, maker_side, posted_px)),
        book_ts_ms: Some(snap.ts_ms),
        book_staleness_ms: Some(staleness),
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueueScan {
    pub cleared: Option<Fill>,
    pub cum_opp_usd: f64,
}

fn opposite_at_or_through(maker_side: MakerSide, px: f64, posted: f64, is_buy: bool) -> bool {
    let opposite = match maker_side {
        MakerSide::Buy => !is_buy,
        MakerSide::Sell => is_buy,
    };
    if !opposite {
        return false;
    }
    match maker_side {
        MakerSide::Buy => px <= posted + eps(posted),
        MakerSide::Sell => px >= posted - eps(posted),
    }
}

pub fn scan_queue(
    prints: &[Print],
    maker_side: MakerSide,
    posted_px: f64,
    t_post_ms: u64,
    window_end_ms: u64,
    depth_ahead_usd: f64,
) -> QueueScan {
    let mut scan = QueueScan::default();
    let start = prints.partition_point(|p| p.ts_ms <= t_post_ms);
    for p in &prints[start..] {
        if p.ts_ms > window_end_ms {
            break;
        }
        if !opposite_at_or_through(maker_side, p.px, posted_px, p.is_buy) {
            continue;
        }
        scan.cum_opp_usd += p.px * p.sz;
        if scan.cum_opp_usd > depth_ahead_usd {
            scan.cleared = Some(Fill {
                ts_ms: p.ts_ms,
                px: p.px,
            });
            break;
        }
    }
    scan
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OppositeEvidence {
    pub notional_usd: f64,
    pub vwap: Option<f64>,
    pub n_prints: u64,
}

pub fn opposite_evidence(
    prints: &[FillRow],
    maker_side: MakerSide,
    posted_px: f64,
    start_ms: u64,
    end_ms: u64,
) -> OppositeEvidence {
    let mut ev = OppositeEvidence::default();
    let mut sz_sum = 0.0;
    let mut px_x_sz = 0.0;
    let start = prints.partition_point(|p| p.time_ms <= start_ms);
    for p in &prints[start..] {
        if p.time_ms > end_ms {
            break;
        }
        if !opposite_at_or_through(maker_side, p.px, posted_px, p.is_buy) {
            continue;
        }
        ev.notional_usd += p.px * p.sz;
        ev.n_prints += 1;
        sz_sum += p.sz;
        px_x_sz += p.px * p.sz;
    }
    if sz_sum > 0.0 {
        ev.vwap = Some(px_x_sz / sz_sum);
    }
    ev
}

pub fn prints_as_fillrows(prints: &[Print]) -> Vec<FillRow> {
    prints
        .iter()
        .map(|p| FillRow {
            time_ms: p.ts_ms,
            px: p.px,
            sz: p.sz,
            is_buy: p.is_buy,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn print(ts: u64, px: f64, sz: f64, is_buy: bool) -> Print {
        Print {
            ts_ms: ts,
            px,
            sz,
            is_buy,
            id: format!("{ts}"),
        }
    }

    fn snap(ts_ms: u64) -> BookSnapshot {
        BookSnapshot {
            ts_ms,
            bids: vec![(99.0, 10.0), (100.0, 5.0), (100.5, 2.0)],
            asks: vec![(101.0, 3.0), (102.0, 7.0)],
        }
    }

    #[test]
    fn queue_rule_ties_at_posted_level() {
        let s = snap(0);
        let ahead = depth_ahead_usd(&s, MakerSide::Buy, 100.0);
        assert!((ahead - (100.0 * 5.0 + 100.5 * 2.0)).abs() < 1e-9);

        let prints = vec![
            print(1_000, 100.0, 5.0, false),
            print(2_000, 100.0, 2.01, false),
            print(3_000, 99.9, 1.0, false),
        ];
        let equality_not_cleared =
            scan_queue(&prints[..2], MakerSide::Buy, 100.0, 0, 10_000, ahead);
        assert!(equality_not_cleared.cleared.is_none());
        assert!((equality_not_cleared.cum_opp_usd - ahead).abs() < 1e-9);
        let scan = scan_queue(&prints, MakerSide::Buy, 100.0, 0, 10_000, ahead);
        assert_eq!(scan.cleared.unwrap().ts_ms, 3_000);

        let strict = scan_queue(&prints, MakerSide::Buy, 100.0, 0, 10_000, 500.0);
        assert_eq!(strict.cleared.unwrap().ts_ms, 2_000);
    }

    #[test]
    fn queue_ignores_same_side_and_outside_prices() {
        let prints = vec![
            print(1_000, 100.0, 100.0, true),
            print(2_000, 100.2, 100.0, false),
            print(3_000, 99.5, 1.0, false),
        ];
        let scan = scan_queue(&prints, MakerSide::Buy, 100.0, 0, 10_000, 50.0);
        assert_eq!(scan.cleared.unwrap().ts_ms, 3_000);
        assert!((scan.cum_opp_usd - 99.5).abs() < 1e-9);
    }

    #[test]
    fn depth_at_post_staleness_boundaries() {
        let root = std::env::temp_dir().join("tracer_book_stale_test");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("hyperliquid/perpetual/orderbooks")).unwrap();
        let mut books = BookTailer::new(&root).unwrap();
        books.snapshots.insert(10_000, snap(10_000));

        let fresh = depth_at_post(Some(&books), MakerSide::Buy, 100.0, 14_999);
        assert!(fresh.depth_ahead_usd.is_some());
        assert_eq!(fresh.book_staleness_ms, Some(4_999));
        let at_cap = depth_at_post(Some(&books), MakerSide::Buy, 100.0, 15_000);
        assert!(at_cap.depth_ahead_usd.is_some());
        let over_cap = depth_at_post(Some(&books), MakerSide::Buy, 100.0, 15_001);
        assert!(over_cap.depth_ahead_usd.is_none());
        assert_eq!(over_cap.book_ts_ms, Some(10_000));

        let absent = depth_at_post(None, MakerSide::Buy, 100.0, 14_999);
        assert!(absent.depth_ahead_usd.is_none());
        assert!(absent.book_ts_ms.is_none());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn sell_side_depth_mirrors() {
        let s = snap(0);
        let ahead = depth_ahead_usd(&s, MakerSide::Sell, 101.0);
        assert!((ahead - 101.0 * 3.0).abs() < 1e-9);
    }

    #[test]
    fn books_binding_pending_then_resolves() {
        let root = std::env::temp_dir().join("tracer_book_pending");
        let _ = std::fs::remove_dir_all(&root);
        let mut books = BookTailer::new(&root).unwrap();
        assert!(books.pending());
        books.poll().unwrap();
        assert!(books.pending());
        std::fs::create_dir_all(root.join("hyperliquid/perpetual/orderbooks")).unwrap();
        books.poll().unwrap();
        assert!(!books.pending());
        std::fs::remove_dir_all(&root).unwrap();
    }
}
