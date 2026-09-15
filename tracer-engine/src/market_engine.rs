use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use rust_decimal::prelude::ToPrimitive;

use aetelier_io::trades::trades_parquet::read_trades_parquet;
use aetelier_types::trades::TradeSide;

#[derive(Clone, Debug)]
pub struct Print {
    pub ts_ms: u64,
    pub px: f64,
    pub sz: f64,
    pub is_buy: bool,
    pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GapIncident {
    pub opened_epoch_us: u64,
    pub closed_epoch_us: u64,
    pub window_us: u64,
    pub cause: String,
    pub exchange: String,
    pub symbol: String,
    pub possible_dropped_trades: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GapSpan {
    pub start_ms: u64,
    pub end_ms: u64,
}

pub fn datatype_dir(binding_root: &Path, datatype: &str) -> anyhow::Result<Option<PathBuf>> {
    if binding_root
        .parent()
        .and_then(|p| p.file_name())
        .is_some_and(|n| n == datatype)
    {
        return if binding_root.is_dir() {
            Ok(Some(binding_root.to_path_buf()))
        } else {
            Ok(None)
        };
    }
    let mut level = binding_root.to_path_buf();
    for _ in 0..2 {
        let entries = match std::fs::read_dir(&level) {
            Ok(e) => e,
            Err(_) => return Ok(None),
        };
        let mut subdirs: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        subdirs.sort();
        if subdirs.is_empty() {
            return Ok(None);
        }
        anyhow::ensure!(
            subdirs.len() == 1,
            "{}: expected exactly one subdir (single exchange/market_type binding), found {}",
            level.display(),
            subdirs.len()
        );
        level = subdirs.remove(0);
    }
    Ok(Some(level.join(datatype)))
}

fn is_day_dir_name(name: &str) -> bool {
    name.len() == 10
        && name.bytes().enumerate().all(|(i, b)| match i {
            4 | 7 => b == b'-',
            _ => b.is_ascii_digit(),
        })
}

pub fn tape_files_sorted(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut out: Vec<(String, PathBuf)> = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() && is_day_dir_name(&name) {
            let Ok(day_entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for de in day_entries.filter_map(|e| e.ok()) {
                let fname = de.file_name().to_string_lossy().into_owned();
                if fname.ends_with(".parquet") {
                    out.push((format!("{name}/{fname}"), de.path()));
                }
            }
        } else if name.ends_with(".parquet") {
            out.push((name, path));
        }
    }
    out.sort();
    out
}

pub struct StreamTail {
    dir: PathBuf,
    processed: BTreeSet<String>,
    pub watermark_ms: u64,
    pub last_file: Option<String>,
    pub files_read: u64,
}

impl StreamTail {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            processed: BTreeSet::new(),
            watermark_ms: 0,
            last_file: None,
            files_read: 0,
        }
    }

    pub fn poll_new_files(&mut self) -> anyhow::Result<Vec<PathBuf>> {
        let mut fresh = Vec::new();
        for (key, path) in tape_files_sorted(&self.dir) {
            if self.processed.insert(key.clone()) {
                self.last_file = Some(key);
                self.files_read += 1;
                fresh.push(path);
            }
        }
        Ok(fresh)
    }
}

pub struct TradeTailer {
    binding_root: PathBuf,
    resolved: bool,
    pub tail: StreamTail,
    seen_ids: HashMap<String, u64>,
    pub duplicates_dropped: u64,
}

impl TradeTailer {
    pub fn new(binding_root: &Path) -> anyhow::Result<Self> {
        let mut tailer = Self {
            binding_root: binding_root.to_path_buf(),
            resolved: false,
            tail: StreamTail::new(PathBuf::new()),
            seen_ids: HashMap::new(),
            duplicates_dropped: 0,
        };
        tailer.try_resolve()?;
        Ok(tailer)
    }

    pub fn try_resolve(&mut self) -> anyhow::Result<bool> {
        if self.resolved {
            return Ok(true);
        }
        match datatype_dir(&self.binding_root, "trades")? {
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

    pub fn poll(&mut self) -> anyhow::Result<Vec<Print>> {
        let mut out = Vec::new();
        if !self.try_resolve()? {
            return Ok(out);
        }
        for path in self.tail.poll_new_files()? {
            let trades = read_trades_parquet(&path)
                .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
            for t in trades {
                let ts_ms = t.source_trade_ts_us / 1_000;
                if let Some(prev) = self.seen_ids.insert(t.id.clone(), ts_ms) {
                    self.seen_ids.insert(t.id.clone(), prev.max(ts_ms));
                    self.duplicates_dropped += 1;
                    continue;
                }
                let (Some(px), Some(sz)) = (t.price.to_f64(), t.amount.to_f64()) else {
                    continue;
                };
                out.push(Print {
                    ts_ms,
                    px,
                    sz,
                    is_buy: matches!(t.side, TradeSide::Buy),
                    id: t.id,
                });
                self.tail.watermark_ms = self.tail.watermark_ms.max(ts_ms);
            }
        }
        out.sort_by(|a, b| (a.ts_ms, &a.id).cmp(&(b.ts_ms, &b.id)));
        Ok(out)
    }

    pub fn prune_ids(&mut self, before_ms: u64) {
        self.seen_ids.retain(|_, ts| *ts >= before_ms);
    }
}

fn symbol_stem_norm(symbol: &str) -> String {
    symbol.replace(':', "_")
}

fn wave_leaf_gaps(binding_root: &Path) -> Option<(PathBuf, String)> {
    let datatype = binding_root.parent()?.file_name()?.to_str()?;
    if datatype != "trades" && datatype != "orderbooks" {
        return None;
    }
    let binding = binding_root.file_name()?.to_string_lossy().into_owned();
    let collected = binding_root.ancestors().nth(5)?;
    Some((collected.join("gaps"), binding))
}

pub fn read_gap_incidents(
    binding_root: &Path,
    symbol_stem: Option<&str>,
) -> anyhow::Result<Vec<GapIncident>> {
    let mut incidents = Vec::new();
    let wave = wave_leaf_gaps(binding_root);
    let gaps_dir = match &wave {
        Some((dir, _)) => dir.clone(),
        None => match datatype_dir(binding_root, "gaps") {
            Ok(Some(dir)) => dir,
            _ => return Ok(incidents),
        },
    };
    let entries = match std::fs::read_dir(&gaps_dir) {
        Ok(e) => e,
        Err(_) => return Ok(incidents),
    };
    let want = symbol_stem.map(symbol_stem_norm);
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "jsonl") {
            continue;
        }
        if let Some((_, binding)) = &wave
            && !entry
                .file_name()
                .to_string_lossy()
                .starts_with(binding.as_str())
        {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        for line in text.lines() {
            let Ok(inc) = serde_json::from_str::<GapIncident>(line) else {
                continue;
            };
            if let Some(w) = &want
                && symbol_stem_norm(&inc.symbol) != *w
            {
                continue;
            }
            incidents.push(inc);
        }
    }
    incidents.sort_by_key(|i| i.opened_epoch_us);
    Ok(incidents)
}

pub fn read_gap_spans(
    binding_root: &Path,
    symbol_stem: Option<&str>,
) -> anyhow::Result<Vec<GapSpan>> {
    Ok(read_gap_incidents(binding_root, symbol_stem)?
        .into_iter()
        .map(|inc| GapSpan {
            start_ms: inc.opened_epoch_us / 1_000,
            end_ms: inc.closed_epoch_us / 1_000,
        })
        .collect())
}

pub fn gap_overlaps(spans: &[GapSpan], start_ms: u64, end_ms: u64) -> bool {
    spans
        .iter()
        .any(|s| s.start_ms < end_ms && s.end_ms > start_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_io::trades::trades_parquet::write_trades_parquet;
    use aetelier_types::trades::{Trade, TradeOrigin, TradeSide};
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;

    fn trade(ts_us: u64, px: f64, sz: f64, buy: bool, id: &str) -> Trade {
        Trade {
            source_trade_ts_us: ts_us,
            local_trade_ts_us: ts_us,
            source_trade_rtt_us: 0,
            pair: TradingPair::new("xyz:TSLA", "USDC"),
            side: if buy { TradeSide::Buy } else { TradeSide::Sell },
            amount: Decimal::from_f64_retain(sz).unwrap(),
            price: Decimal::from_f64_retain(px).unwrap(),
            exchange: "hyperliquid".into(),
            id: id.into(),
            origin: TradeOrigin::Ws,
        }
    }

    fn binding_fixture(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("tracer_me_{name}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("hyperliquid/perpetual/trades")).unwrap();
        root
    }

    #[test]
    fn tailer_reads_new_files_dedupes_and_advances_watermark() {
        let root = binding_fixture("dedupe");
        let dir = root.join("hyperliquid/perpetual/trades");
        write_trades_parquet(
            &[
                trade(1_000_000, 100.0, 1.0, true, "t1"),
                trade(2_000_000, 100.5, 1.0, false, "t2"),
            ],
            &dir.join("a.parquet"),
        )
        .unwrap();
        let mut tailer = TradeTailer::new(&root).unwrap();
        let first = tailer.poll().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(tailer.tail.watermark_ms, 2_000);
        assert!(first[0].is_buy);
        assert!(!first[1].is_buy);

        write_trades_parquet(
            &[
                trade(2_000_000, 100.5, 1.0, false, "t2"),
                trade(3_000_000, 101.0, 2.0, true, "t3"),
            ],
            &dir.join("b.parquet"),
        )
        .unwrap();
        let second = tailer.poll().unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].id, "t3");
        assert_eq!(tailer.duplicates_dropped, 1);
        assert_eq!(tailer.tail.watermark_ms, 3_000);

        assert!(tailer.poll().unwrap().is_empty());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn ambiguous_binding_layout_fails_loudly() {
        let root = binding_fixture("ambiguous");
        std::fs::create_dir_all(root.join("binance/perpetual/trades")).unwrap();
        assert!(TradeTailer::new(&root).is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn missing_binding_layout_is_pending_then_resolves() {
        let root = std::env::temp_dir().join("tracer_me_pending");
        let _ = std::fs::remove_dir_all(&root);
        let mut tailer = TradeTailer::new(&root).unwrap();
        assert!(tailer.pending());
        assert!(tailer.poll().unwrap().is_empty());
        std::fs::create_dir_all(root.join("hyperliquid")).unwrap();
        assert!(tailer.poll().unwrap().is_empty());
        assert!(tailer.pending());
        let dir = root.join("hyperliquid/perpetual/trades");
        std::fs::create_dir_all(&dir).unwrap();
        write_trades_parquet(
            &[trade(1_000_000, 100.0, 1.0, true, "t1")],
            &dir.join("a.parquet"),
        )
        .unwrap();
        let prints = tailer.poll().unwrap();
        assert_eq!(prints.len(), 1);
        assert!(!tailer.pending());
        assert_eq!(tailer.tail.watermark_ms, 1_000);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn datatype_leaf_absent_resolves_and_idles() {
        let root = std::env::temp_dir().join("tracer_me_leafless");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("hyperliquid/perpetual")).unwrap();
        let mut tailer = TradeTailer::new(&root).unwrap();
        assert!(!tailer.pending());
        assert!(tailer.poll().unwrap().is_empty());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn ambiguity_appearing_after_start_fails_at_poll() {
        let root = std::env::temp_dir().join("tracer_me_late_ambiguous");
        let _ = std::fs::remove_dir_all(&root);
        let mut tailer = TradeTailer::new(&root).unwrap();
        assert!(tailer.pending());
        std::fs::create_dir_all(root.join("hyperliquid/perpetual/trades")).unwrap();
        std::fs::create_dir_all(root.join("binance/perpetual/trades")).unwrap();
        assert!(tailer.poll().is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn gap_spans_parse_and_overlap_permutations() {
        let root = binding_fixture("gaps");
        let gaps = root.join("hyperliquid/perpetual/gaps");
        std::fs::create_dir_all(&gaps).unwrap();
        std::fs::write(
            gaps.join("hyperliquid_xyz-TSLA_gap_ledger.jsonl"),
            concat!(
                r#"{"opened_epoch_us":10000000,"closed_epoch_us":20000000,"window_us":10000000,"cause":"disconnect","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":5}"#,
                "\n",
                "not json\n",
                r#"{"opened_epoch_us":50000000,"closed_epoch_us":60000000,"window_us":10000000,"cause":"resync","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":null}"#,
                "\n"
            ),
        )
        .unwrap();
        let spans = read_gap_spans(&root, Some("xyz_TSLA")).unwrap();
        let foreign = read_gap_spans(&root, Some("xyz_NVDA")).unwrap();
        assert!(foreign.is_empty());
        let incidents = read_gap_incidents(&root, None).unwrap();
        assert_eq!(incidents.len(), 2);
        assert_eq!(incidents[0].cause, "disconnect");
        assert_eq!(incidents[0].possible_dropped_trades, Some(5));
        assert_eq!(incidents[0].exchange, "hyperliquid");
        assert_eq!(incidents[0].symbol, "xyz:TSLA");
        assert_eq!(spans.len(), 2);
        assert_eq!(
            spans[0],
            GapSpan {
                start_ms: 10_000,
                end_ms: 20_000
            }
        );

        assert!(gap_overlaps(&spans, 5_000, 10_001));
        assert!(gap_overlaps(&spans, 15_000, 16_000));
        assert!(gap_overlaps(&spans, 19_999, 30_000));
        assert!(!gap_overlaps(&spans, 20_000, 50_000));
        assert!(!gap_overlaps(&spans, 60_000, 70_000));

        let no_gaps = binding_fixture("nogaps");
        assert!(read_gap_spans(&no_gaps, None).unwrap().is_empty());
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&no_gaps).unwrap();
    }
}

#[cfg(test)]
mod wave_layout_tests {
    use super::*;

    fn leaf_fixture(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("tracer_wave_{name}"));
        let _ = std::fs::remove_dir_all(&root);
        let leaf =
            root.join("recorded/hyperliquid/perpetual/trades/0f0f0f0f-aaaa-bbbb-cccc-dddddddddddd");
        std::fs::create_dir_all(&leaf).unwrap();
        leaf
    }

    fn touch(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"x").unwrap();
    }

    #[test]
    fn a_wave_leaf_root_resolves_to_itself() {
        let leaf = leaf_fixture("leaf_self");
        let resolved = datatype_dir(&leaf, "trades").unwrap();
        assert_eq!(resolved, Some(leaf));
    }

    #[test]
    fn a_wave_leaf_that_does_not_exist_yet_stays_pending() {
        let leaf = leaf_fixture("leaf_absent");
        std::fs::remove_dir_all(&leaf).unwrap();
        assert_eq!(datatype_dir(&leaf, "trades").unwrap(), None);
    }

    #[test]
    fn a_legacy_binding_root_still_descends_to_the_datatype_dir() {
        let root = std::env::temp_dir().join("tracer_wave_legacy");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("hyperliquid/perpetual/trades")).unwrap();
        assert_eq!(
            datatype_dir(&root, "trades").unwrap(),
            Some(root.join("hyperliquid/perpetual/trades"))
        );
    }

    #[test]
    fn day_dirs_are_tailed_in_order_and_a_rollover_day_appends() {
        let leaf = leaf_fixture("day_tail");
        touch(&leaf.join("2026-09-14/b.parquet"));
        touch(&leaf.join("2026-09-14/a.parquet"));
        touch(&leaf.join("2026-09-14/index.jsonl"));
        let mut tail = StreamTail::new(leaf.clone());
        let first: Vec<_> = tail.poll_new_files().unwrap();
        assert_eq!(
            first,
            vec![
                leaf.join("2026-09-14/a.parquet"),
                leaf.join("2026-09-14/b.parquet")
            ]
        );
        assert_eq!(tail.files_read, 2);
        touch(&leaf.join("2026-09-15/c.parquet"));
        let second = tail.poll_new_files().unwrap();
        assert_eq!(second, vec![leaf.join("2026-09-15/c.parquet")]);
        assert_eq!(tail.last_file.as_deref(), Some("2026-09-15/c.parquet"));
    }

    #[test]
    fn a_flat_legacy_dir_still_tails_and_ignores_non_day_subdirs() {
        let root = std::env::temp_dir().join("tracer_wave_flat");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("not-a-day")).unwrap();
        touch(&root.join("not-a-day/hidden.parquet"));
        touch(&root.join("a.parquet"));
        let mut tail = StreamTail::new(root.clone());
        assert_eq!(tail.poll_new_files().unwrap(), vec![root.join("a.parquet")]);
    }
}

#[cfg(test)]
mod wave_gap_tests {
    use super::*;

    #[test]
    fn wave_leaf_reads_only_its_own_ledger_from_collected_gaps() {
        let root = std::env::temp_dir().join("tracer_wave_gaps");
        let _ = std::fs::remove_dir_all(&root);
        let leaf = root.join(
            "collected/recorded/hyperliquid/perpetual/trades/aaaa1111-0000-0000-0000-000000000000",
        );
        std::fs::create_dir_all(&leaf).unwrap();
        let gaps = root.join("collected/gaps");
        std::fs::create_dir_all(&gaps).unwrap();
        let own = r#"{"opened_epoch_us":1000,"closed_epoch_us":2000,"window_us":1000,"cause":"ws","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":1}"#;
        let foreign = r#"{"opened_epoch_us":9000,"closed_epoch_us":9500,"window_us":500,"cause":"ws","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":2}"#;
        std::fs::write(
            gaps.join("aaaa1111-0000-0000-0000-000000000000_gap_ledger.jsonl"),
            own,
        )
        .unwrap();
        std::fs::write(
            gaps.join("bbbb2222-0000-0000-0000-000000000000_gap_ledger.jsonl"),
            foreign,
        )
        .unwrap();
        let incidents = read_gap_incidents(&leaf, None).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].opened_epoch_us, 1000);
    }
}
