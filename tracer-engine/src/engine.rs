use std::path::{Path, PathBuf};

use crate::bars::Bar;
use crate::book::{BookTailer, DepthAtPost, depth_at_post, prints_as_fillrows, scan_queue};
use crate::deciles::decile_of;
use crate::ledger::{
    EVENT_SCHEMA, Event, EventKind, LedgerWriter, Leg, Mode, PostState, Verdict, event_id, trade_id,
};
use crate::market_engine::{GapSpan, Print, TradeTailer, gap_overlaps, read_gap_spans};
use crate::nyse::{calendar_expired, regime};
use crate::params::TracerParams;
use crate::verdict::{MakerSide, first_opposite_print, scan_maker};

pub const WATERMARK_SLACK_MS: u64 = 2_000;
pub const DEFAULT_IDLE_AFTER_MS: u64 = 30_000;

#[derive(Clone, Debug)]
pub struct LiveConfig {
    pub symbol_stem: String,
    pub grace_ms: u64,
    pub cross_window_ms: u64,
    pub queue_verdicts: bool,
    pub idle_after_ms: u64,
}

struct SignalState {
    buy: f64,
    sell: f64,
    last_ms: Option<u64>,
    prev_extreme: bool,
}

impl SignalState {
    fn update(&mut self, bar: &Bar, tau_secs: f64) -> f64 {
        let decay = match self.last_ms {
            Some(prev) if bar.close_ms > prev => {
                let dt = (bar.close_ms - prev) as f64 / 1000.0;
                (-dt / tau_secs).exp()
            }
            _ => 1.0,
        };
        self.buy = self.buy * decay + bar.buy_usd;
        self.sell = self.sell * decay + bar.sell_usd;
        self.last_ms = Some(bar.close_ms);
        let total = self.buy + self.sell;
        if total > 0.0 {
            (self.buy - self.sell) / total
        } else {
            0.0
        }
    }
}

struct BarBuilder {
    threshold_usd: f64,
    cur: Option<Bar>,
    dollars: f64,
    px_x_usd: f64,
}

impl BarBuilder {
    fn push(&mut self, p: &Print) -> Option<Bar> {
        let usd = p.px * p.sz;
        let bar = self.cur.get_or_insert(Bar {
            open_ms: p.ts_ms,
            close_ms: p.ts_ms,
            n: 0,
            buy_usd: 0.0,
            sell_usd: 0.0,
            vwap: 0.0,
            last_px: p.px,
        });
        bar.close_ms = p.ts_ms;
        bar.n += 1;
        bar.last_px = p.px;
        if p.is_buy {
            bar.buy_usd += usd;
        } else {
            bar.sell_usd += usd;
        }
        self.dollars += usd;
        self.px_x_usd += p.px * usd;
        if self.dollars >= self.threshold_usd {
            let mut done = self.cur.take().unwrap();
            done.vwap = self.px_x_usd / self.dollars;
            self.dollars = 0.0;
            self.px_x_usd = 0.0;
            let mut single = [done];
            crate::bars::quantize_bars(&mut single);
            let [done] = single;
            return Some(done);
        }
        None
    }
}

enum LegPhase {
    Entry {
        window_end_ms: u64,
    },
    Exit {
        t_exit_post_ms: u64,
        posted_px: f64,
        grace_end_ms: u64,
    },
}

struct OpenTrade {
    trade_id: String,
    seq: u32,
    side: i8,
    score: f64,
    decile: usize,
    t_post_ms: u64,
    posted_px: f64,
    depth: DepthAtPost,
    phase: LegPhase,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct EngineCounters {
    pub posted: u64,
    pub entry_filled_through: u64,
    pub entry_filled_queue_only: u64,
    pub entry_expired_unfilled: u64,
    pub entry_gapped: u64,
    pub exit_maker: u64,
    pub exit_taker_cross: u64,
    pub exit_unpriced: u64,
    pub would_be_voided_by_paper_filter: u64,
    pub queue_vs_through_disagreements: u64,
    pub book_degraded: u64,
    pub duplicates_dropped: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct StreamStatus {
    pub watermark_ms: u64,
    pub last_file: Option<String>,
    pub files_read: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct EngineStatus {
    pub schema: &'static str,
    pub mode: &'static str,
    pub engine_version: &'static str,
    pub params_version: u32,
    pub symbol: String,
    pub queue_verdicts: bool,
    pub trades: StreamStatus,
    pub books: Option<StreamStatus>,
    pub merged_watermark_ms: u64,
    pub idle: bool,
    pub calendar_expired: bool,
    pub open_trades: usize,
    pub counters: EngineCounters,
    pub corrected_n_kept: usize,
    pub corrected_total_usd: f64,
    pub events_written: u64,
    pub ready: bool,
}

pub struct LiveEngine {
    params: TracerParams,
    cfg: LiveConfig,
    maker_fee_leg_bps: f64,
    taker_fee_leg_bps: f64,
    trades_root: PathBuf,
    books_root: Option<PathBuf>,
    trades: TradeTailer,
    books: Option<BookTailer>,
    gaps: Vec<GapSpan>,
    prints: Vec<Print>,
    bars: BarBuilder,
    signal: SignalState,
    recent_bars: Vec<Bar>,
    open: Vec<OpenTrade>,
    ledger: LedgerWriter,
    pub counters: EngineCounters,
    pub corrected_points: Vec<(u64, f64)>,
    corrected_total: f64,
    last_advance_wall_ms: u64,
    ready: bool,
}

impl LiveEngine {
    pub fn new(
        params: TracerParams,
        cfg: LiveConfig,
        trades_binding: &Path,
        books_binding: Option<&Path>,
        ledger_dir: &Path,
    ) -> anyhow::Result<Self> {
        let sym = params.symbol(&cfg.symbol_stem)?.clone();
        let books = match books_binding {
            Some(root) => Some(BookTailer::new(root)?),
            None => None,
        };
        Ok(Self {
            trades: TradeTailer::new(trades_binding)?,
            books,
            gaps: Vec::new(),
            prints: Vec::new(),
            bars: BarBuilder {
                threshold_usd: sym.bar_threshold_usd,
                cur: None,
                dollars: 0.0,
                px_x_usd: 0.0,
            },
            signal: SignalState {
                buy: 0.0,
                sell: 0.0,
                last_ms: None,
                prev_extreme: false,
            },
            recent_bars: Vec::new(),
            open: Vec::new(),
            ledger: LedgerWriter::new(ledger_dir)?,
            counters: EngineCounters::default(),
            corrected_points: Vec::new(),
            corrected_total: 0.0,
            maker_fee_leg_bps: sym.maker_fee_leg_bps,
            taker_fee_leg_bps: sym.taker_fee_leg_bps,
            trades_root: trades_binding.to_path_buf(),
            books_root: books_binding.map(|p| p.to_path_buf()),
            last_advance_wall_ms: 0,
            ready: false,
            params,
            cfg,
        })
    }

    pub fn merged_watermark_ms(&self) -> u64 {
        match &self.books {
            Some(b) => self.trades.tail.watermark_ms.min(b.tail.watermark_ms),
            None => self.trades.tail.watermark_ms,
        }
    }

    pub fn status(&self, wall_ms: u64) -> EngineStatus {
        EngineStatus {
            schema: "tracer.status.v1",
            mode: "live",
            engine_version: crate::ENGINE_VERSION,
            params_version: self.params.params_version,
            symbol: self.cfg.symbol_stem.clone(),
            queue_verdicts: self.cfg.queue_verdicts,
            trades: StreamStatus {
                watermark_ms: self.trades.tail.watermark_ms,
                last_file: self.trades.tail.last_file.clone(),
                files_read: self.trades.tail.files_read,
            },
            books: self.books.as_ref().map(|b| StreamStatus {
                watermark_ms: b.tail.watermark_ms,
                last_file: b.tail.last_file.clone(),
                files_read: b.tail.files_read,
            }),
            merged_watermark_ms: self.merged_watermark_ms(),
            idle: self.ready
                && wall_ms.saturating_sub(self.last_advance_wall_ms) > self.cfg.idle_after_ms,
            calendar_expired: self.trades.tail.watermark_ms > 0
                && calendar_expired(self.trades.tail.watermark_ms),
            open_trades: self.open.len(),
            counters: EngineCounters {
                duplicates_dropped: self.trades.duplicates_dropped,
                ..self.counters
            },
            corrected_n_kept: self.corrected_points.len(),
            corrected_total_usd: self.corrected_total,
            events_written: self.ledger.events_written,
            ready: self.ready,
        }
    }

    fn base_event(&self, t: &OpenTrade, seq: u32, kind: EventKind, leg: Leg, ts_ms: u64) -> Event {
        let (posted_px, window_end_ms, maker_side) = match &t.phase {
            LegPhase::Entry { window_end_ms } => (
                t.posted_px,
                *window_end_ms,
                MakerSide::entry_for_signal_side(t.side),
            ),
            LegPhase::Exit {
                posted_px,
                grace_end_ms,
                ..
            } => (
                *posted_px,
                *grace_end_ms,
                MakerSide::exit_for_signal_side(t.side),
            ),
        };
        Event {
            schema: EVENT_SCHEMA.into(),
            event_id: event_id(&t.trade_id, seq),
            trade_id: t.trade_id.clone(),
            seq,
            kind,
            leg,
            mode: Mode::Live,
            ts_ms,
            wall_ts_ms: now_wall_ms(),
            engine_version: crate::ENGINE_VERSION.into(),
            params_version: self.params.params_version,
            symbol: self.cfg.symbol_stem.clone(),
            signal_side: t.side,
            maker_side: maker_side.label().into(),
            decile: t.decile,
            score: t.score,
            regime: regime(ts_ms).label().into(),
            posted_px,
            qty_usd: self.params.notional_usd,
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
        }
    }

    fn post_trade(&mut self, bar: &Bar, score: f64, decile: usize) -> anyhow::Result<()> {
        let side: i8 = if decile == 10 { 1 } else { -1 };
        let maker_side = MakerSide::entry_for_signal_side(side);
        let tid = trade_id(&self.cfg.symbol_stem, bar.close_ms, side);
        let depth = if self.cfg.queue_verdicts {
            let d = depth_at_post(self.books.as_ref(), maker_side, bar.vwap, bar.close_ms);
            if d.depth_ahead_usd.is_none() {
                self.counters.book_degraded += 1;
            }
            d
        } else {
            DepthAtPost::default()
        };
        let mut t = OpenTrade {
            trade_id: tid,
            seq: 0,
            side,
            score,
            decile,
            t_post_ms: bar.close_ms,
            posted_px: bar.vwap,
            depth,
            phase: LegPhase::Entry {
                window_end_ms: bar.close_ms + self.params.hold_ms,
            },
        };
        let already_through = match maker_side {
            MakerSide::Buy => bar.last_px < bar.vwap,
            MakerSide::Sell => bar.last_px > bar.vwap,
        };
        let mut e = self.base_event(&t, 0, EventKind::Posted, Leg::Entry, bar.close_ms);
        e.post_state = Some(if already_through {
            PostState::AlreadyThrough
        } else {
            PostState::Passive
        });
        e.depth_ahead_usd = t.depth.depth_ahead_usd;
        e.book_ts_ms = t.depth.book_ts_ms;
        e.book_staleness_ms = t.depth.book_staleness_ms;
        self.ledger.append(&e)?;
        t.seq = 1;
        self.counters.posted += 1;
        self.open.push(t);
        Ok(())
    }

    fn exit_posted_px(&self, t_exit_ms: u64) -> Option<(f64, u64)> {
        let idx = self
            .recent_bars
            .partition_point(|b| b.close_ms <= t_exit_ms);
        if idx == 0 {
            return None;
        }
        let b = &self.recent_bars[idx - 1];
        Some((b.vwap, b.close_ms))
    }

    fn finalize_entries(&mut self, wm: u64) -> anyhow::Result<()> {
        let fillrows = prints_as_fillrows(&self.prints);
        let mut still_open = Vec::new();
        for mut t in std::mem::take(&mut self.open) {
            let LegPhase::Entry { window_end_ms } = t.phase else {
                still_open.push(t);
                continue;
            };
            if wm < window_end_ms + WATERMARK_SLACK_MS {
                still_open.push(t);
                continue;
            }
            let maker_side = MakerSide::entry_for_signal_side(t.side);
            if gap_overlaps(&self.gaps, t.t_post_ms, window_end_ms) {
                let mut e =
                    self.base_event(&t, t.seq, EventKind::Expired, Leg::Entry, window_end_ms);
                e.verdict = Some(Verdict::UnfilledGapped);
                e.gap_flags = vec!["gap_overlap".into()];
                self.ledger.append(&e)?;
                self.counters.entry_gapped += 1;
                continue;
            }
            let scan = scan_maker(
                &fillrows,
                maker_side,
                t.posted_px,
                t.t_post_ms,
                0,
                window_end_ms,
                None,
            );
            let queue = match (self.cfg.queue_verdicts, t.depth.depth_ahead_usd) {
                (true, Some(depth)) => Some(scan_queue(
                    &self.prints,
                    maker_side,
                    t.posted_px,
                    t.t_post_ms,
                    window_end_ms,
                    depth,
                )),
                _ => None,
            };
            let queue_cleared = queue.as_ref().and_then(|q| q.cleared);
            if let Some(q) = &queue {
                let through_hit = scan.through.is_some();
                if through_hit != q.cleared.is_some() {
                    self.counters.queue_vs_through_disagreements += 1;
                }
            }
            if let Some(f) = scan.touch {
                let mut e = self.base_event(&t, t.seq, EventKind::Observed, Leg::Entry, f.ts_ms);
                e.milestone = Some("touch".into());
                e.evidence_px = Some(f.px);
                self.ledger.append(&e)?;
                t.seq += 1;
            }
            let verdict = if scan.through.is_some() {
                Verdict::FilledThrough
            } else if queue_cleared.is_some() {
                Verdict::FilledQueue
            } else {
                Verdict::Unfilled
            };
            let filled = !matches!(verdict, Verdict::Unfilled);
            let (kind, ts) = if filled {
                let ts = scan
                    .through
                    .map(|f| f.ts_ms)
                    .or(queue_cleared.map(|f| f.ts_ms))
                    .unwrap();
                (EventKind::Filled, ts)
            } else {
                (EventKind::Expired, window_end_ms)
            };
            let mut e = self.base_event(&t, t.seq, kind, Leg::Entry, ts);
            e.verdict = Some(verdict);
            e.filled_touch_ts_ms = scan.touch.map(|f| f.ts_ms);
            e.filled_through_ts_ms = scan.through.map(|f| f.ts_ms);
            e.filled_queue_ts_ms = queue_cleared.map(|f| f.ts_ms);
            e.cum_opp_usd = queue.as_ref().map(|q| q.cum_opp_usd);
            if filled {
                e.milestone = Some(if scan.through.is_some() {
                    "through".into()
                } else {
                    "queue_cleared".into()
                });
            }
            self.ledger.append(&e)?;
            self.ledger.flush()?;
            t.seq += 1;
            match verdict {
                Verdict::FilledThrough => self.counters.entry_filled_through += 1,
                Verdict::FilledQueue => self.counters.entry_filled_queue_only += 1,
                _ => {}
            }
            if !filled {
                self.counters.entry_expired_unfilled += 1;
                continue;
            }
            let t_exit_post = t.t_post_ms + self.params.hold_ms;
            let (posted_px, px_ts) = match self.exit_posted_px(t_exit_post) {
                Some(v) => v,
                None => {
                    self.counters.exit_unpriced += 1;
                    continue;
                }
            };
            let stale = t_exit_post.saturating_sub(px_ts) > self.params.exit_staleness_ms;
            if stale {
                self.counters.would_be_voided_by_paper_filter += 1;
            }
            t.phase = LegPhase::Exit {
                t_exit_post_ms: t_exit_post,
                posted_px,
                grace_end_ms: t_exit_post + self.cfg.grace_ms,
            };
            let mut e = self.base_event(&t, t.seq, EventKind::Posted, Leg::Exit, t_exit_post);
            e.exit_price_stale = Some(stale);
            self.ledger.append(&e)?;
            t.seq += 1;
            still_open.push(t);
        }
        self.open = still_open;
        Ok(())
    }

    fn finalize_exits(&mut self, wm: u64) -> anyhow::Result<()> {
        let fillrows = prints_as_fillrows(&self.prints);
        let mut still_open = Vec::new();
        for mut t in std::mem::take(&mut self.open) {
            let LegPhase::Exit {
                t_exit_post_ms,
                posted_px,
                grace_end_ms,
                ..
            } = t.phase
            else {
                still_open.push(t);
                continue;
            };
            let final_bound = grace_end_ms + self.cfg.cross_window_ms + WATERMARK_SLACK_MS;
            if wm < final_bound {
                still_open.push(t);
                continue;
            }
            let maker_side = MakerSide::exit_for_signal_side(t.side);
            let scan = scan_maker(
                &fillrows,
                maker_side,
                posted_px,
                t_exit_post_ms,
                0,
                grace_end_ms,
                None,
            );
            if let Some(f) = scan.touch {
                let mut e = self.base_event(&t, t.seq, EventKind::Observed, Leg::Exit, f.ts_ms);
                e.milestone = Some("touch".into());
                e.evidence_px = Some(f.px);
                self.ledger.append(&e)?;
                t.seq += 1;
            }
            let (exit_px, exit_ts, exit_fee, cross) = if let Some(f) = scan.through {
                self.counters.exit_maker += 1;
                let fee = self.params.notional_usd * self.maker_fee_leg_bps / 1e4;
                (Some(posted_px), f.ts_ms, fee, None)
            } else if let Some(f) = first_opposite_print(
                &fillrows,
                maker_side,
                grace_end_ms,
                grace_end_ms + self.cfg.cross_window_ms,
            ) {
                self.counters.exit_taker_cross += 1;
                let fee = self.params.notional_usd * self.taker_fee_leg_bps / 1e4;
                (Some(f.px), f.ts_ms, fee, Some(f))
            } else {
                self.counters.exit_unpriced += 1;
                (None, grace_end_ms, 0.0, None)
            };
            let (kind, verdict) = if scan.through.is_some() {
                (EventKind::Filled, Verdict::FilledThrough)
            } else {
                (EventKind::Expired, Verdict::Unfilled)
            };
            let terminal_ts = scan.through.map(|f| f.ts_ms).unwrap_or(grace_end_ms);
            let mut e = self.base_event(&t, t.seq, kind, Leg::Exit, terminal_ts);
            e.verdict = Some(verdict);
            e.filled_touch_ts_ms = scan.touch.map(|f| f.ts_ms);
            e.filled_through_ts_ms = scan.through.map(|f| f.ts_ms);
            self.ledger.append(&e)?;
            t.seq += 1;
            if let Some(f) = cross {
                let mut e = self.base_event(&t, t.seq, EventKind::TakerCross, Leg::Exit, f.ts_ms);
                e.cross_px = Some(f.px);
                e.cross_kind = Some("opposite_print".into());
                e.taker_fee_usd = Some(exit_fee);
                self.ledger.append(&e)?;
            }
            self.ledger.flush()?;
            if let Some(px) = exit_px {
                let entry_fee = self.params.notional_usd * self.maker_fee_leg_bps / 1e4;
                let gross = t.side as f64 * (px / t.posted_px - 1.0) * self.params.notional_usd;
                let pnl = gross - entry_fee - exit_fee;
                self.corrected_total += pnl;
                self.corrected_points.push((exit_ts, self.corrected_total));
            }
        }
        self.open = still_open;
        Ok(())
    }

    pub fn tick(&mut self, wall_ms: u64) -> anyhow::Result<()> {
        let wm_before = self.merged_watermark_ms();
        let new_prints = self.trades.poll()?;
        if let Some(b) = self.books.as_mut() {
            b.poll()?;
        }
        let mut gaps = read_gap_spans(&self.trades_root)?;
        if let Some(root) = &self.books_root {
            gaps.extend(read_gap_spans(root)?);
        }
        self.gaps = gaps;
        for p in &new_prints {
            if let Some(bar) = self.bars.push(p) {
                let score = self.signal.update(&bar, self.params.tau_secs);
                let d = decile_of(&self.params.decile_thresholds, score);
                let extreme = d == 0 || d == 9;
                if extreme && !self.signal.prev_extreme {
                    self.post_trade(&bar, score, d + 1)?;
                }
                self.signal.prev_extreme = extreme;
                self.recent_bars.push(bar);
            }
        }
        self.prints.extend(new_prints);
        self.prints
            .sort_by(|a, b| (a.ts_ms, &a.id).cmp(&(b.ts_ms, &b.id)));
        let wm = self.merged_watermark_ms();
        if wm > wm_before || !self.ready {
            self.last_advance_wall_ms = wall_ms;
        }
        self.finalize_entries(wm)?;
        self.finalize_exits(wm)?;
        let retention_ms = self.params.hold_ms
            + self.cfg.grace_ms
            + self.cfg.cross_window_ms
            + 10 * WATERMARK_SLACK_MS;
        let cutoff = wm.saturating_sub(retention_ms);
        self.prints.retain(|p| p.ts_ms >= cutoff);
        self.trades.prune_ids(cutoff);
        if let Some(b) = self.books.as_mut() {
            b.prune(cutoff);
        }
        self.recent_bars.retain(|b| b.close_ms >= cutoff);
        self.ready = true;
        Ok(())
    }
}

fn now_wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{SymbolParams, TracerParams};
    use aetelier_io::trades::trades_parquet::write_trades_parquet;
    use aetelier_types::trades::{Trade, TradeOrigin, TradeSide};
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;

    fn trade(ts_ms: u64, px: f64, sz: f64, buy: bool, id: &str) -> Trade {
        Trade {
            source_trade_ts_us: ts_ms * 1_000,
            local_trade_ts_us: ts_ms * 1_000,
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

    fn test_params() -> TracerParams {
        TracerParams {
            schema: crate::params::SCHEMA.into(),
            params_version: 1,
            tau_secs: 60.0,
            hold_ms: 60_000,
            notional_usd: 1_000.0,
            exit_staleness_ms: 15_000,
            t_split_ms: 0,
            decile_thresholds: vec![-0.3, -0.2, -0.1, -0.05, 0.0, 0.05, 0.1, 0.2, 0.3],
            symbols: vec![SymbolParams {
                stem: "xyz_TSLA".into(),
                bar_threshold_usd: 1_000.0,
                maker_fee_leg_bps: -0.015,
                taker_fee_leg_bps: 1.044,
            }],
            input_sha256: Default::default(),
        }
    }

    fn cfg() -> LiveConfig {
        LiveConfig {
            symbol_stem: "xyz_TSLA".into(),
            grace_ms: 30_000,
            cross_window_ms: 10_000,
            queue_verdicts: false,
            idle_after_ms: 5_000,
        }
    }

    fn fixture(name: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("tracer_eng_{name}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("binding/hyperliquid/perpetual/trades")).unwrap();
        (root.join("binding"), root.join("ledger"))
    }

    fn preamble_and_burst() -> Vec<Trade> {
        vec![
            trade(1_000, 100.0, 5.0, true, "n1"),
            trade(2_000, 100.0, 5.0, false, "n2"),
            trade(3_000, 100.0, 5.0, true, "n3"),
            trade(4_000, 100.0, 5.0, false, "n4"),
            trade(10_000, 100.0, 10.0, true, "burst"),
        ]
    }

    #[test]
    fn watermark_blocks_expiry_under_flush_lag() {
        let (binding, ledger) = fixture("wm");
        let dir = binding.join("hyperliquid/perpetual/trades");
        write_trades_parquet(&preamble_and_burst(), &dir.join("a.parquet")).unwrap();
        let mut eng = LiveEngine::new(test_params(), cfg(), &binding, None, &ledger).unwrap();
        eng.tick(1_000).unwrap();
        assert_eq!(eng.counters.posted, 1);
        assert_eq!(eng.open.len(), 1);

        write_trades_parquet(
            &[
                trade(20_000, 99.0, 1.0, false, "thru"),
                trade(40_000, 100.0, 5.0, true, "wm40"),
            ],
            &dir.join("b.parquet"),
        )
        .unwrap();
        eng.tick(120_000).unwrap();
        assert_eq!(eng.counters.entry_filled_through, 0);
        assert_eq!(eng.counters.entry_expired_unfilled, 0);
        assert_eq!(eng.open.len(), 1);

        write_trades_parquet(
            &[trade(73_000, 100.0, 0.1, true, "wm73")],
            &dir.join("c.parquet"),
        )
        .unwrap();
        eng.tick(121_000).unwrap();
        assert_eq!(eng.counters.entry_filled_through, 1);
        assert_eq!(eng.counters.would_be_voided_by_paper_filter, 1);
        assert_eq!(eng.open.len(), 1);

        write_trades_parquet(
            &[
                trade(80_000, 101.0, 1.0, true, "exit_thru"),
                trade(113_000, 100.0, 0.1, true, "wm113"),
            ],
            &dir.join("d.parquet"),
        )
        .unwrap();
        eng.tick(122_000).unwrap();
        assert_eq!(eng.counters.exit_maker, 1);
        assert_eq!(eng.corrected_points.len(), 1);
        assert!((eng.corrected_total - 0.003).abs() < 1e-9);
        assert!(eng.open.is_empty());
        assert!(eng.ledger.events_written >= 6);
        std::fs::remove_dir_all(binding.parent().unwrap()).unwrap();
    }

    #[test]
    fn gap_overlap_yields_unfilled_gapped_out_of_denominators() {
        let (binding, ledger) = fixture("gap");
        let dir = binding.join("hyperliquid/perpetual/trades");
        let gaps = binding.join("hyperliquid/perpetual/gaps");
        std::fs::create_dir_all(&gaps).unwrap();
        std::fs::write(
            gaps.join("hyperliquid_xyz-TSLA_gap_ledger.jsonl"),
            r#"{"opened_epoch_us":30000000,"closed_epoch_us":40000000,"window_us":10000000,"cause":"disconnect","exchange":"hyperliquid","symbol":"xyz:TSLA","possible_dropped_trades":null}"#,
        )
        .unwrap();
        write_trades_parquet(&preamble_and_burst(), &dir.join("a.parquet")).unwrap();
        write_trades_parquet(
            &[
                trade(20_000, 99.0, 1.0, false, "thru"),
                trade(73_000, 100.0, 0.1, true, "wm73"),
            ],
            &dir.join("b.parquet"),
        )
        .unwrap();
        let mut eng = LiveEngine::new(test_params(), cfg(), &binding, None, &ledger).unwrap();
        eng.tick(1_000).unwrap();
        assert_eq!(eng.counters.entry_gapped, 1);
        assert_eq!(eng.counters.entry_filled_through, 0);
        assert_eq!(eng.counters.entry_expired_unfilled, 0);
        assert!(eng.open.is_empty());
        std::fs::remove_dir_all(binding.parent().unwrap()).unwrap();
    }

    #[test]
    fn verdict_degrades_without_book() {
        let (binding, ledger) = fixture("degrade");
        let dir = binding.join("hyperliquid/perpetual/trades");
        let books_root = binding.parent().unwrap().join("books_binding");
        std::fs::create_dir_all(books_root.join("hyperliquid/perpetual/orderbooks")).unwrap();
        write_trades_parquet(&preamble_and_burst(), &dir.join("a.parquet")).unwrap();
        write_trades_parquet(
            &[
                trade(20_000, 99.0, 1.0, false, "thru"),
                trade(73_000, 100.0, 0.1, true, "wm73"),
            ],
            &dir.join("b.parquet"),
        )
        .unwrap();
        let mut queue_cfg = cfg();
        queue_cfg.queue_verdicts = true;
        let mut eng = LiveEngine::new(
            test_params(),
            queue_cfg,
            &binding,
            Some(&books_root),
            &ledger,
        )
        .unwrap();
        eng.tick(1_000).unwrap();
        assert_eq!(eng.counters.book_degraded, 1);
        assert_eq!(eng.counters.entry_filled_through, 0);
        assert_eq!(eng.open.len(), 1);
        std::fs::remove_dir_all(binding.parent().unwrap()).unwrap();
    }

    #[test]
    fn idle_flags_when_watermark_stalls() {
        let (binding, ledger) = fixture("idle");
        let dir = binding.join("hyperliquid/perpetual/trades");
        write_trades_parquet(&preamble_and_burst(), &dir.join("a.parquet")).unwrap();
        let mut eng = LiveEngine::new(test_params(), cfg(), &binding, None, &ledger).unwrap();
        eng.tick(1_000).unwrap();
        assert!(!eng.status(2_000).idle);
        eng.tick(3_000).unwrap();
        assert!(eng.status(10_000).idle);
        assert_eq!(eng.status(10_000).counters.entry_expired_unfilled, 0);
        std::fs::remove_dir_all(binding.parent().unwrap()).unwrap();
    }
}
