use crate::bars::FillRow;

pub const DELTAS_MS: [u64; 3] = [0, 250, 1000];
pub const MARKOUT_WINDOWS_MS: [u64; 2] = [60_000, 300_000];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MakerSide {
    Buy,
    Sell,
}

impl MakerSide {
    pub fn label(&self) -> &'static str {
        match self {
            MakerSide::Buy => "buy",
            MakerSide::Sell => "sell",
        }
    }

    pub fn entry_for_signal_side(side: i8) -> MakerSide {
        if side > 0 {
            MakerSide::Buy
        } else {
            MakerSide::Sell
        }
    }

    pub fn exit_for_signal_side(side: i8) -> MakerSide {
        if side > 0 {
            MakerSide::Sell
        } else {
            MakerSide::Buy
        }
    }
}

fn eps(px: f64) -> f64 {
    1e-9 * px.abs()
}

fn opposite_aggressor(maker_side: MakerSide, trade_is_buy: bool) -> bool {
    match maker_side {
        MakerSide::Buy => !trade_is_buy,
        MakerSide::Sell => trade_is_buy,
    }
}

fn at_or_through(maker_side: MakerSide, px: f64, posted: f64) -> bool {
    match maker_side {
        MakerSide::Buy => px <= posted + eps(posted),
        MakerSide::Sell => px >= posted - eps(posted),
    }
}

fn strictly_through(maker_side: MakerSide, px: f64, posted: f64) -> bool {
    match maker_side {
        MakerSide::Buy => px < posted - eps(posted),
        MakerSide::Sell => px > posted + eps(posted),
    }
}

fn already_through(maker_side: MakerSide, last_px: f64, posted: f64) -> bool {
    strictly_through(maker_side, last_px, posted)
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Fill {
    pub ts_ms: u64,
    pub px: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MakerScan {
    pub already_through: bool,
    pub touch: Option<Fill>,
    pub through: Option<Fill>,
}

fn idx_after(trades: &[FillRow], ts_ms: u64) -> usize {
    trades.partition_point(|r| r.time_ms <= ts_ms)
}

fn last_px_at_or_before(trades: &[FillRow], ts_ms: u64) -> Option<f64> {
    let idx = idx_after(trades, ts_ms);
    if idx == 0 {
        None
    } else {
        Some(trades[idx - 1].px)
    }
}

pub fn scan_maker(
    trades: &[FillRow],
    maker_side: MakerSide,
    posted_px: f64,
    t_post_ms: u64,
    delta_ms: u64,
    window_end_ms: u64,
    post_last_px: Option<f64>,
) -> MakerScan {
    let t_start = t_post_ms + delta_ms;
    let last_px = if delta_ms == 0 {
        post_last_px.or_else(|| last_px_at_or_before(trades, t_start))
    } else {
        last_px_at_or_before(trades, t_start).or(post_last_px)
    };
    let mut scan = MakerScan {
        already_through: last_px.is_some_and(|px| already_through(maker_side, px, posted_px)),
        ..MakerScan::default()
    };
    for r in &trades[idx_after(trades, t_start)..] {
        if r.time_ms > window_end_ms {
            break;
        }
        if !opposite_aggressor(maker_side, r.is_buy) {
            continue;
        }
        if scan.touch.is_none() && at_or_through(maker_side, r.px, posted_px) {
            scan.touch = Some(Fill {
                ts_ms: r.time_ms,
                px: r.px,
            });
        }
        if strictly_through(maker_side, r.px, posted_px) {
            scan.through = Some(Fill {
                ts_ms: r.time_ms,
                px: r.px,
            });
            break;
        }
    }
    scan
}

pub fn first_opposite_trade(
    trades: &[FillRow],
    maker_side: MakerSide,
    after_ms: u64,
    until_ms: u64,
) -> Option<Fill> {
    trades[idx_after(trades, after_ms)..]
        .iter()
        .take_while(|r| r.time_ms <= until_ms)
        .find(|r| opposite_aggressor(maker_side, r.is_buy))
        .map(|r| Fill {
            ts_ms: r.time_ms,
            px: r.px,
        })
}

pub fn markout_bps(
    trades: &[FillRow],
    fill: Fill,
    window_ms: u64,
    position_dir: i8,
) -> Option<f64> {
    let idx = idx_after(trades, fill.ts_ms + window_ms);
    if idx == 0 {
        return None;
    }
    let r = &trades[idx - 1];
    if r.time_ms <= fill.ts_ms {
        return None;
    }
    Some(position_dir as f64 * (r.px / fill.px - 1.0) * 1e4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tape_trade(ts: u64, px: f64, is_buy: bool) -> FillRow {
        FillRow {
            time_ms: ts,
            px,
            sz: 1.0,
            is_buy,
        }
    }

    #[test]
    fn maker_buy_touch_and_through_boundaries() {
        let trades = vec![
            tape_trade(1_000, 100.10, false),
            tape_trade(2_000, 100.00, false),
            tape_trade(3_000, 99.99, false),
        ];
        let s = scan_maker(&trades, MakerSide::Buy, 100.0, 0, 0, 10_000, Some(100.10));
        assert!(!s.already_through);
        assert_eq!(s.touch.unwrap().ts_ms, 2_000);
        assert_eq!(s.through.unwrap().ts_ms, 3_000);
    }

    #[test]
    fn same_side_trades_never_fill() {
        let trades = vec![tape_trade(1_000, 99.0, true), tape_trade(2_000, 98.0, true)];
        let s = scan_maker(&trades, MakerSide::Buy, 100.0, 0, 0, 10_000, Some(101.0));
        assert!(s.touch.is_none());
        assert!(s.through.is_none());
    }

    #[test]
    fn maker_sell_orientation_mirrors() {
        let trades = vec![
            tape_trade(1_000, 100.00, true),
            tape_trade(2_000, 100.01, true),
        ];
        let s = scan_maker(&trades, MakerSide::Sell, 100.0, 0, 0, 10_000, Some(99.9));
        assert_eq!(s.touch.unwrap().ts_ms, 1_000);
        assert_eq!(s.through.unwrap().ts_ms, 2_000);
        assert!(!s.already_through);
    }

    #[test]
    fn epsilon_tie_counts_as_touch_not_through() {
        let posted = 330.39;
        let tie = posted + 1e-12;
        let trades = vec![tape_trade(1_000, tie, false)];
        let s = scan_maker(&trades, MakerSide::Buy, posted, 0, 0, 10_000, Some(331.0));
        assert!(s.touch.is_some());
        assert!(s.through.is_none());
    }

    #[test]
    fn already_through_uses_post_last_px_at_delta_zero_and_reevaluates_at_delta() {
        let trades = vec![tape_trade(100, 99.0, false), tape_trade(400, 101.0, false)];
        let s0 = scan_maker(&trades, MakerSide::Buy, 100.0, 0, 0, 10_000, Some(99.5));
        assert!(s0.already_through);
        let s250 = scan_maker(&trades, MakerSide::Buy, 100.0, 0, 250, 10_000, Some(99.5));
        assert!(s250.already_through);
        let s1000 = scan_maker(&trades, MakerSide::Buy, 100.0, 0, 1_000, 10_000, Some(99.5));
        assert!(!s1000.already_through);
    }

    #[test]
    fn window_bounds_are_exclusive_start_inclusive_end() {
        let trades = vec![
            tape_trade(0, 99.0, false),
            tape_trade(10_000, 99.0, false),
            tape_trade(10_001, 99.0, false),
        ];
        let s = scan_maker(&trades, MakerSide::Buy, 100.0, 0, 0, 10_000, Some(101.0));
        assert_eq!(s.through.unwrap().ts_ms, 10_000);
        let none = scan_maker(
            &trades,
            MakerSide::Buy,
            100.0,
            10_001,
            0,
            10_000,
            Some(101.0),
        );
        assert!(none.through.is_none());
    }

    #[test]
    fn taker_cross_finds_first_opposite_within_window() {
        let trades = vec![
            tape_trade(1_000, 100.0, false),
            tape_trade(2_000, 99.5, true),
            tape_trade(3_000, 99.4, true),
        ];
        let f = first_opposite_trade(&trades, MakerSide::Sell, 1_000, 60_000).unwrap();
        assert_eq!(f.ts_ms, 2_000);
        assert!(first_opposite_trade(&trades, MakerSide::Sell, 3_000, 60_000).is_none());
    }

    #[test]
    fn markout_signs_by_position_direction_and_needs_a_later_trade() {
        let trades = vec![
            tape_trade(1_000, 100.0, false),
            tape_trade(50_000, 101.0, true),
        ];
        let fill = Fill {
            ts_ms: 1_000,
            px: 100.0,
        };
        let m = markout_bps(&trades, fill, 60_000, 1).unwrap();
        assert!((m - 100.0).abs() < 1e-9);
        let m_short = markout_bps(&trades, fill, 60_000, -1).unwrap();
        assert!((m_short + 100.0).abs() < 1e-9);
        assert!(markout_bps(&trades[..1], fill, 60_000, 1).is_none());
    }
}
