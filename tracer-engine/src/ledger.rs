use std::io::Write;
use std::path::{Path, PathBuf};

pub const EVENT_SCHEMA: &str = "tracer.event.v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    Posted,
    Observed,
    Filled,
    Expired,
    TakerCross,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Leg {
    Entry,
    Exit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Live,
    Replay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostState {
    Passive,
    AlreadyThrough,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Verdict {
    FilledTouch,
    FilledThrough,
    FilledQueue,
    Unfilled,
    UnfilledGapped,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub schema: String,
    pub event_id: String,
    pub trade_id: String,
    pub seq: u32,
    pub kind: EventKind,
    pub leg: Leg,
    pub mode: Mode,
    pub ts_ms: u64,
    pub wall_ts_ms: u64,
    pub engine_version: String,
    pub params_version: u32,
    pub symbol: String,
    pub signal_side: i8,
    pub maker_side: String,
    pub decile: usize,
    pub score: f64,
    pub regime: String,
    pub posted_px: f64,
    pub qty_usd: f64,
    pub window_end_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_state: Option<PostState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth_ahead_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub book_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub book_staleness_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_price_stale: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub milestone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_px: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cum_opp_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_touch_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_through_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_queue_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cross_px: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cross_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taker_fee_usd: Option<f64>,
    pub gap_flags: Vec<String>,
}

pub fn trade_id(symbol: &str, t_post_ms: u64, signal_side: i8) -> String {
    format!(
        "{symbol}|{t_post_ms}|{}",
        if signal_side > 0 { "L" } else { "S" }
    )
}

pub fn event_id(trade_id: &str, seq: u32) -> String {
    format!("{trade_id}|{seq}")
}

fn day_stamp(ts_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_ms as i64)
        .expect("valid ms timestamp")
        .format("%Y%m%d")
        .to_string()
}

pub struct LedgerWriter {
    dir: PathBuf,
    open_day: Option<(String, std::io::BufWriter<std::fs::File>)>,
    pub events_written: u64,
}

impl LedgerWriter {
    pub fn new(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            open_day: None,
            events_written: 0,
        })
    }

    pub fn append(&mut self, event: &Event) -> anyhow::Result<()> {
        let day = day_stamp(event.ts_ms);
        let rotate = match &self.open_day {
            Some((open, _)) => *open != day,
            None => true,
        };
        if rotate {
            if let Some((_, mut w)) = self.open_day.take() {
                w.flush()?;
            }
            let path = self.dir.join(format!("events_{day}.jsonl"));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            self.open_day = Some((day, std::io::BufWriter::new(file)));
        }
        let (_, w) = self.open_day.as_mut().unwrap();
        serde_json::to_writer(&mut *w, event)?;
        w.write_all(b"\n")?;
        self.events_written += 1;
        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        if let Some((_, w)) = self.open_day.as_mut() {
            w.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(ts_ms: u64, seq: u32) -> Event {
        let tid = trade_id("xyz_TSLA", 1_755_000_000_000, 1);
        Event {
            schema: EVENT_SCHEMA.into(),
            event_id: event_id(&tid, seq),
            trade_id: tid,
            seq,
            kind: EventKind::Posted,
            leg: Leg::Entry,
            mode: Mode::Replay,
            ts_ms,
            wall_ts_ms: ts_ms,
            engine_version: "0.1.0".into(),
            params_version: 1,
            symbol: "xyz_TSLA".into(),
            signal_side: 1,
            maker_side: "buy".into(),
            decile: 10,
            score: 0.91,
            regime: "EXT".into(),
            posted_px: 330.39,
            qty_usd: 1_000.0,
            window_end_ms: ts_ms + 900_000,
            post_state: Some(PostState::Passive),
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

    #[test]
    fn events_round_trip_and_omit_absent_options() {
        let e = event(1_755_000_000_000, 0);
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"kind\":\"POSTED\""));
        assert!(json.contains("\"post_state\":\"passive\""));
        assert!(!json.contains("depth_ahead_usd"));
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(back.trade_id, e.trade_id);
        assert_eq!(back.kind, EventKind::Posted);
    }

    #[test]
    fn writer_rotates_by_utc_day() {
        let dir = std::env::temp_dir().join("tracer_ledger_rotation_test");
        let _ = std::fs::remove_dir_all(&dir);
        let mut w = LedgerWriter::new(&dir).unwrap();
        w.append(&event(1_755_000_000_000, 0)).unwrap();
        w.append(&event(1_755_100_000_000, 1)).unwrap();
        w.flush().unwrap();
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["events_20250812.jsonl", "events_20250813.jsonl"]
        );
        assert_eq!(w.events_written, 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
