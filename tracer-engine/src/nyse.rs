use std::collections::HashSet;
use std::sync::LazyLock;

use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Weekday};
use chrono_tz::America::New_York;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Regime {
    Rth,
    Ext,
    Wknd,
}

impl Regime {
    pub fn label(&self) -> &'static str {
        match self {
            Regime::Rth => "RTH",
            Regime::Ext => "EXT",
            Regime::Wknd => "WKND",
        }
    }
}

#[derive(serde::Deserialize)]
struct CalendarFile {
    #[allow(dead_code)]
    source: String,
    valid_through: NaiveDate,
    holidays: Vec<NaiveDate>,
    early_closes: Vec<NaiveDate>,
}

struct Calendar {
    valid_through: NaiveDate,
    holidays: HashSet<NaiveDate>,
    early_closes: HashSet<NaiveDate>,
}

static CALENDAR: LazyLock<Calendar> = LazyLock::new(|| {
    let file: CalendarFile = serde_json::from_str(include_str!("nyse_calendar.json"))
        .expect("embedded nyse_calendar.json parses");
    Calendar {
        valid_through: file.valid_through,
        holidays: file.holidays.into_iter().collect(),
        early_closes: file.early_closes.into_iter().collect(),
    }
});

fn is_holiday(d: NaiveDate) -> bool {
    CALENDAR.holidays.contains(&d)
}

fn close_minute(d: NaiveDate) -> u32 {
    if CALENDAR.early_closes.contains(&d) {
        13 * 60
    } else {
        16 * 60
    }
}

pub const OUT_OF_RANGE_LABEL: &str = "OUT_OF_RANGE";

fn new_york(ms_utc: u64) -> Option<chrono::DateTime<chrono_tz::Tz>> {
    New_York
        .timestamp_millis_opt(i64::try_from(ms_utc).ok()?)
        .single()
}

pub fn calendar_expired(ms_utc: u64) -> bool {
    new_york(ms_utc).is_none_or(|ny| ny.date_naive() > CALENDAR.valid_through)
}

pub fn regime_label(ms_utc: u64) -> &'static str {
    regime(ms_utc).map_or(OUT_OF_RANGE_LABEL, |r| r.label())
}

pub fn regime(ms_utc: u64) -> Option<Regime> {
    let ny = new_york(ms_utc)?;
    let date = ny.date_naive();
    let minute = ny.hour() * 60 + ny.minute();
    let wd = ny.weekday();

    let in_wknd = match wd {
        Weekday::Sat | Weekday::Sun => true,
        Weekday::Fri => minute >= close_minute(date),
        Weekday::Mon => minute < 9 * 60 + 30,
        _ => false,
    };
    if in_wknd {
        return Some(Regime::Wknd);
    }
    let trading_day = !is_holiday(date);
    if trading_day && minute >= 9 * 60 + 30 && minute < close_minute(date) {
        Some(Regime::Rth)
    } else {
        Some(Regime::Ext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_range_timestamps_have_no_regime() {
        assert_eq!(regime(u64::MAX), None);
        assert_eq!(regime(i64::MAX as u64), None);
        assert_eq!(regime_label(u64::MAX), OUT_OF_RANGE_LABEL);
        assert!(calendar_expired(u64::MAX));
    }

    fn ms(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> u64 {
        New_York
            .with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .unwrap()
            .timestamp_millis() as u64
    }

    #[test]
    fn regimes_partition_the_week() {
        assert_eq!(regime(ms(2026, 8, 12, 10, 0)), Some(Regime::Rth));
        assert_eq!(regime(ms(2026, 8, 12, 8, 0)), Some(Regime::Ext));
        assert_eq!(regime(ms(2026, 8, 12, 17, 0)), Some(Regime::Ext));
        assert_eq!(regime(ms(2026, 8, 14, 16, 1)), Some(Regime::Wknd));
        assert_eq!(regime(ms(2026, 8, 15, 12, 0)), Some(Regime::Wknd));
        assert_eq!(regime(ms(2026, 8, 16, 23, 0)), Some(Regime::Wknd));
        assert_eq!(regime(ms(2026, 8, 17, 9, 29)), Some(Regime::Wknd));
        assert_eq!(regime(ms(2026, 8, 17, 9, 30)), Some(Regime::Rth));
        assert_eq!(regime(ms(2026, 5, 25, 11, 0)), Some(Regime::Ext));
        assert_eq!(regime(ms(2026, 6, 19, 11, 0)), Some(Regime::Ext));
    }

    #[test]
    fn early_closes_shift_the_boundary() {
        assert_eq!(regime(ms(2026, 12, 24, 12, 59)), Some(Regime::Rth));
        assert_eq!(regime(ms(2026, 12, 24, 13, 0)), Some(Regime::Ext));
        assert_eq!(regime(ms(2026, 11, 27, 13, 30)), Some(Regime::Wknd));
    }

    #[test]
    fn calendar_covers_2027() {
        assert_eq!(regime(ms(2027, 7, 5, 11, 0)), Some(Regime::Ext));
        assert_eq!(regime(ms(2027, 11, 25, 11, 0)), Some(Regime::Ext));
        assert_eq!(regime(ms(2027, 11, 26, 13, 30)), Some(Regime::Wknd));
        assert_eq!(regime(ms(2027, 7, 6, 11, 0)), Some(Regime::Rth));
    }

    #[test]
    fn expiry_flag_flips_past_valid_through() {
        assert!(!calendar_expired(ms(2027, 12, 30, 12, 0)));
        assert!(calendar_expired(ms(2028, 1, 3, 12, 0)));
    }
}
