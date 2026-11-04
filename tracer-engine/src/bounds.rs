pub const MIN_TS_MS: u64 = 946_684_800_000;
pub const MAX_TS_MS: u64 = 4_102_444_800_000;
pub const MAX_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1000;

pub fn ts_in_range(ms: u64) -> bool {
    (MIN_TS_MS..MAX_TS_MS).contains(&ms)
}

pub fn check_window(name: &str, ms: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        ms <= MAX_WINDOW_MS,
        "{name} {ms} exceeds the {MAX_WINDOW_MS} ms cap"
    );
    Ok(())
}

pub fn check_windows(grace_ms: u64, cross_window_ms: u64) -> anyhow::Result<()> {
    check_window("grace_ms", grace_ms)?;
    check_window("cross_window_ms", cross_window_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_outside_2000_to_2100_are_rejected() {
        assert!(ts_in_range(MIN_TS_MS));
        assert!(ts_in_range(1_787_600_000_000));
        assert!(!ts_in_range(MIN_TS_MS - 1));
        assert!(!ts_in_range(MAX_TS_MS));
        assert!(!ts_in_range(u64::MAX));
        assert!(!ts_in_range(0));
    }

    #[test]
    fn windows_are_capped_at_seven_days() {
        assert!(check_windows(300_000, 60_000).is_ok());
        assert!(check_windows(MAX_WINDOW_MS, MAX_WINDOW_MS).is_ok());
        assert!(check_windows(MAX_WINDOW_MS + 1, 0).is_err());
        assert!(check_windows(0, u64::MAX).is_err());
    }
}
