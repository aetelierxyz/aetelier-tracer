use std::path::{Path, PathBuf};

use tracer_engine::nyse::regime;
use tracer_engine::paper::ExecutedTrade;
use tracer_engine::params::TracerParams;
use tracer_engine::replay::paper_trades;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn load_params() -> TracerParams {
    TracerParams::load(&repo_root().join("params/tracer_params.v1.json")).unwrap()
}

const GOLDEN_HEADER: &str = "symbol,side,score,decile,t_entry_ms,entry_px,t_exit_ms,t_exit_real_ms,exit_px,fee_entry_usd,fee_exit_usd,pnl_usd,regime";

struct GoldenRow {
    symbol: String,
    side: i8,
    score: f64,
    decile: usize,
    t_entry_ms: u64,
    entry_px: f64,
    t_exit_ms: u64,
    t_exit_real_ms: u64,
    exit_px: f64,
    fee_entry_usd: f64,
    fee_exit_usd: f64,
    pnl_usd: f64,
    regime: String,
}

fn read_golden(path: &Path) -> Vec<GoldenRow> {
    let text = std::fs::read_to_string(path).unwrap();
    let mut lines = text.lines();
    assert_eq!(lines.next().unwrap(), GOLDEN_HEADER, "{}", path.display());
    lines
        .map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            assert_eq!(c.len(), 13, "bad golden row: {l}");
            GoldenRow {
                symbol: c[0].to_string(),
                side: c[1].parse().unwrap(),
                score: c[2].parse().unwrap(),
                decile: c[3].parse().unwrap(),
                t_entry_ms: c[4].parse().unwrap(),
                entry_px: c[5].parse().unwrap(),
                t_exit_ms: c[6].parse().unwrap(),
                t_exit_real_ms: c[7].parse().unwrap(),
                exit_px: c[8].parse().unwrap(),
                fee_entry_usd: c[9].parse().unwrap(),
                fee_exit_usd: c[10].parse().unwrap(),
                pnl_usd: c[11].parse().unwrap(),
                regime: c[12].to_string(),
            }
        })
        .collect()
}

fn render_golden(trades: &[ExecutedTrade]) -> String {
    let mut out = String::from(GOLDEN_HEADER);
    out.push('\n');
    for t in trades {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            t.symbol,
            t.side,
            t.score,
            t.decile,
            t.t_entry_ms,
            t.entry_px,
            t.t_exit_ms,
            t.t_exit_real_ms,
            t.exit_px,
            t.fee_entry_usd,
            t.fee_exit_usd,
            t.pnl_usd,
            regime(t.t_entry_ms).label()
        ));
    }
    out
}

fn assert_matches(trades: &[ExecutedTrade], golden: &[GoldenRow]) {
    assert_eq!(trades.len(), golden.len(), "trade count mismatch");
    for (i, (t, g)) in trades.iter().zip(golden.iter()).enumerate() {
        assert_eq!(t.symbol, g.symbol, "row {i} symbol");
        assert_eq!(t.side, g.side, "row {i} side");
        assert_eq!(t.score, g.score, "row {i} score");
        assert_eq!(t.decile, g.decile, "row {i} decile");
        assert_eq!(t.t_entry_ms, g.t_entry_ms, "row {i} t_entry_ms");
        assert_eq!(t.entry_px, g.entry_px, "row {i} entry_px");
        assert_eq!(t.t_exit_ms, g.t_exit_ms, "row {i} t_exit_ms");
        assert_eq!(t.t_exit_real_ms, g.t_exit_real_ms, "row {i} t_exit_real_ms");
        assert_eq!(t.exit_px, g.exit_px, "row {i} exit_px");
        assert_eq!(t.fee_entry_usd, g.fee_entry_usd, "row {i} fee_entry_usd");
        assert_eq!(t.fee_exit_usd, g.fee_exit_usd, "row {i} fee_exit_usd");
        assert_eq!(t.pnl_usd, g.pnl_usd, "row {i} pnl_usd");
        assert_eq!(regime(t.t_entry_ms).label(), g.regime, "row {i} regime");
    }
}

#[test]
fn replay_reproduces_14433_paper_trades() {
    let Ok(fills_dir) = std::env::var("TRACER_FILLS_DIR") else {
        eprintln!("TRACER_FILLS_DIR unset; full golden gate skipped");
        return;
    };
    let params = load_params();
    let trades = paper_trades(Path::new(&fills_dir), &params).unwrap();
    assert_eq!(trades.len(), 14_433);
    let golden = read_golden(&repo_root().join("fixtures/paper_trades_golden.csv"));
    assert_matches(&trades, &golden);
}

#[test]
fn corrected_paper_block_matches_performance_md_to_published_precision() {
    let Ok(fills_dir) = std::env::var("TRACER_FILLS_DIR") else {
        eprintln!("TRACER_FILLS_DIR unset; performance.md gate skipped");
        return;
    };
    let params = load_params();
    let trades = paper_trades(Path::new(&fills_dir), &params).unwrap();
    let refs: Vec<&ExecutedTrade> = trades.iter().collect();
    let b = tracer_engine::stats::block(&refs).unwrap();
    assert_eq!(b.n, 14_433);
    assert_eq!(format!("{:+.2}", b.total), "+3794.40");
    assert_eq!(format!("{:+.4}", b.mean_usd), "+0.2629");
    assert_eq!(format!("{:+.2}", b.t_stat), "+7.88");
    assert_eq!(format!("{:.2}", b.win_rate), "0.54");
    assert_eq!(format!("{:.2}", b.profit_factor), "1.26");
    assert_eq!(format!("{:+.3}", b.p05), "-4.902");
    assert_eq!(format!("{:+.3}", b.p50), "+0.132");
    assert_eq!(format!("{:+.3}", b.p95), "+5.653");
    assert_eq!(format!("{:.2}", b.max_dd), "166.34");
    assert_eq!(format!("{:.2}", b.fees_total), "-84.33");
}

#[test]
fn mini_fixture_replay_is_stable() {
    let params = load_params();
    let mini_dir = std::env::temp_dir().join("tracer_mini_fixture_test");
    std::fs::create_dir_all(&mini_dir).unwrap();
    std::fs::copy(
        repo_root().join("fixtures/mini_fills_xyz_TSLA.csv"),
        mini_dir.join("xyz_TSLA.csv"),
    )
    .unwrap();
    let mut mini_params = params.clone();
    mini_params.symbols.retain(|s| s.stem == "xyz_TSLA");
    let trades = paper_trades(&mini_dir, &mini_params).unwrap();
    assert!(!trades.is_empty());

    let expected_path = repo_root().join("fixtures/mini_trades_golden.csv");
    let rendered = render_golden(&trades);
    if std::env::var("TRACER_BLESS").is_ok() {
        std::fs::write(&expected_path, &rendered).unwrap();
        return;
    }
    let golden = read_golden(&expected_path);
    assert_matches(&trades, &golden);
}
