use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub const GOLDEN_FILE: &str = "paper_trades_golden.csv";

pub struct GoldenInputs {
    pub fills_dir: PathBuf,
    pub golden_csv: PathBuf,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn non_empty_env(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

pub fn assert_golden_pinned(golden_csv: &Path) {
    let pins = std::fs::read_to_string(repo_root().join("fixtures/golden.sha256"))
        .expect("fixtures/golden.sha256 missing");
    let mut lines = pins.lines();
    let (want, name) = lines
        .next()
        .and_then(|l| l.split_once("  "))
        .expect("golden.sha256 line must be '<sha256>  <file>'");
    assert!(
        lines.next().is_none(),
        "golden.sha256 must pin exactly one file"
    );
    assert_eq!(name, GOLDEN_FILE, "golden.sha256 pins an unexpected file");
    let bytes = std::fs::read(golden_csv).unwrap_or_else(|e| {
        panic!(
            "golden ledger {} unreadable under TRACER_GOLDEN_DIR: {e}",
            golden_csv.display()
        )
    });
    let got = format!("{:x}", Sha256::digest(&bytes));
    assert_eq!(
        got,
        want,
        "{} does not match its pin in fixtures/golden.sha256",
        golden_csv.display()
    );
}

pub fn golden_inputs(gate: &str) -> Option<GoldenInputs> {
    match (
        non_empty_env("TRACER_FILLS_DIR"),
        non_empty_env("TRACER_GOLDEN_DIR"),
    ) {
        (None, None) => {
            eprintln!("TRACER_FILLS_DIR and TRACER_GOLDEN_DIR unset; {gate} skipped");
            None
        }
        (Some(_), None) => {
            panic!("TRACER_FILLS_DIR is set but TRACER_GOLDEN_DIR is not; {gate} needs both")
        }
        (None, Some(_)) => {
            panic!("TRACER_GOLDEN_DIR is set but TRACER_FILLS_DIR is not; {gate} needs both")
        }
        (Some(fills_dir), Some(golden_dir)) => {
            let golden_csv = golden_dir.join(GOLDEN_FILE);
            assert_golden_pinned(&golden_csv);
            Some(GoldenInputs {
                fills_dir,
                golden_csv,
            })
        }
    }
}
