use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn tracked_files() -> Vec<PathBuf> {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in ["fixtures", "fixtures/api", "params"] {
        for entry in std::fs::read_dir(root.join(dir)).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() && path.file_name().is_none_or(|n| n != "MANIFEST.sha256") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn rel(path: &Path) -> String {
    let root = repo_root().canonicalize().unwrap();
    path.canonicalize()
        .unwrap()
        .strip_prefix(&root)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/")
}

fn manifest_lines() -> String {
    let mut out = String::new();
    for path in tracked_files() {
        let bytes = std::fs::read(&path).unwrap();
        let hash = Sha256::digest(&bytes);
        out.push_str(&format!("{:x}  {}\n", hash, rel(&path)));
    }
    out
}

#[test]
fn fixtures_match_committed_manifest() {
    let manifest_path = repo_root().join("fixtures/MANIFEST.sha256");
    let got = manifest_lines();
    if std::env::var("TRACER_BLESS").is_ok() {
        std::fs::write(&manifest_path, &got).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&manifest_path)
        .expect("fixtures/MANIFEST.sha256 missing; regenerate with TRACER_BLESS=1");
    assert_eq!(
        got, expected,
        "fixture tree drifted from fixtures/MANIFEST.sha256; regenerate with TRACER_BLESS=1 after verifying the change is intended"
    );
}
