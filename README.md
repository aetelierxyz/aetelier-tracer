# aetelier-tracer

Fill-probability measurement engine. Replays paper trading signals against recorded venue trade and order-book tapes, classifies each posted maker order as filled through, filled touch or unfilled, and recomputes the paper ledger with unfilled entries dropped. It never places orders.

- `tracer-engine/`: signal core, verdicts, event ledger, replay, corrected statistics.
- `tracerd/`: binary with `run`, `replay`, `serve`, `export-golden` and `evaluate`.
- `params/tracer_params.v1.json`: frozen study parameters.
- `fixtures/`: one-day `mini_fills_xyz_TSLA.csv` and API fixtures for the test suite.

Build and test: `cargo test --locked --workspace` (Rust 1.99.0, pinned in `rust-toolchain.toml`). The full parity gates run only when `TRACER_FILLS_DIR`, `TRACER_REPLAY_DIR` and `TRACER_GOLDEN_DIR` point at the private study export, frozen replay and golden ledger.

License: Apache-2.0.
