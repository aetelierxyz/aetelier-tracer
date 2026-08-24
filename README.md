# aetelier-tracer

Fill-probability truth instrument for the flow-after-hours signal: measures whether posted maker orders would actually have filled, recomputes the paper ledger with unfilled signals dropped, and serves the posted → observed → filled/expired lifecycle. Never places orders.

- `tracer-engine/` — library: ported signal core, verdicts, event ledger, replay, corrected stats.
- `tracerd/` — binary: `run | replay | serve | export-golden`.
- `params/tracer_params.v1.json` — frozen study parameters; refits are new artifact versions, never in-place.
- `fixtures/` — `paper_trades_golden.csv` (the paper's 14,433 trades, replay parity gate) and `mini_fills_xyz_TSLA.csv` (1-day CI fixture, 2026-07-15 UTC).

