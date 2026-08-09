# Calculation Layer — Test Plan

Scope: `backend/datapipe/calculations.py` plus everything downstream that
consumes it (`bar_processor.process_bar`, `rvol_baseline.rebuild`). Data
source is 1-min Massive bars; every indicator produced here lands in
`livestream` and drives strategy alerts, so wrong numbers here silently
poison every alert.

Run tests: from the project root with the env file loaded,

```
python -m pytest tests/ -q
```

Fast (~2s). No live DB or network required — everything mocks or uses
in-memory frames. New tests should also be pure.

---

## Design principles the tests enforce

1. **Pure and deterministic.** Every function under test takes plain
   values / dataclasses and returns plain values. No I/O, no globals, no
   time-of-day dependency, no RNG.
2. **Bulk == incremental.** For any indicator with both a
   `compute_*_series` (pandas batch) and a `next_*` (per-bar) form,
   feeding the same sequence through both must produce identical values.
   This is what protects against divergence when we change one path.
3. **Behaviour on degenerate input is defined, not accidental.** Zero
   volume, missing history, None ATR, empty frame, single-bar session —
   every one has an explicit test asserting the specific chosen behaviour.
4. **Boundary check math is written out.** When a test asserts a
   specific numeric result, the comment shows the arithmetic so the
   expected value is auditable by eye. No "just copy the current
   output" tests.

---

## What's covered today (`tests/test_calculations.py`, 11 tests)

| Function | Case | Notes |
|---|---|---|
| `next_vwap` | Bulk vs incremental parity over a 5-bar session | Fixture in test file |
| `next_vwap` | Zero-volume bar → returns 0.0 | Divide-by-zero guard |
| `next_ema` | Bulk vs incremental parity over a 5-bar session | span=9, adjust=False |
| `compute_atr_series` | First-row TR collapses to `high − low` (no prev_close) | Documented in 22 as intentional |
| `latest_atr` | Empty frame → None | |
| `next_relatr` | `atr == 0` → None (no divide-by-zero) | |
| `next_relatr` | `atr is None` → None | |
| `next_relatr` | Positive when VWAP above close | Sign check |
| `next_rvol_cum` | Zero baseline (start of session) → 0.0 | 22 semantics |
| `next_rvol_cum` | Ratio math against a manually computed denominator | Auditable |
| `next_rvol_cum` | Current slot has no baseline but prior sum > 0 → stays defined | The bug the accumulator fix solved |
| `next_rvol_cum` | `baseline_slot_avg is None` → treated as 0 | Robust to sparse dict |
| `enrich_bar` | Populates all four indicators; RelATR and RVOL math match hand calc | End-to-end assembly |

Plus `tests/test_bar_processor.py` (2 tests) covering `process_bar` with a
stub asyncpg pool: enrichment + persistence + `baseline_history_sum`
accumulation across two consecutive bars.

**Total: 13 tests passing.**

---

## Gaps to fill

Rank-ordered by risk (highest first).

### 1. `compute_atr_series` — beyond the first-row case

Currently only asserts row 0. A stock with an overnight gap has the
biggest possible TR component (`|H − prev_close|` or `|L − prev_close|`)
on row 1, and that's the exact case ATR14 is meant to capture. Missing.

**Add:**
- Multi-row daily fixture with a real gap-up (prev close 100, next
  open 108, low 107, high 110 → TR = 10, not 3).
- Multi-row daily fixture with a gap-down.
- ATR at row 13 (span=14 stabilised): compare against pandas ewm on the
  same TR series computed by hand.
- Latest ATR on a 14-day frame: known-good value from a spreadsheet.

### 2. `next_vwap` — session boundary and no history

- First bar of a session (empty history) with non-zero volume: VWAP
  should equal that bar's OHLC4 exactly. Currently only zero-volume
  first-bar is covered.
- History plus a zero-volume new bar: VWAP unchanged from the
  history-only VWAP.

### 3. `next_ema` — sensitivity to first value

- Single-bar case: EMA9 equals close. Currently not explicitly asserted
  as its own test (only implied by parity test).
- Feeding the *same close* repeatedly: EMA should converge to that close
  (proves span=9 arithmetic is correct).

### 4. RVOL — session-long walk

Existing tests hit `next_rvol_cum` in isolation. Missing: an end-to-end
"walk a full session through `process_bar` and check that
`rvol_cum` matches a hand-computed golden series for every bar." This is
the highest-value integration test because it's exactly what shipped to
the user's CSV comparison — and where they noticed the earlier bug.

**Add:** a fixture with 10 bars, a hand-authored per-slot baseline dict,
and an expected `rvol_cum` value per bar. Loop `process_bar` with a stub
pool, assert each output.

### 5. `enrich_bar` — every "missing" input path

- `atr is None` → `relatr is None`, other three still fill.
- `history` empty → first-bar semantics for all four.
- `baseline_slot_avg = 0` AND `baseline_history_sum = 0` → `rvol_cum = 0.0`.

Some of these are indirectly covered but not asserted in one place.

### 6. `rvol_baseline.rebuild` — SQL semantics

This is currently untested because it needs Postgres. Options:

- **Skip in unit suite**, cover with a live integration test the user
  runs manually against the dev DB. Add a shell script
  `tests/integration/rebuild_baseline_smoke.sh` that:
  1. Seeds `intraday_bars` with a scripted 7-day scenario (one symbol,
     known volumes at 09:30 and 09:31 ET across sessions).
  2. Calls the rebuild for `end_day = day 6`.
  3. Asserts `SELECT avg_volume, sample_days` matches the hand mean.
- **Fake Postgres** (pgTAP or testcontainers) — bigger lift, more
  reliable in CI. Not worth it right now.

Recommend option 1.

### 7. Timezone edge cases

- A bar whose ET slot straddles DST (early November ET fall-back). The
  ET slot for the same UTC ts differs by an hour before/after. Assert
  `et_time_slot` produces the calendar-correct ET minute in both cases.
- A bar whose UTC ts crosses ET midnight (00:00 ET pre-market wraps
  session_date). Assert `session_date_et` returns the right ET calendar
  date.

Both live in `time_utils.py`, not `calculations.py`, but they feed the
RVOL baseline lookup — a wrong slot lookup produces wrong RVOL. Belongs
in this plan.

### 8. `process_bar` — history de-duplication

Not currently tested: what happens if the same bar (same ts) arrives
twice via WS reconnect? Right now `insert_livestream_bar` upserts (idempotent
in the DB), but `st.history.append` would double-count in memory —
polluting VWAP, EMA9, and RVOL for every subsequent bar.

**Add:** feed the same bar twice, assert `st.history` length stays at 1
and downstream calculations don't drift. If the current code doesn't
guard against this (I believe it doesn't), the test fails and forces us
to add a dedupe check keyed on `(symbolid, ts)`.

---

## Testing methodology

### Fixtures live at module top

`_mk_bar(i, o, h, l, c, v)` builds bars a minute apart starting at a
fixed UTC datetime. `session_bars` is the canonical 5-bar sequence.
Every new test should reuse or extend these — no ad-hoc bar builders
scattered through the file.

### Golden values, not snapshots

Never generate the expected value by running the code being tested.
Always compute it by hand (or in a spreadsheet, or with numpy directly)
and paste it into the assertion. A snapshot test that just captures the
current output can't catch a regression that changes the calculation.

### Comparison tolerance

Financial rounding matters. Assert with `pytest.approx(v, abs=1e-4)` for
prices/indicators (matches the 4-decimal rounding the code uses).
Volumes are integers — use `==`.

### Testing NULL-vs-0.0 boundaries

`next_rvol_cum` returns `0.0` on missing baseline, `next_relatr` returns
`None`. Different semantics because RelATR of 0.0 has a real meaning
(price sitting at VWAP) but RVOL of 0.0 is our "no data yet" sentinel.
Any new test on these functions must respect the distinction.

### Comparing against 22

For a black-box sanity check, `tests/fixtures/22_expected.csv` (not yet
committed) could contain a small hand-migrated slice of 22's output for a
known 5-bar sequence. Load it, feed the same inputs through our
`enrich_bar`, and assert numerically equal (accounting for the granularity
gap — see below).

---

## Note on cross-project comparison

22 produces 2-min bars; 32 produces 1-min. Two 1-min bars pair into one
2-min bar, but the intermediate indicator values on 1-min bars have no
direct counterpart in 22. The tests here do NOT assert equality with 22
across the whole session — only for **fully-comparable moments**:

- First bar of the session (both trivially agree; VWAP = OHLC4, EMA = close).
- Any moment where a 2-min bar's window happens to contain exactly one
  1-min bar (rare, only true in periods with no trades).

For everything else, comparison against 22 is a *directional* check
(same sign, same order of magnitude) rather than a value-equality test.

---

## Running one section at a time

```
python -m pytest tests/test_calculations.py -k "vwap"
python -m pytest tests/test_calculations.py -k "rvol"
python -m pytest tests/test_bar_processor.py
```

Add a `pytest.ini` `markers` list if we want group markers later.

---

## Priority order for implementing the gaps

If you only have time for three:

1. **Gap #4** — RVOL end-to-end session walk. Highest-value integration.
2. **Gap #1** — ATR14 with real gaps. The RelATR denominator depends on
   this being correct and it's the least-covered pure function.
3. **Gap #8** — `process_bar` dedupe. Reconnect scenarios are the most
   likely real-world source of subtle indicator drift.

The others (#2, #3, #5, #6, #7) are worth doing once but lower priority.
