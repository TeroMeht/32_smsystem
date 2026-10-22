"""
Calculation layer.

Two modules:

  * ``calculations`` -- per-bar indicator math (VWAP, EMA9, ATR14,
                        cumulative RVOL). Pure functions; no I/O.
  * ``rvol_baseline`` -- the whole rvol_baseline table model: pandas
                         compute pipeline + rebuild orchestrator.
  * ``speed``         -- EMA9-anchored speed of the move (%/bar), one
                         tracker per symbol fed bar by bar.
"""
