"""
Elevated RVOL table -- display only, never alarmed.

Its single filter (min_rvol) lives in the unified server config next to
the other tables' filters; these tests cover that config section and
guard that the table stays out of the alarm dispatcher.
"""

from __future__ import annotations

from backend.alarms import alarm_config, dispatcher
from backend.alarms.alarm_config import (
    AlarmConfig,
    AlarmConfigUpdate,
    ElevatedRvolFiltersUpdate,
)
from backend.alarms.strategies import capitulation


def test_default_threshold_is_10():
    assert AlarmConfig().elevated_rvol.min_rvol == 10.0


def test_not_alarmed():
    """Only Uptrend Reversals is registered with the alarm dispatcher."""
    assert dispatcher._STRATEGIES == [capitulation.run]


def test_partial_update_and_reset(tmp_path, monkeypatch):
    monkeypatch.setattr(alarm_config, "CONFIG_PATH", tmp_path / "cfg.json")
    alarm_config.set_for_tests(AlarmConfig())

    cfg = alarm_config.update(AlarmConfigUpdate(
        elevated_rvol=ElevatedRvolFiltersUpdate(min_rvol=7.5),
    ))
    assert cfg.elevated_rvol.min_rvol == 7.5
    assert cfg.shorts.max_relatr == -0.40            # other tables untouched

    alarm_config.update(AlarmConfigUpdate(min_relatr=0.9))
    alarm_config.reset("elevated_rvol")
    cfg = alarm_config.get()
    assert cfg.elevated_rvol.min_rvol == 10.0
    assert cfg.min_relatr == 0.9                     # uptrend untouched by this reset


def test_old_config_file_without_section_gets_default(tmp_path, monkeypatch):
    """An alarm_config.json without an elevated_rvol key must load fine."""
    p = tmp_path / "cfg.json"
    p.write_text('{"enabled": true, "min_relatr": 0.4}', encoding="utf-8")
    monkeypatch.setattr(alarm_config, "CONFIG_PATH", p)
    assert alarm_config._load_from_disk().elevated_rvol.min_rvol == 10.0
