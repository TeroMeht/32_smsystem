"""
FastAPI entrypoint.

Two responsibilities:

  * On lifespan startup, call ``datapipe.pipeline.startup_live`` -- this
    creates the DB pool, warms the historian, and spawns the WS livestream
    background task.
  * On lifespan shutdown, cancel it via ``datapipe.pipeline.shutdown``.

The replay endpoint (``POST /replay``) starts a replay run in the
background. Live mode and replay mode aren't run at the same time in
practice; the endpoint is provided as an operator escape hatch. In a
production deploy you'd flip this via a config flag instead.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from backend.common.logging_config import setup_app_logging
from backend.core.config import settings
from backend.database.pool import get_pool
from backend.database.readers import load_top_relatr
from backend.datapipe import pipeline
from backend.datapipe.replay import ReplayConfig

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Root-logger setup so every backend.* module surfaces to stdout + logs/app.log.
    # Called first so a startup failure below is captured in the log.
    setup_app_logging(log_dir=Path("logs"))
    logger.info("=" * 72)
    logger.info("32_smsystem starting up")
    logger.info("=" * 72)
    try:
        await pipeline.startup_live()
        logger.info("32_smsystem ready -- serving HTTP")
        yield
    finally:
        logger.info("32_smsystem shutting down")
        await pipeline.shutdown()
        logger.info("32_smsystem shutdown complete")


app = FastAPI(title="32_smsystem", lifespan=lifespan)


class ReplayRequest(BaseModel):
    day: date            # ET session date to replay, e.g. "2026-08-07"
    speed: float = 60.0  # default 60x = 1 replay minute per real second
    lookback_days: int = 8
    sample_sessions: int = 5


@app.post("/replay")
async def trigger_replay(req: ReplayRequest, background_tasks: BackgroundTasks):
    """
    Ad-hoc replay trigger. Only usable when the app was started in
    MODE=live -- in MODE=replay a replay is already running from startup
    and a second one would clobber the shared session state.
    """
    if settings.MODE == "replay":
        raise HTTPException(
            status_code=409,
            detail=(
                "App is running in MODE=replay -- a replay is already active "
                "(REPLAY_DAY=%s). To run a different replay, restart with "
                "different env vars." % settings.REPLAY_DAY
            ),
        )
    cfg = ReplayConfig(
        day=req.day,
        speed=req.speed,
        lookback_days=req.lookback_days,
        sample_sessions=req.sample_sessions,
    )
    logger.info(
        "[api] /replay requested: day=%s speed=%.2f lookback=%dd sample_sessions=%d",
        req.day, req.speed, req.lookback_days, req.sample_sessions,
    )
    background_tasks.add_task(pipeline.startup_replay, cfg)
    return {
        "status": "scheduled",
        "day": req.day.isoformat(),
        "speed": req.speed,
        "note": "follow progress in the app logs",
    }


@app.get("/health")
async def health():
    return {"ok": True, "mode": settings.MODE, "replay_day": (
        settings.REPLAY_DAY.isoformat() if settings.REPLAY_DAY else None
    )}


# ---------------------------------------------------------------------------
# /relatr -- live dashboard: top-N latest RelATR per symbol, polling refresh
# ---------------------------------------------------------------------------


@app.get("/api/livestream/top")
async def api_livestream_top(
    n: int = Query(20, ge=1, le=200),
    order: str = Query("desc", pattern="^(desc|abs)$"),
    min_volume: int = Query(10_000, ge=0),
    min_rvol: float = Query(2.0, ge=0.0),
):
    """
    Top N symbols by RelATR from the ``livestream`` table, restricted to
    bars with meaningful liquidity (volume >= min_volume) and relative
    volume (rvol_cum >= min_rvol). Both filters can be overridden per
    request; defaults are 10,000 shares and RVOL 2.0.
    """
    pool = get_pool()
    rows = await load_top_relatr(
        pool, n=n, order=order,
        min_volume=min_volume, min_rvol=min_rvol,
    )
    return {
        "n": n, "order": order,
        "min_volume": min_volume, "min_rvol": min_rvol,
        "rows": rows,
    }


_RELATR_HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>RelATR live</title>
<style>
  html, body { margin: 0; padding: 0; background: #0f1115; color: #e6e8ec;
               font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
  .wrap { max-width: 1100px; margin: 24px auto; padding: 0 16px; }
  header { display: flex; align-items: baseline; gap: 16px; margin-bottom: 12px; }
  h1 { font-size: 18px; margin: 0; font-weight: 600; letter-spacing: 0.5px; }
  .meta { font-size: 12px; color: #8a92a5; }
  .controls { margin-left: auto; display: flex; gap: 10px; align-items: center;
              font-size: 12px; color: #8a92a5; }
  select, input { background: #1a1e26; color: #e6e8ec; border: 1px solid #2a2f3a;
                  border-radius: 4px; padding: 3px 6px; font-size: 12px; }
  table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
  th, td { padding: 6px 10px; text-align: right; border-bottom: 1px solid #1e232d; }
  th { text-align: right; font-size: 11px; color: #8a92a5; font-weight: 500;
       text-transform: uppercase; letter-spacing: 0.6px; }
  th:first-child, td:first-child { text-align: left; }
  tr.updated td { background: rgba(80, 200, 120, 0.08); transition: background 0.9s; }
  td.pos { color: #6ec48b; }
  td.neg { color: #d47a7a; }
  .stale { color: #8a92a5; font-style: italic; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Live RelATR</h1>
    <span class="meta" id="meta">loading&hellip;</span>
    <div class="controls">
      Sort:
      <select id="order">
        <option value="desc">RelATR desc (below VWAP)</option>
        <option value="abs">|RelATR| magnitude</option>
      </select>
      Rows:
      <input id="n" type="number" min="1" max="200" value="20" style="width: 60px">
      Refresh:
      <select id="interval">
        <option value="2000">2s</option>
        <option value="5000" selected>5s</option>
        <option value="10000">10s</option>
        <option value="30000">30s</option>
      </select>
    </div>
  </header>
  <table id="tbl">
    <thead>
      <tr>
        <th>Symbol</th>
        <th>Time</th>
        <th>Close</th>
        <th>VWAP</th>
        <th>EMA9</th>
        <th>RVOL</th>
        <th>RelATR</th>
        <th>Volume</th>
      </tr>
    </thead>
    <tbody id="body"></tbody>
  </table>
</div>
<script>
(function () {
  const bodyEl = document.getElementById('body');
  const metaEl = document.getElementById('meta');
  const orderEl = document.getElementById('order');
  const nEl = document.getElementById('n');
  const intervalEl = document.getElementById('interval');
  let last = new Map();  // symbol -> ts, to flash updated rows
  let timer = null;

  function fmt(v, digits) {
    if (v === null || v === undefined) return '-';
    return Number(v).toFixed(digits);
  }
  function fmtInt(v) {
    if (v === null || v === undefined) return '-';
    return Number(v).toLocaleString();
  }
  function fmtTime(iso) {
    if (!iso) return '-';
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  async function tick() {
    const n = Number(nEl.value) || 20;
    const order = orderEl.value;
    try {
      const r = await fetch(`/api/livestream/top?n=${n}&order=${order}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      const now = new Date();
      metaEl.textContent = `${data.rows.length} rows  |  last update ${now.toLocaleTimeString()}`;

      const seen = new Set();
      const frag = document.createDocumentFragment();
      for (const row of data.rows) {
        seen.add(row.symbol);
        const tr = document.createElement('tr');
        const changed = last.get(row.symbol) !== row.ts;
        if (changed) tr.classList.add('updated');
        last.set(row.symbol, row.ts);

        const rel = row.relatr;
        const relClass = rel > 0 ? 'pos' : (rel < 0 ? 'neg' : '');

        tr.innerHTML = `
          <td>${row.symbol}</td>
          <td>${fmtTime(row.ts)}</td>
          <td>${fmt(row.close, 2)}</td>
          <td>${fmt(row.vwap, 2)}</td>
          <td>${fmt(row.ema9, 2)}</td>
          <td>${fmt(row.rvol_cum, 2)}</td>
          <td class="${relClass}">${fmt(row.relatr, 4)}</td>
          <td>${fmtInt(row.volume)}</td>
        `;
        frag.appendChild(tr);
      }
      bodyEl.replaceChildren(frag);
      // prune 'last' map so it doesn't grow across sort changes
      for (const k of Array.from(last.keys())) if (!seen.has(k)) last.delete(k);
    } catch (e) {
      metaEl.innerHTML = `<span class="stale">error: ${e.message}</span>`;
    }
  }

  function reschedule() {
    if (timer) clearInterval(timer);
    timer = setInterval(tick, Number(intervalEl.value));
  }
  orderEl.addEventListener('change', tick);
  nEl.addEventListener('change', tick);
  intervalEl.addEventListener('change', reschedule);

  tick();
  reschedule();
})();
</script>
</body>
</html>
"""


@app.get("/relatr", response_class=HTMLResponse)
async def relatr_page():
    return HTMLResponse(_RELATR_HTML)
