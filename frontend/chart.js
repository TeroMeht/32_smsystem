/**
 * Candlestick chart component.
 *
 * Wraps TradingView's lightweight-charts. The container/title/info DOM
 * elements are passed in by the host page; this module owns nothing on
 * the page except its own render target.
 *
 * Usage:
 *
 *   import { initChart } from '/ui/chart.js';
 *   const chart = initChart({
 *     containerEl: document.getElementById('chart'),
 *     titleEl:     document.getElementById('chartTitle'),
 *     infoEl:      document.getElementById('chartInfo'),
 *   });
 *   chart.load('DUOL');
 *
 * Depends on window.LightweightCharts being loaded already (via the
 * <script> tag in the host page).
 */

export function initChart({ containerEl, titleEl, infoEl, cacheTtlMs = 10_000 }) {
  // Both the axis tick labels and the crosshair tooltip need explicit
  // formatters -- otherwise lightweight-charts renders the numeric `time`
  // (unix seconds) in UTC on the axis, which desynchs against the table
  // that uses browser-local time. These two formatters push everything
  // to the browser's local timezone (Helsinki for the trader).
  // Manual HH:MM padding rather than toLocaleTimeString({ hour12: false })
  // because some locales still coerce hour:'2-digit' back to 12-hour with
  // an AM/PM suffix. Manual formatting is bulletproof.
  const pad = (n) => String(n).padStart(2, '0');
  const tickFmt = (unixSec) => {
    const d = new Date(unixSec * 1000);
    return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };
  const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const crosshairFmt = (unixSec) => {
    const d = new Date(unixSec * 1000);
    return `${MONTHS[d.getMonth()]} ${d.getDate()}, ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };

  const chart = LightweightCharts.createChart(containerEl, {
    layout: { background: { color: '#12161d' }, textColor: '#c8ccd6' },
    grid: {
      vertLines: { color: '#1e232d' },
      horzLines: { color: '#1e232d' },
    },
    rightPriceScale: { borderColor: '#2a2f3a' },
    timeScale: {
      borderColor: '#2a2f3a',
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: tickFmt,
    },
    localization: {
      timeFormatter: crosshairFmt,
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  });
  // Brighter, higher-contrast candle colors than the muted defaults.
  const UP_COLOR   = '#22e56d';   // bright green
  const DOWN_COLOR = '#ff4757';   // bright red

  const candles = chart.addCandlestickSeries({
    upColor: UP_COLOR,   downColor: DOWN_COLOR,
    borderUpColor: UP_COLOR, borderDownColor: DOWN_COLOR,
    wickUpColor: UP_COLOR,   wickDownColor: DOWN_COLOR,
  });

  // Indicator overlays. Each is its own series so lightweight-charts can
  // interpolate cleanly and so we can toggle/style them independently.
  const vwapLine = chart.addLineSeries({
    color: DOWN_COLOR,    // same bright red as down candles -- session VWAP
    lineWidth: 1,
    priceLineVisible: false,
    lastValueVisible: false,
  });
  const ema9Line = chart.addLineSeries({
    color: '#6aa7ff',     // blue -- EMA9
    lineWidth: 1,
    priceLineVisible: false,
    lastValueVisible: false,
  });

  // Fit to container width; keep in sync on resize.
  chart.applyOptions({ width: containerEl.clientWidth, height: 360 });
  window.addEventListener('resize', () => {
    chart.applyOptions({ width: containerEl.clientWidth });
  });

  // Short cache: livestream ticks each minute; anything longer than this
  // means the chart lags visibly behind the table's own row updates.
  const cache = new Map();  // symbol -> { data, fetchedAt }

  async function load(symbol) {
    titleEl.textContent = symbol;
    infoEl.textContent = 'loading…';

    const now = Date.now();
    const cached = cache.get(symbol);
    if (cached && (now - cached.fetchedAt) < cacheTtlMs) {
      _render(cached.data);
      infoEl.textContent = `${cached.data.length} bars this session  |  cached`;
      return;
    }

    try {
      const r = await fetch(`/api/livestream/bars/${symbol}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      const bars = data.bars || [];
      cache.set(symbol, { data: bars, fetchedAt: Date.now() });
      _render(bars);
      infoEl.textContent = `${bars.length} bars this session`;
    } catch (e) {
      infoEl.innerHTML = `<span class="stale">error: ${e.message}</span>`;
    }
  }

  function _render(bars) {
    // lightweight-charts wants time as unix seconds (UTC).
    const toSec = (iso) => Math.floor(new Date(iso).getTime() / 1000);

    const candleSeries = bars.map(b => ({
      time: toSec(b.ts),
      open: b.open, high: b.high, low: b.low, close: b.close,
    }));
    // Indicator series: skip bars where the value is missing so
    // lightweight-charts draws a gap rather than a spurious 0.
    const vwapSeries = bars
      .filter(b => b.vwap !== null && b.vwap !== undefined)
      .map(b => ({ time: toSec(b.ts), value: b.vwap }));
    const ema9Series = bars
      .filter(b => b.ema9 !== null && b.ema9 !== undefined)
      .map(b => ({ time: toSec(b.ts), value: b.ema9 }));

    candles.setData(candleSeries);
    vwapLine.setData(vwapSeries);
    ema9Line.setData(ema9Series);
    // Refit BOTH axes -- otherwise switching from a $700 stock to an $8
    // stock leaves the price axis pinned to the old range and forces the
    // user to zoom/pan manually.
    chart.timeScale().fitContent();
    chart.priceScale('right').applyOptions({ autoScale: true });
  }

  return { load };
}
