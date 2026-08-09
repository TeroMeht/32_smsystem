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
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  });
  const candles = chart.addCandlestickSeries({
    upColor: '#6ec48b', downColor: '#d47a7a',
    borderUpColor: '#6ec48b', borderDownColor: '#d47a7a',
    wickUpColor: '#6ec48b', wickDownColor: '#d47a7a',
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
    const series = bars.map(b => ({
      time: Math.floor(new Date(b.ts).getTime() / 1000),
      open: b.open, high: b.high, low: b.low, close: b.close,
    }));
    candles.setData(series);
    chart.timeScale().fitContent();
  }

  return { load };
}
