"""
Intraday chart image for Telegram alarms.

Ported from 22_WatchlistStreamer/src/alarms/alarm_plotchart.py (the
same 3-panel layout: candles + VWAP + EMA9 / volume / relatr with the
0 and +-0.5 guide lines and the latest relatr value annotated).

32-specific adaptations:
    * input rows come from ``readers.load_livestream_bars_for_symbol``
      (``ts`` ISO strings, no ``symbol`` column) -- converted to a
      Helsinki-local ``time`` column so the chart matches the dashboard,
    * symbol + alarm bar are passed in explicitly; the alarm bar is
      marked with a vertical line,
    * ``render_png`` wraps ``plotly.io.to_image`` (kaleido) for the
      async caller, which runs it in a worker thread.

Rendering needs ``kaleido`` (>= 1.0), which drives a local Chrome /
Chromium. If none is installed, run once in the venv:
    .venv\\Scripts\\python -c "import kaleido; kaleido.get_chrome_sync()"
A rendering failure never blocks the alarm -- the generator falls back
to a text-only Telegram message.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

_LOCAL_TZ = "Europe/Helsinki"


def bars_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Reader rows -> DataFrame with a tz-naive Helsinki ``time`` column."""
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["time"] = (
        pd.to_datetime(df["ts"], utc=True)
        .dt.tz_convert(_LOCAL_TZ)
        .dt.tz_localize(None)
    )
    return df


def plot_intraday_chart(
    df_intraday: pd.DataFrame,
    symbol: str,
    alarm_ts: Optional[datetime] = None,
) -> go.Figure:
    if df_intraday.empty:
        return go.Figure()

    last = df_intraday.iloc[-1]

    fig_intraday = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=[f"{symbol}, Time={last['time']:%Y-%m-%d %H:%M}"],
    )

    # Candlestick
    fig_intraday.add_trace(go.Candlestick(
        x=df_intraday["time"],
        open=df_intraday["open"],
        high=df_intraday["high"],
        low=df_intraday["low"],
        close=df_intraday["close"],
        name="OHLC",
    ), row=1, col=1)

    # VWAP
    if "vwap" in df_intraday.columns:
        fig_intraday.add_trace(go.Scatter(
            x=df_intraday["time"],
            y=df_intraday["vwap"],
            mode="lines",
            line=dict(color="red", width=2),
            name="vwap",
        ), row=1, col=1)

    # EMA9
    if "ema9" in df_intraday.columns:
        fig_intraday.add_trace(go.Scatter(
            x=df_intraday["time"],
            y=df_intraday["ema9"],
            mode="lines",
            line=dict(color="purple", width=1),
            name="ema9",
        ), row=1, col=1)

    # Volume
    fig_intraday.add_trace(go.Bar(
        x=df_intraday["time"],
        y=df_intraday["volume"],
        marker_color="blue",
        name="volume",
    ), row=2, col=1)

    # Relatr
    if "relatr" in df_intraday.columns:
        fig_intraday.add_trace(go.Scatter(
            x=df_intraday["time"],
            y=df_intraday["relatr"],
            mode="lines",
            line=dict(color="green", width=2),
            name="relatr",
        ), row=3, col=1)

        for y_val in [0, 0.5, -0.5]:
            fig_intraday.add_shape(
                type="line",
                x0=df_intraday["time"].min(),
                x1=df_intraday["time"].max(),
                y0=y_val,
                y1=y_val,
                line=dict(color="black", width=1, dash="dash"),
                xref="x3", yref="y3",
            )

        latest_value = df_intraday["relatr"].iloc[-1]
        if latest_value is not None and pd.notna(latest_value):
            fig_intraday.add_annotation(
                x=last["time"],
                y=latest_value,
                xref="x3", yref="y3",
                text=f"{latest_value:.2f}",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                ax=40,
                ay=0,
                font=dict(color="black", size=15),
                bgcolor="rgba(255,255,255,0.7)",
            )

    # Alarm bar marker across all three panels.
    if alarm_ts is not None:
        alarm_local = (
            pd.Timestamp(alarm_ts).tz_convert(_LOCAL_TZ).tz_localize(None)
            if pd.Timestamp(alarm_ts).tzinfo is not None
            else pd.Timestamp(alarm_ts)
        )
        for ref in ("", "2", "3"):
            fig_intraday.add_shape(
                type="line",
                x0=alarm_local, x1=alarm_local,
                y0=0, y1=1,
                xref=f"x{ref}", yref=f"y{ref} domain",
                line=dict(color="orange", width=1, dash="dot"),
            )

    fig_intraday.update_layout(
        height=800,
        width=1100,
        showlegend=False,
        xaxis3=dict(title="time"),
        yaxis=dict(title="Price"),
        yaxis2=dict(title="volume"),
        yaxis3=dict(title="relatr"),
        xaxis_rangeslider_visible=False,
        margin=dict(l=60, r=40, t=50, b=40),
    )

    return fig_intraday


def render_png(fig: go.Figure) -> bytes:
    """Blocking kaleido render -- call via ``asyncio.to_thread``."""
    return pio.to_image(fig, format="png")
