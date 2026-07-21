# chart_renderer.py
# Renders OHLCV candlestick + volume charts as PNG bytes, for embedding in
# Discord alerts (attachment). Data source: GeckoTerminal OHLCV endpoint.
#
# Rendering is CPU-bound and blocking, so the async wrapper offloads it to a
# thread (asyncio.to_thread) — it must never block the scanner's event loop.
# matplotlib runs on the headless 'Agg' backend (no GUI, safe on a server).

import asyncio
import io

import matplotlib
matplotlib.use("Agg")  # headless backend — must be set before pyplot/mplfinance
import matplotlib.pyplot as plt  # noqa: E402
import mplfinance as mpf  # noqa: E402
import pandas as pd  # noqa: E402

# Minimum candles worth drawing — a token minutes old may return 0-2 candles,
# in which case a chart is meaningless and we skip it (embed shows no image).
MIN_CANDLES = 5

# Dark style to blend into Discord's dark embeds. Easy to swap for a light
# look (e.g. Rick's white background) by changing base_mpf_style below.
_STYLE = mpf.make_mpf_style(
    base_mpf_style="nightclouds",
    rc={
        "axes.edgecolor": "#2b2b2b",
        "figure.facecolor": "#1e1f22",
        "axes.facecolor": "#1e1f22",
        "savefig.facecolor": "#1e1f22",
    },
    marketcolors=mpf.make_marketcolors(
        up="#3fd48c", down="#ff5555",
        edge={"up": "#3fd48c", "down": "#ff5555"},
        wick={"up": "#3fd48c", "down": "#ff5555"},
        volume={"up": "#2f6f52", "down": "#7a3535"},
    ),
)


def _ohlcv_to_dataframe(ohlcv_list: list) -> pd.DataFrame | None:
    """
    Convert GeckoTerminal's ohlcv_list ([ts, o, h, l, c, v], newest-first) into
    a time-indexed DataFrame that mplfinance expects. Returns None if there's
    not enough usable data.
    """
    if not ohlcv_list or len(ohlcv_list) < MIN_CANDLES:
        return None

    rows = []
    for candle in ohlcv_list:
        if not isinstance(candle, (list, tuple)) or len(candle) < 6:
            continue
        ts, o, h, l, c, v = candle[:6]
        try:
            rows.append({
                "Date": pd.to_datetime(int(ts), unit="s"),
                "Open": float(o),
                "High": float(h),
                "Low": float(l),
                "Close": float(c),
                "Volume": float(v),
            })
        except (ValueError, TypeError):
            continue

    if len(rows) < MIN_CANDLES:
        return None

    df = pd.DataFrame(rows).set_index("Date").sort_index()  # oldest -> newest
    return df


def render_ohlcv_chart(
    ohlcv_list: list,
    title: str,
    timeframe_label: str = "5m",
) -> bytes | None:
    """
    Render a candlestick + volume chart to PNG bytes. Returns None when there's
    not enough data to draw (caller then just sends the alert without an image).
    Blocking/CPU-bound — call via render_chart_async from async code.
    """
    df = _ohlcv_to_dataframe(ohlcv_list)
    if df is None:
        return None

    buf = io.BytesIO()
    try:
        mpf.plot(
            df,
            type="candle",
            volume=True,
            style=_STYLE,
            title=f"\n{title}  ·  {timeframe_label}",
            figratio=(16, 9),
            figscale=1.1,
            tight_layout=True,
            savefig=dict(fname=buf, format="png", dpi=110, bbox_inches="tight"),
        )
    except Exception as e:
        print(f"[chart] Render failed for {title}: {e}")
        plt.close("all")
        return None
    finally:
        plt.close("all")  # always free the figure — leaks add up over a long run

    buf.seek(0)
    return buf.getvalue()


async def render_chart_async(
    ohlcv_list: list,
    title: str,
    timeframe_label: str = "5m",
) -> bytes | None:
    """Async wrapper — offloads the blocking render to a worker thread."""
    return await asyncio.to_thread(
        render_ohlcv_chart, ohlcv_list, title, timeframe_label
    )


# ---------------------------------------------------------------------------
# Diagnostic: run `python chart_renderer.py` to fetch real OHLCV for a known
# pool and save a test chart to test_chart.png — open it and judge the look
# before we wire charts into the alert senders.
# Pool: $IN / VIRTUAL on Robinhood Chain (from the step-1 diagnostic).
# ---------------------------------------------------------------------------
async def _diagnostic():
    import aiohttp

    network = "robinhood"
    pool = "0x69c32a8d365a1bf6f6fbbdb21958fdab2ba98b98"  # $IN / VIRTUAL
    url = (
        f"https://api.geckoterminal.com/api/v2/networks/{network}"
        f"/pools/{pool}/ohlcv/minute?aggregate=5&limit=100&currency=usd"
    )
    headers = {"Accept": "application/json;version=20230302"}

    print("Fetching OHLCV for $IN / VIRTUAL (robinhood)...")
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                print(f"HTTP {resp.status} — cannot fetch OHLCV")
                return
            data = await resp.json()

    ohlcv_list = (
        (data.get("data") or {}).get("attributes") or {}
    ).get("ohlcv_list") or []
    print(f"Got {len(ohlcv_list)} candles")

    png = render_ohlcv_chart(ohlcv_list, "IN / VIRTUAL", "5m")
    if png is None:
        print("Not enough data to render (or render failed).")
        return

    with open("test_chart.png", "wb") as f:
        f.write(png)
    print(f"Saved test_chart.png ({len(png):,} bytes) — open it and check the look.")


if __name__ == "__main__":
    asyncio.run(_diagnostic())