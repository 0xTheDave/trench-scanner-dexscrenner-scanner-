# performance_tracker.py
# Measures alert performance: fetches token price 1h/6h/24h after each
# alert and stores it in SQLite. Posts a per-channel performance report
# on a fixed cadence (Discord webhook optional — logs only if unset).

import asyncio
import aiohttp
import os
import statistics
import time
from datetime import datetime, timezone

import db

DEXSCREENER_BASE = "https://api.dexscreener.com"
WEBHOOK_PERFORMANCE = os.environ.get("DISCORD_WEBHOOK_PERFORMANCE", "")

# (column, seconds after alert)
MEASUREMENT_SLOTS = (
    ("price_1h", 3_600),
    ("price_6h", 21_600),
    ("price_24h", 86_400),
)

BATCH_LIMIT = 25            # max measurements per column per run
REQUEST_DELAY = 1.0
REPORT_DAYS = 7
RUG_THRESHOLD = 0.10        # price_24h <= 10% of entry counts as rugged

# Report cadence is enforced HERE via a DB timestamp, not by the scanner's
# sleep loop. The old design slept 24h before the first report and reset that
# clock on every restart, so during active development the report never fired.
# 6h while tuning thresholds; bump back to 24h once the strategy settles.
REPORT_INTERVAL_SECONDS = 6 * 3600
_LAST_REPORT_KEY = "perf_last_report_ts"


async def _fetch_current_price(session: aiohttp.ClientSession, address: str) -> float:
    """
    Current price from the most liquid DexScreener pair. The chain is inferred
    from the address format: Robinhood Chain tokens are 0x-prefixed EVM
    addresses, Solana mints are base58 (never 0x). Without this split, robinhood
    alerts would be queried against the solana endpoint and always read as dead.
    Returns 0.0 when no tradable pair exists — the token is dead, which
    correctly counts as a -100% outcome, not missing data.
    """
    chain = "robinhood" if address.startswith("0x") else "solana"
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/{chain}/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return 0.0
            data = await resp.json()
    except Exception:
        return 0.0

    if not data:
        return 0.0

    valid = [p for p in data if (p.get("liquidity") or {}).get("usd")]
    if not valid:
        return 0.0

    best = max(valid, key=lambda x: x.get("liquidity", {}).get("usd", 0))
    try:
        return float(best.get("priceUsd") or 0)
    except (ValueError, TypeError):
        return 0.0


async def track_performance(session: aiohttp.ClientSession):
    """Fill in due price measurements for recorded alerts."""
    measured = 0

    for column, delay_seconds in MEASUREMENT_SLOTS:
        rows = db.due_measurements(column, delay_seconds, BATCH_LIMIT)
        for row_id, address in rows:
            await asyncio.sleep(REQUEST_DELAY)
            price = await _fetch_current_price(session, address)
            db.set_measurement(row_id, column, price)
            measured += 1

    if measured:
        print(f"[perf] Recorded {measured} price measurements")


def _pct_return(entry: float, current: float | None) -> float | None:
    if current is None or not entry or entry <= 0:
        return None
    return (current / entry - 1) * 100


def _channel_summary(rows: list[dict]) -> str:
    returns_1h = [r for r in (_pct_return(x["price_at_alert"], x["price_1h"]) for x in rows) if r is not None]
    returns_24h = [r for r in (_pct_return(x["price_at_alert"], x["price_24h"]) for x in rows) if r is not None]

    lines = [f"Alerts: **{len(rows)}**"]

    if returns_1h:
        wins = sum(1 for r in returns_1h if r > 0)
        lines.append(
            f"1h: win rate **{wins / len(returns_1h):.0%}** | "
            f"median **{statistics.median(returns_1h):+.1f}%** "
            f"({len(returns_1h)} measured)"
        )
    if returns_24h:
        wins = sum(1 for r in returns_24h if r > 0)
        rugs = sum(
            1 for x in rows
            if x["price_24h"] is not None and x["price_at_alert"] > 0
            and x["price_24h"] <= x["price_at_alert"] * RUG_THRESHOLD
        )
        lines.append(
            f"24h: win rate **{wins / len(returns_24h):.0%}** | "
            f"median **{statistics.median(returns_24h):+.1f}%** | "
            f"rugged: {rugs}"
        )
        best = max(
            (x for x in rows if _pct_return(x["price_at_alert"], x["price_24h"]) is not None),
            key=lambda x: _pct_return(x["price_at_alert"], x["price_24h"]),
            default=None,
        )
        if best:
            best_ret = _pct_return(best["price_at_alert"], best["price_24h"])
            lines.append(f"Best: **${best['symbol']}** {best_ret:+.0f}%")

    if len(lines) == 1:
        lines.append("No measurements completed yet")

    return "\n".join(lines)


async def send_performance_report(session: aiohttp.ClientSession):
    """
    Aggregate last N days of alerts per channel and post a report.
    Sends unconditionally — the cadence gate lives in
    maybe_send_performance_report. Kept public so it can be triggered manually
    (e.g. from a one-off script) to force a report on demand.
    """
    rows = db.performance_rows(REPORT_DAYS)

    if not rows:
        print("[perf] No alert data yet — skipping report")
        return

    by_channel: dict[str, list[dict]] = {}
    for row in rows:
        by_channel.setdefault(row["channel"], []).append(row)

    print(f"[perf] Report: {len(rows)} alerts across {len(by_channel)} channels (last {REPORT_DAYS}d)")

    fields = []
    for channel in sorted(by_channel.keys()):
        channel_rows = by_channel[channel]
        summary = _channel_summary(channel_rows)
        fields.append({
            "name": f"📊 #{channel}",
            "value": summary,
            "inline": False,
        })
        print(f"[perf]   {channel}: {summary.replace(chr(10), ' | ').replace('**', '')}")

    if not WEBHOOK_PERFORMANCE:
        print("[perf] DISCORD_WEBHOOK_PERFORMANCE not set — report logged only")
        return

    embed = {
        "embeds": [{
            "title": f"📈 PERFORMANCE REPORT — last {REPORT_DAYS} days",
            "description": "Median return measured from alert price. Dead tokens count as -100%.",
            "color": 0x3498DB,
            "fields": fields[:25],
            "footer": {"text": "Trench Scanner • Performance Tracker"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_PERFORMANCE, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[perf] Discord send error: {resp.status} {text}")


async def maybe_send_performance_report(session: aiohttp.ClientSession):
    """
    Restart-proof report gate. Sends a report only when REPORT_INTERVAL_SECONDS
    have elapsed since the last one (timestamp persisted in DB, so restarts
    never reset the clock). Cheap to poll frequently — the scanner calls this on
    a short interval and this decides whether a report is actually due. On the
    very first call (no stored timestamp) it sends immediately, since there is
    already accumulated data to report.
    """
    last_ts = db.kv_get_json(_LAST_REPORT_KEY, 0) or 0
    now = time.time()

    if last_ts and (now - last_ts) < REPORT_INTERVAL_SECONDS:
        return  # not due yet

    await send_performance_report(session)
    db.kv_set_json(_LAST_REPORT_KEY, now)