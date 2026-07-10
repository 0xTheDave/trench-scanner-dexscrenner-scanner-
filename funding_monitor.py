# funding_monitor.py

import asyncio
import aiohttp
import time
from datetime import datetime, timezone

from discord_client import send_funding_alert

HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"

FUNDING_HIGH_THRESHOLD = 0.0005     # +0.05%/h = overcrowded long
FUNDING_LOW_THRESHOLD = -0.0002     # -0.02%/h = overcrowded short
OI_CHANGE_THRESHOLD = 0.20          # 20% OI change since last snapshot
MIN_OI_USD = 1_000_000              # ignore markets under $1M OI
MIN_DAY_VOLUME = 500_000            # skip markets with dead volume
ALERT_DEDUP_TTL = 3600              # don't re-alert same coin within 1h

# In-memory stores
oi_history: dict[str, float] = {}
alerted_coins: dict[str, float] = {}


def _is_deduped(coin: str) -> bool:
    last = alerted_coins.get(coin)
    if last is None:
        return False
    return time.time() - last < ALERT_DEDUP_TTL


def _mark_alerted(coin: str):
    alerted_coins[coin] = time.time()


def _cleanup_dedup():
    now = time.time()
    expired = [k for k, v in alerted_coins.items() if now - v > ALERT_DEDUP_TTL]
    for k in expired:
        del alerted_coins[k]


async def fetch_asset_contexts(session: aiohttp.ClientSession) -> list[dict] | None:
    """
    Fetch all perp markets in one call.
    Returns list of dicts with keys: coin, funding, open_interest,
    mark_px, premium, day_volume
    """
    payload = {"type": "metaAndAssetCtxs"}
    try:
        async with session.post(
            HYPERLIQUID_API,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                print(f"[funding] HTTP {resp.status} from Hyperliquid")
                return None

            data = await resp.json()

            # Response structure: [meta, [assetCtx, ...]]
            if not isinstance(data, list) or len(data) < 2:
                print("[funding] Unexpected response structure")
                return None

            universe = data[0].get("universe", [])
            asset_ctxs = data[1]

            if len(universe) != len(asset_ctxs):
                print("[funding] Universe/context length mismatch")
                return None

            results = []
            for meta, ctx in zip(universe, asset_ctxs):
                results.append({
                    "coin": meta.get("name", "???"),
                    "funding": float(ctx.get("funding", 0) or 0),
                    "open_interest": float(ctx.get("openInterest", 0) or 0),
                    "mark_px": float(ctx.get("markPx", 0) or 0),
                    "premium": float(ctx.get("premium", 0) or 0),
                    "day_volume": float(ctx.get("dayNtlVlm", 0) or 0),
                })
            return results

    except asyncio.TimeoutError:
        print("[funding] Timeout fetching Hyperliquid data")
        return None
    except Exception as e:
        print(f"[funding] Error: {e}")
        return None


async def scan_funding(session: aiohttp.ClientSession):
    """Main funding scan — fetch all markets, detect anomalies, send alerts."""
    print(f"[funding] {datetime.now().strftime('%H:%M:%S')} — scanning funding rates...")

    markets = await fetch_asset_contexts(session)
    if not markets:
        return

    alerts_sent = 0

    for market in markets:
        coin = market["coin"]
        funding = market["funding"]
        oi_raw = market["open_interest"]
        mark_px = market["mark_px"]
        day_vol = market["day_volume"]

        # Convert OI from contracts to USD
        oi_usd = oi_raw * mark_px

        # Skip tiny markets
        if oi_usd < MIN_OI_USD:
            continue

        # Skip markets with high OI but dead trading activity
        if day_vol < MIN_DAY_VOLUME:
            continue

        signal = None
        oi_change_pct = None

        # Check funding extremes
        if funding >= FUNDING_HIGH_THRESHOLD:
            signal = "high"
        elif funding <= FUNDING_LOW_THRESHOLD:
            signal = "low"

        # Check OI spike vs previous snapshot
        prev_oi = oi_history.get(coin)
        if prev_oi and prev_oi > 0:
            oi_change_pct = (oi_usd - prev_oi) / prev_oi
            if abs(oi_change_pct) >= OI_CHANGE_THRESHOLD and signal is None:
                signal = "oi_spike"

        # Update OI snapshot regardless of signal
        oi_history[coin] = oi_usd

        if signal is None:
            continue

        if _is_deduped(coin):
            continue

        # Annualized rate: funding is per hour, 8760h/year
        annual_rate = funding * 8760 * 100

        print(
            f"[funding] Alert: {coin} | signal={signal} "
            f"| funding={funding:.4%}/h | OI=${oi_usd:,.0f}"
        )

        await send_funding_alert(session, {
            "coin": coin,
            "funding": funding,
            "annual_rate": annual_rate,
            "oi_usd": oi_usd,
            "oi_change_pct": oi_change_pct,
            "mark_px": mark_px,
            "day_volume": day_vol,
            "signal": signal,
            "premium": market["premium"],
        })

        _mark_alerted(coin)
        alerts_sent += 1

    _cleanup_dedup()
    print(f"[funding] Done. Alerts: {alerts_sent}, markets scanned: {len(markets)}")