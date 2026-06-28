# jupiter_monitor.py

import asyncio
import aiohttp
import os
import time
from datetime import datetime, timezone

from discord_client import send_jupiter_alert

JUPITER_BASE = "https://lite-api.jup.ag"
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY", "")

SCAN_INTERVAL_SECONDS = 300         # every 5 minutes
DEDUP_TTL = 3600                    # don't re-alert same token within 1h
TARGET_CHAIN = "solana"

# Minimum 24h volume through Jupiter to be worth alerting
MIN_JUPITER_VOLUME = 50_000

alerted_jupiter: dict[str, float] = {}


def _is_deduped(address: str) -> bool:
    last = alerted_jupiter.get(address)
    if last is None:
        return False
    return time.time() - last < DEDUP_TTL


def _mark_alerted(address: str):
    alerted_jupiter[address] = time.time()


def _cleanup_dedup():
    now = time.time()
    expired = [k for k, v in alerted_jupiter.items() if now - v > DEDUP_TTL]
    for k in expired:
        del alerted_jupiter[k]


def _get_headers() -> dict:
    if JUPITER_API_KEY:
        return {"x-api-key": JUPITER_API_KEY}
    return {}


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    """Safe GET with retry on 429."""
    for attempt in range(3):
        try:
            async with session.get(
                url,
                headers=_get_headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    wait = 2 ** attempt * 5
                    print(f"[jupiter] Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                print(f"[jupiter] HTTP {resp.status} for {url}")
                return None
        except asyncio.TimeoutError:
            print(f"[jupiter] Timeout, attempt {attempt+1}/3")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[jupiter] Error: {e}")
            return None
    return None


async def fetch_token_price(
    session: aiohttp.ClientSession,
    address: str,
) -> dict | None:
    """
    Fetch price data for a single token from Jupiter price API.
    Returns price object with vsToken price, confidence, etc.
    """
    data = await fetch_json(
        session,
        f"{JUPITER_BASE}/price/v3?ids={address}&showExtraInfo=true"
    )
    if not data:
        return None

    prices = data.get("data", {})
    return prices.get(address)


async def fetch_trending_tokens(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch trending/new tokens from Jupiter token search.
    Uses /tokens/v2/search sorted by volume to find active tokens.
    """
    data = await fetch_json(
        session,
        f"{JUPITER_BASE}/tokens/v2/search?query=&sort_by=volume&limit=20"
    )
    if not data:
        return []

    tokens = data if isinstance(data, list) else data.get("tokens", [])
    return tokens


async def scan_jupiter(session: aiohttp.ClientSession):
    """
    Scan Jupiter for trending tokens with high routing volume.
    Alert on tokens with significant Jupiter swap activity.
    """
    print(f"[jupiter] {datetime.now().strftime('%H:%M:%S')} — scanning Jupiter trending...")

    tokens = await fetch_trending_tokens(session)

    if not tokens:
        print("[jupiter] No tokens received")
        return

    print(f"[jupiter] Received {len(tokens)} tokens from Jupiter")

    alerts_sent = 0

    for token in tokens:
        addr = token.get("address", "")
        if not addr or _is_deduped(addr):
            continue

        # Skip non-Solana or stablecoins
        symbol = token.get("symbol", "").upper()
        if symbol in ("USDC", "USDT", "SOL", "WSOL", "WETH", "BTC", "WBTC"):
            continue

        daily_volume = token.get("daily_volume") or token.get("v24hUSD") or 0
        if isinstance(daily_volume, str):
            try:
                daily_volume = float(daily_volume)
            except ValueError:
                daily_volume = 0

        if daily_volume < MIN_JUPITER_VOLUME:
            continue

        await asyncio.sleep(0.5)

        # Fetch price data for this token
        price_data = await fetch_token_price(session, addr)

        print(
            f"[jupiter] Alert: {symbol} | "
            f"vol=${daily_volume:,.0f} | "
            f"addr={addr[:8]}..."
        )

        await send_jupiter_alert(session, {
            "token": token,
            "price_data": price_data,
            "daily_volume": daily_volume,
        })

        _mark_alerted(addr)
        alerts_sent += 1

    _cleanup_dedup()
    print(f"[jupiter] Done. Alerts: {alerts_sent}")