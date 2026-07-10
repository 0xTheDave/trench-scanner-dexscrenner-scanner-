# jupiter_monitor.py

import asyncio
import aiohttp
import os
import time
from datetime import datetime, timezone

from discord_client import send_jupiter_alert

JUPITER_BASE = "https://lite-api.jup.ag"
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY", "")

DEDUP_TTL = 86_400                  # don't re-alert same token within 24h
MIN_JUPITER_VOLUME = 50_000         # minimum 24h routing volume

alerted_jupiter: dict[str, float] = {}

# Track previous top-20 set — alert only on NEW entrants
previous_top: set[str] = set()

EXCLUDED_SYMBOLS = ("USDC", "USDT", "SOL", "WSOL", "WETH", "BTC", "WBTC", "JUP")


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
    Fetch price data for a single token from Jupiter Price API v3.
    v3 response is keyed by address directly: {address: {usdPrice, ...}}
    """
    data = await fetch_json(
        session,
        f"{JUPITER_BASE}/price/v3?ids={address}"
    )
    if not data:
        return None

    # v3 keys by address directly; fall back to v2-style data wrapper
    entry = data.get(address) or (data.get("data") or {}).get(address)
    return entry


async def fetch_trending_tokens(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch top tokens by volume from Jupiter token search.
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
    Scan Jupiter top-20 volume tokens.
    Alert only on NEW entrants to the top 20 — a token suddenly
    appearing among the highest-volume tokens is the actual signal.
    First run only records the baseline without alerting.
    """
    global previous_top

    print(f"[jupiter] {datetime.now().strftime('%H:%M:%S')} — scanning Jupiter trending...")

    tokens = await fetch_trending_tokens(session)

    if not tokens:
        print("[jupiter] No tokens received")
        return

    current_top = {t.get("address", "") for t in tokens if t.get("address")}
    is_first_run = len(previous_top) == 0
    new_entrants = current_top - previous_top

    if is_first_run:
        print(f"[jupiter] First run — baseline set with {len(current_top)} tokens, no alerts")
        previous_top = current_top
        return

    print(f"[jupiter] Received {len(tokens)} tokens, new entrants: {len(new_entrants)}")

    alerts_sent = 0

    for token in tokens:
        addr = token.get("address", "")
        if not addr or addr not in new_entrants:
            continue

        if _is_deduped(addr):
            continue

        symbol = token.get("symbol", "").upper()
        if symbol in EXCLUDED_SYMBOLS:
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

        price_data = await fetch_token_price(session, addr)

        print(
            f"[jupiter] Alert: {symbol} entered top 20 | "
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

    previous_top = current_top
    _cleanup_dedup()
    print(f"[jupiter] Done. Alerts: {alerts_sent}")