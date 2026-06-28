# boost_monitor.py

import asyncio
import aiohttp
import time
from datetime import datetime, timezone

from discord_client import send_boost_alert

DEXSCREENER_BASE = "https://api.dexscreener.com"

SCAN_INTERVAL_SECONDS = 300         # every 5 minutes
DEDUP_TTL = 3600                    # don't re-alert same token within 1h
MIN_BOOST_AMOUNT = 10               # minimum boost amount to alert
TARGET_CHAIN = "solana"             # filter by chain

# In-memory dedup
alerted_boosts: dict[str, float] = {}


def _is_deduped(address: str) -> bool:
    last = alerted_boosts.get(address)
    if last is None:
        return False
    return time.time() - last < DEDUP_TTL


def _mark_alerted(address: str):
    alerted_boosts[address] = time.time()


def _cleanup_dedup():
    now = time.time()
    expired = [k for k, v in alerted_boosts.items() if now - v > DEDUP_TTL]
    for k in expired:
        del alerted_boosts[k]


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    """Safe GET with retry on 429."""
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    wait = 2 ** attempt * 5
                    print(f"[boost] Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                print(f"[boost] HTTP {resp.status} for {url}")
                return None
        except asyncio.TimeoutError:
            print(f"[boost] Timeout for {url}, attempt {attempt+1}/3")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[boost] Error: {e}")
            return None
    return None


async def fetch_pair_data(
    session: aiohttp.ClientSession,
    address: str,
) -> dict | None:
    """
    Fetch pair data for a token to enrich boost alert
    with price, liquidity, volume, mcap.
    """
    data = await fetch_json(
        session,
        f"{DEXSCREENER_BASE}/token-pairs/v1/{TARGET_CHAIN}/{address}"
    )
    if not data:
        return None
    valid = [p for p in data if p.get("liquidity")]
    if not valid:
        return None
    return max(valid, key=lambda x: x.get("liquidity", {}).get("usd", 0))


async def scan_boosts(session: aiohttp.ClientSession):
    """
    Fetch latest boosted tokens and top boosted tokens.
    Alert on new boosts that pass minimum threshold.
    """
    print(f"[boost] {datetime.now().strftime('%H:%M:%S')} — scanning boosts...")

    # Fetch both endpoints in parallel
    latest, top = await asyncio.gather(
        fetch_json(session, f"{DEXSCREENER_BASE}/token-boosts/latest/v1"),
        fetch_json(session, f"{DEXSCREENER_BASE}/token-boosts/top/v1"),
    )

    # Merge and deduplicate by tokenAddress
    all_boosts: list[dict] = []
    seen_in_batch: set[str] = set()

    for boost, source in [
        *[(b, "latest") for b in (latest or [])],
        *[(b, "top") for b in (top or [])],
    ]:
        if boost.get("chainId") != TARGET_CHAIN:
            continue
        addr = boost.get("tokenAddress")
        if not addr or addr in seen_in_batch:
            continue
        seen_in_batch.add(addr)
        all_boosts.append({**boost, "source": source})

    print(f"[boost] Found {len(all_boosts)} boosted tokens on {TARGET_CHAIN}")

    alerts_sent = 0

    for boost in all_boosts:
        addr = boost.get("tokenAddress")
        amount = boost.get("amount", 0) or 0
        total_amount = boost.get("totalAmount", 0) or 0

        # Skip low-value boosts
        if amount < MIN_BOOST_AMOUNT:
            continue

        if _is_deduped(addr):
            continue

        # Enrich with market data
        await asyncio.sleep(0.5)
        pair = await fetch_pair_data(session, addr)

        print(
            f"[boost] Alert: {addr[:8]}... | "
            f"amount={amount} | total={total_amount} | "
            f"source={boost['source']}"
        )

        await send_boost_alert(session, {
            "boost": boost,
            "pair": pair,
        })

        _mark_alerted(addr)
        alerts_sent += 1

    _cleanup_dedup()
    print(f"[boost] Done. Alerts: {alerts_sent}")