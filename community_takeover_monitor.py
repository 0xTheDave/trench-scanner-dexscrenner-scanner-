# community_takeover_monitor.py

import asyncio
import aiohttp
import time
from datetime import datetime, timezone

from discord_client import send_takeover_alert

DEXSCREENER_BASE = "https://api.dexscreener.com"

DEDUP_TTL = 86_400                  # don't re-alert same token within 24h
TARGET_CHAIN = "solana"
MAX_CLAIM_AGE_HOURS = 1            # only alert takeovers claimed within last 1h

alerted_takeovers: dict[str, float] = {}


def _is_deduped(address: str) -> bool:
    last = alerted_takeovers.get(address)
    if last is None:
        return False
    return time.time() - last < DEDUP_TTL


def _mark_alerted(address: str):
    alerted_takeovers[address] = time.time()


def _cleanup_dedup():
    now = time.time()
    expired = [k for k, v in alerted_takeovers.items() if now - v > DEDUP_TTL]
    for k in expired:
        del alerted_takeovers[k]


def _claim_age_hours(claim_date: str) -> float:
    """Parse ISO claimDate and return age in hours. Returns 9999 on failure."""
    if not claim_date:
        return 9999.0
    try:
        # Handle both with and without timezone suffix
        cleaned = claim_date.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        return delta.total_seconds() / 3600
    except (ValueError, TypeError):
        return 9999.0


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    """Safe GET with retry on 429."""
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    wait = 2 ** attempt * 5
                    print(f"[takeover] Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                print(f"[takeover] HTTP {resp.status} for {url}")
                return None
        except asyncio.TimeoutError:
            print(f"[takeover] Timeout, attempt {attempt+1}/3")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[takeover] Error: {e}")
            return None
    return None


async def fetch_pair_data(
    session: aiohttp.ClientSession,
    address: str,
) -> dict | None:
    """Fetch pair market data to enrich takeover alert."""
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


async def scan_takeovers(session: aiohttp.ClientSession):
    """Fetch latest community takeovers and alert on fresh ones only."""
    print(f"[takeover] {datetime.now().strftime('%H:%M:%S')} — scanning community takeovers...")

    data = await fetch_json(
        session,
        f"{DEXSCREENER_BASE}/community-takeovers/latest/v1"
    )

    if not data:
        print("[takeover] No data received")
        return

    takeovers = [t for t in data if t.get("chainId") == TARGET_CHAIN]
    print(f"[takeover] Found {len(takeovers)} takeovers on {TARGET_CHAIN}")

    alerts_sent = 0
    skipped_old = 0

    for takeover in takeovers:
        addr = takeover.get("tokenAddress")
        if not addr or _is_deduped(addr):
            continue

        claim_date = takeover.get("claimDate", "")
        age_hours = _claim_age_hours(claim_date)

        # Skip stale takeovers — restart-proof freshness filter
        if age_hours > MAX_CLAIM_AGE_HOURS:
            _mark_alerted(addr)
            skipped_old += 1
            continue

        await asyncio.sleep(0.5)

        pair = await fetch_pair_data(session, addr)

        print(f"[takeover] Alert: {addr[:8]}... | claimed={claim_date[:10] if claim_date else 'unknown'}")

        await send_takeover_alert(session, {
            "takeover": takeover,
            "pair": pair,
        })

        _mark_alerted(addr)
        alerts_sent += 1

    _cleanup_dedup()
    print(f"[takeover] Done. Alerts: {alerts_sent}, skipped stale: {skipped_old}")