# scanner.py
from dotenv import load_dotenv
load_dotenv()

import asyncio
import aiohttp
import time
import os
from datetime import datetime, timezone

from filters import FILTERS
from state import SeenTokens, VolumeHistory
from discord_client import send_gem_alert, send_narrative_update, send_spike_alert
from funding_monitor import scan_funding
from liquidation_monitor import run_liquidation_monitor
from boost_monitor import scan_boosts
from community_takeover_monitor import scan_takeovers
from jupiter_monitor import scan_jupiter

DEXSCREENER_BASE = "https://api.dexscreener.com"

SCAN_INTERVAL_SECONDS = 120
NARRATIVE_INTERVAL_SECONDS = 7200
WATCHLIST_SCAN_INTERVAL_SECONDS = 60
FUNDING_SCAN_INTERVAL_SECONDS = 1800
BOOST_SCAN_INTERVAL_SECONDS = 300
TAKEOVER_SCAN_INTERVAL_SECONDS = 300
JUPITER_SCAN_INTERVAL_SECONDS = 300

REQUEST_DELAY = 1.0

seen = SeenTokens(ttl_seconds=FILTERS["dedup_ttl_seconds"])
vol_history = VolumeHistory(ttl_seconds=3600)

watchlist: dict[str, dict] = {}
seen_spikes = SeenTokens(ttl_seconds=600)


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    """Safe GET with retry on 429."""
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    wait = 2 ** attempt * 5
                    print(f"[api] Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                print(f"[api] HTTP {resp.status} for {url}")
                return None
        except asyncio.TimeoutError:
            print(f"[api] Timeout for {url}, attempt {attempt+1}/3")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[api] Error: {e}")
            return None
    return None


def passes_filters(pair: dict, age_hours: float) -> tuple[bool, str]:
    """
    Returns (True, "") if pair passes all filters.
    Returns (False, "reason") if rejected.
    """
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol = (pair.get("volume") or {}).get("h24") or 0
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total_txns = buys + sells
    ch_h1 = (pair.get("priceChange") or {}).get("h1") or 0
    mcap = pair.get("marketCap") or pair.get("fdv") or 0

    if liq < FILTERS["min_liquidity_usd"]:
        return False, f"liq_low ${liq:,.0f}"
    if liq > FILTERS["max_liquidity_usd"]:
        return False, f"liq_high ${liq:,.0f}"
    if vol < FILTERS["min_volume_h24"]:
        return False, f"vol_low ${vol:,.0f}"
    if total_txns < FILTERS["min_txns_h24"]:
        return False, f"txns_low {total_txns}"
    if age_hours > FILTERS["max_age_hours"]:
        return False, f"too_old {age_hours:.1f}h"
    if ch_h1 < FILTERS["min_price_change_h1"]:
        return False, f"no_momentum {ch_h1:.1f}%"

    if buys >= FILTERS["honeypot_min_buys_threshold"] and sells == 0:
        return False, f"honeypot {buys} buys / 0 sells"

    if total_txns > 0:
        buy_ratio = buys / total_txns
        if buy_ratio > FILTERS["max_buy_ratio"]:
            return False, f"sus_buys {buy_ratio:.0%}"
        if buy_ratio < FILTERS["min_buy_ratio"]:
            return False, f"dump {buy_ratio:.0%} buys"

    if mcap > 0:
        liq_ratio = liq / mcap
        if liq_ratio < FILTERS["min_liq_to_mcap_ratio"]:
            return False, f"rug_risk liq/mcap={liq_ratio:.2%}"

    return True, ""


def has_inline_spike(pair: dict) -> bool:
    """
    Approach B: check if vol.m5 is a large % of vol.h1.
    Signals that most of the hourly volume just happened.
    """
    vol_m5 = (pair.get("volume") or {}).get("m5") or 0
    vol_h1 = (pair.get("volume") or {}).get("h1") or 0

    if vol_m5 < FILTERS["spike_min_vol_m5"]:
        return False
    if vol_h1 <= 0:
        return False

    ratio = vol_m5 / vol_h1
    return ratio >= FILTERS["spike_m5_to_h1_ratio"]


async def process_profile(
    session: aiohttp.ClientSession,
    profile: dict,
    source: str,
    alerts_sent_ref: list,
):
    """Process a single token profile — shared logic for both sources."""
    addr = profile.get("tokenAddress")
    if not addr or seen.has(addr):
        return

    await asyncio.sleep(REQUEST_DELAY)

    chain = FILTERS["chain_id"]
    pairs_data = await fetch_json(
        session,
        f"{DEXSCREENER_BASE}/token-pairs/v1/{chain}/{addr}"
    )

    if not pairs_data:
        seen.add(addr)
        return

    valid_pairs = [p for p in pairs_data if p.get("liquidity")]
    if not valid_pairs:
        seen.add(addr)
        return

    pair = max(valid_pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0))

    created_at = pair.get("pairCreatedAt")
    age_hours = (time.time() - created_at / 1000) / 3600 if created_at else 999

    if has_inline_spike(pair) and not seen_spikes.has(addr):
        symbol = pair.get("baseToken", {}).get("symbol", "?")
        print(f"[spike-B] Inline spike detected for {symbol}")
        await send_spike_alert(session, pair, spike_type="inline")
        seen_spikes.add(addr)

    passed, reason = passes_filters(pair, age_hours)

    if not passed:
        symbol = pair.get("baseToken", {}).get("symbol", "???")
        print(f"[filter] {symbol} rejected — {reason}")
        seen.add(addr)
        return

    liq = (pair.get("liquidity") or {}).get("usd") or 0
    if liq >= FILTERS["spike_min_liquidity"]:
        watchlist[addr] = pair
        if len(watchlist) > FILTERS["watchlist_size"]:
            sorted_wl = sorted(
                watchlist.items(),
                key=lambda x: (x[1].get("liquidity") or {}).get("usd") or 0,
                reverse=True
            )
            watchlist.clear()
            watchlist.update(dict(sorted_wl[:FILTERS["watchlist_size"]]))

    result = await send_gem_alert(session, {"pair": pair, "age_hours": age_hours, "source": source})
    seen.add(addr)

    if result in (200, 204):
        symbol = pair.get("baseToken", {}).get("symbol", "???")
        print(f"[alert] ✅ ${symbol.lstrip('$')} [{source}] | age={age_hours:.1f}h | liq=${liq:,.0f}")
        alerts_sent_ref[0] += 1


async def scan_tokens(session: aiohttp.ClientSession):
    """Fetch new token profiles from both endpoints and process each."""
    print(f"[scan] {datetime.now().strftime('%H:%M:%S')} — scanning tokens...")

    chain = FILTERS["chain_id"]

    latest, recent = await asyncio.gather(
        fetch_json(session, f"{DEXSCREENER_BASE}/token-profiles/latest/v1"),
        fetch_json(session, f"{DEXSCREENER_BASE}/token-profiles/recent-updates/v1"),
    )

    all_profiles: list[tuple[dict, str]] = []
    seen_in_batch: set[str] = set()

    for profile, source in [
        *[(p, "latest") for p in (latest or [])],
        *[(p, "update") for p in (recent or [])],
    ]:
        if profile.get("chainId") != chain:
            continue
        addr = profile.get("tokenAddress")
        if addr and addr not in seen_in_batch:
            seen_in_batch.add(addr)
            all_profiles.append((profile, source))

    print(f"[scan] Profiles to check: {len(all_profiles)} (latest + recent-updates)")

    alerts_sent_ref = [0]

    for profile, source in all_profiles:
        await process_profile(session, profile, source, alerts_sent_ref)

    cleaned = seen.cleanup()
    print(f"[scan] Done. Alerts: {alerts_sent_ref[0]}, cleaned: {cleaned} entries")


async def scan_watchlist(session: aiohttp.ClientSession):
    """
    Approach A: re-fetch watchlist tokens and compare vol.m5
    to previous snapshot. Alert if spike_multiplier exceeded.
    """
    if not watchlist:
        return

    print(f"[watchlist] Scanning {len(watchlist)} tokens...")
    spikes_found = 0
    chain = FILTERS["chain_id"]

    for addr, _ in list(watchlist.items()):
        await asyncio.sleep(REQUEST_DELAY)

        pairs_data = await fetch_json(
            session,
            f"{DEXSCREENER_BASE}/token-pairs/v1/{chain}/{addr}"
        )

        if not pairs_data:
            continue

        valid_pairs = [p for p in pairs_data if p.get("liquidity")]
        if not valid_pairs:
            continue

        pair = max(valid_pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0))

        vol_m5 = (pair.get("volume") or {}).get("m5") or 0
        vol_h1 = (pair.get("volume") or {}).get("h1") or 0
        liq = (pair.get("liquidity") or {}).get("usd") or 0

        if liq < FILTERS["spike_min_liquidity"]:
            watchlist.pop(addr, None)
            continue

        if vol_m5 < FILTERS["spike_min_vol_m5"]:
            vol_history.set(addr, vol_m5, vol_h1)
            continue

        prev = vol_history.get(addr)

        if prev is not None:
            prev_vol_m5 = prev["vol_m5"]
            if prev_vol_m5 > 0:
                multiplier = vol_m5 / prev_vol_m5
                if multiplier >= FILTERS["spike_multiplier"] and not seen_spikes.has(addr):
                    symbol = pair.get("baseToken", {}).get("symbol", "???")
                    print(f"[spike-A] {symbol} — {multiplier:.1f}x vol spike detected")
                    await send_spike_alert(
                        session,
                        pair,
                        spike_type="watchlist",
                        prev_vol_m5=prev_vol_m5,
                    )
                    seen_spikes.add(addr)
                    spikes_found += 1

        vol_history.set(addr, vol_m5, vol_h1)

    vol_history.cleanup()
    print(f"[watchlist] Done. Spikes found: {spikes_found}, tracking: {len(watchlist)} tokens")


async def scan_narratives(session: aiohttp.ClientSession):
    """Fetch and post trending narrative metas."""
    print("[narratives] Fetching trending metas...")

    data = await fetch_json(session, f"{DEXSCREENER_BASE}/metas/trending/v1")
    if not data:
        print("[narratives] No data received")
        return

    sorted_metas = sorted(
        data,
        key=lambda x: (x.get("marketCapChange") or {}).get("h1", 0),
        reverse=True
    )

    await send_narrative_update(session, sorted_metas)
    print(f"[narratives] Sent {min(5, len(sorted_metas))} narratives")


async def main_loop(session: aiohttp.ClientSession):
    """Main polling loop — all scanners except liquidations."""
    last_narrative = 0
    last_watchlist = 0
    last_funding = 0
    last_boost = 0
    last_takeover = 0
    last_jupiter = 0

    while True:
        try:
            await scan_tokens(session)

            if time.time() - last_watchlist >= WATCHLIST_SCAN_INTERVAL_SECONDS:
                await scan_watchlist(session)
                last_watchlist = time.time()

            if time.time() - last_funding >= FUNDING_SCAN_INTERVAL_SECONDS:
                await scan_funding(session)
                last_funding = time.time()

            if time.time() - last_boost >= BOOST_SCAN_INTERVAL_SECONDS:
                await scan_boosts(session)
                last_boost = time.time()

            if time.time() - last_takeover >= TAKEOVER_SCAN_INTERVAL_SECONDS:
                await scan_takeovers(session)
                last_takeover = time.time()

            if time.time() - last_jupiter >= JUPITER_SCAN_INTERVAL_SECONDS:
                await scan_jupiter(session)
                last_jupiter = time.time()

            if time.time() - last_narrative >= NARRATIVE_INTERVAL_SECONDS:
                await scan_narratives(session)
                last_narrative = time.time()

        except Exception as e:
            print(f"[main] Unexpected error: {e}")

        print(f"[main] Waiting {SCAN_INTERVAL_SECONDS}s...\n")
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


async def main():
    print("=" * 50)
    print("Trench Scanner — starting")
    print(f"Chain: {FILTERS['chain_id']}")
    print(f"Scan interval: {SCAN_INTERVAL_SECONDS}s")
    print(f"Watchlist interval: {WATCHLIST_SCAN_INTERVAL_SECONDS}s")
    print(f"Funding interval: {FUNDING_SCAN_INTERVAL_SECONDS}s")
    print(f"Boost interval: {BOOST_SCAN_INTERVAL_SECONDS}s")
    print(f"Takeover interval: {TAKEOVER_SCAN_INTERVAL_SECONDS}s")
    print(f"Jupiter interval: {JUPITER_SCAN_INTERVAL_SECONDS}s")
    print(f"Liquidations: WebSocket (real-time)")
    print("=" * 50)

    required = [
        "DISCORD_WEBHOOK_GEMS",
        "DISCORD_WEBHOOK_NARRATIVES",
        "DISCORD_WEBHOOK_SPIKES",
        "DISCORD_WEBHOOK_FUNDING",
        "DISCORD_WEBHOOK_LIQUIDATIONS",
        "DISCORD_WEBHOOK_BOOSTED",
        "DISCORD_WEBHOOK_TAKEOVERS",
        "DISCORD_WEBHOOK_JUPITER",
    ]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {missing}")

    async with aiohttp.ClientSession(
        headers={"User-Agent": "TrenchScanner/1.0"}
    ) as session:
        await asyncio.gather(
            main_loop(session),
            run_liquidation_monitor(session),
        )


if __name__ == "__main__":
    asyncio.run(main())