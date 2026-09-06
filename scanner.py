# scanner.py
from dotenv import load_dotenv
load_dotenv()

import asyncio
import aiohttp
import time
import os
from datetime import datetime, timezone

import db
from filters import FILTERS
from state import SeenTokens, VolumeHistory
from discord_client import (
    send_gem_alert,
    send_narrative_update,
    send_spike_alert,
    send_alpha_alert,
)
from funding_monitor import scan_funding
from liquidation_monitor import run_liquidation_monitor
from whale_radar_monitor import run_whale_radar_monitor
from boost_monitor import scan_boosts
from community_takeover_monitor import scan_takeovers
from jupiter_monitor import scan_jupiter
from robinhood_monitor import scan_robinhood
from pumpportal_client import run_pumpportal_client
from multichain_monitor import scan_multichain
from nft_monitor import (  # NEW NFT
    scan_nft_new_collections,
    scan_nft_volume_spikes,
    scan_nft_trending,
)
from performance_tracker import track_performance, maybe_send_performance_report
from scoring import (
    compute_base_score,
    compute_safety_score,
    ALPHA_THRESHOLD,
    GEM_THRESHOLD,
    RESCORE_TRIGGER,
)
from rugcheck_client import fetch_rugcheck_report
from haiku_client import generate_tldr

DEXSCREENER_BASE = "https://api.dexscreener.com"

SCAN_INTERVAL_SECONDS = 120
NARRATIVE_INTERVAL_SECONDS = 7200
WATCHLIST_SCAN_INTERVAL_SECONDS = 60
FUNDING_SCAN_INTERVAL_SECONDS = 1800
BOOST_SCAN_INTERVAL_SECONDS = 300
TAKEOVER_SCAN_INTERVAL_SECONDS = 300
JUPITER_SCAN_INTERVAL_SECONDS = 300
ROBINHOOD_SCAN_INTERVAL_SECONDS = 120
MULTICHAIN_SCAN_INTERVAL_SECONDS = 60
# NEW NFT: shared OpenSea budget is 600 req/h with a 7s global lock in
# opensea_client. Worst case below ≈ 150 req/h — comfortable headroom.
NFT_NEW_SCAN_INTERVAL_SECONDS = 120
NFT_SPIKES_SCAN_INTERVAL_SECONDS = 180
NFT_TRENDING_SCAN_INTERVAL_SECONDS = 600
PERF_TRACK_INTERVAL_SECONDS = 600
# Poll often; the actual report cadence is gated inside
# maybe_send_performance_report via a DB timestamp (restart-proof), so this is
# just how often we CHECK whether a report is due — not how often one is sent.
PERF_REPORT_POLL_SECONDS = 1800

REQUEST_DELAY = 1.0

# Persistent dedup — restarts no longer cause duplicate alert waves
seen = SeenTokens(ttl_seconds=FILTERS["dedup_ttl_seconds"], scope="main")
vol_history = VolumeHistory(ttl_seconds=3600)

watchlist: dict[str, dict] = {}
seen_spikes = SeenTokens(ttl_seconds=600)

# Tokens already promoted to alpha — persistent, 48h TTL (tokens older
# than max_age_hours are never re-scored anyway)
alpha_promoted = SeenTokens(ttl_seconds=172_800, scope="alpha_promoted")


def _price_of(pair: dict) -> float:
    try:
        return float(pair.get("priceUsd") or 0)
    except (ValueError, TypeError):
        return 0.0


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
    ch_m5 = (pair.get("priceChange") or {}).get("m5") or 0
    mcap = pair.get("marketCap") or pair.get("fdv") or 0

    # Base filters
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

    # Momentum: fresh pairs (<1h) use 5m change, older pairs use 1h change
    if age_hours < 1.0:
        if ch_m5 < FILTERS["min_price_change_m5_fresh"]:
            return False, f"no_momentum_fresh {ch_m5:.1f}% (5m)"
    else:
        if ch_h1 < FILTERS["min_price_change_h1"]:
            return False, f"no_momentum {ch_h1:.1f}%"

    # MCap bounds
    if mcap > 0:
        if mcap < FILTERS["min_mcap"]:
            return False, f"mcap_low ${mcap:,.0f}"
        if mcap > FILTERS["max_mcap"]:
            return False, f"mcap_high ${mcap:,.0f}"

    # Anti-wash-trading: volume far exceeding liquidity = fake activity
    if liq > 0 and (vol / liq) > FILTERS["max_vol_to_liq_ratio"]:
        return False, f"wash_trading vol/liq={vol/liq:.0f}x"

    # Honeypot: many buys but zero sells = can't sell
    if buys >= FILTERS["honeypot_min_buys_threshold"] and sells == 0:
        return False, f"honeypot {buys} buys / 0 sells"

    # Anti-rug: buy/sell ratio
    if total_txns > 0:
        buy_ratio = buys / total_txns
        if buy_ratio > FILTERS["max_buy_ratio"]:
            return False, f"sus_buys {buy_ratio:.0%}"
        if buy_ratio < FILTERS["min_buy_ratio"]:
            return False, f"dump {buy_ratio:.0%} buys"

    # Anti-rug: liquidity vs mcap
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


def _rugcheck_debug_line(rugcheck: dict | None) -> str:
    """One-line summary of parsed RugCheck fields for log verification."""
    if rugcheck is None:
        return "rugcheck=None"
    return (
        f"mint_active={rugcheck.get('mint_authority_active')} "
        f"freeze_active={rugcheck.get('freeze_authority_active')} "
        f"lp_locked={rugcheck.get('lp_locked_pct')} "
        f"top10={rugcheck.get('top10_holders_pct')} "
        f"risks={len(rugcheck.get('risks') or [])}"
    )


def _build_rugcheck_summary(rugcheck: dict | None) -> str:
    """Human-readable safety summary for TL;DR context."""
    if not rugcheck:
        return "unknown"
    parts = []
    if rugcheck.get("mint_authority_active") is False:
        parts.append("mint revoked")
    if rugcheck.get("freeze_authority_active") is False:
        parts.append("not freezable")
    if rugcheck.get("lp_locked_pct") is not None:
        parts.append(f"LP locked {rugcheck['lp_locked_pct']:.0f}%")
    if rugcheck.get("top10_holders_pct") is not None:
        parts.append(f"top10 hold {rugcheck['top10_holders_pct']:.0f}%")
    return ", ".join(parts) if parts else "unknown"


async def send_alpha_with_tldr(
    session: aiohttp.ClientSession,
    pair: dict,
    score: int,
    breakdown: list[str],
    rugcheck: dict | None,
    age_hours: float,
    description: str = "",
):
    """Generate TL;DR and send alpha alert. Shared by discovery and promotion paths."""
    symbol = pair.get("baseToken", {}).get("symbol", "???")
    info = pair.get("info") or {}
    socials_list = [s.get("platform", "") for s in (info.get("socials") or [])]

    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol_h24 = (pair.get("volume") or {}).get("h24") or 0
    mcap = pair.get("marketCap") or pair.get("fdv") or 0
    market_summary = (
        f"MCap ${mcap:,.0f}, liquidity ${liq:,.0f}, "
        f"vol 24h ${vol_h24:,.0f}, age {age_hours:.1f}h"
    )

    tldr = await generate_tldr(session, {
        "symbol": symbol.lstrip("$"),
        "name": pair.get("baseToken", {}).get("name", symbol),
        "description": description,
        "socials": ", ".join(socials_list) if socials_list else None,
        "score": score,
        "rugcheck_summary": _build_rugcheck_summary(rugcheck),
        "market_summary": market_summary,
    })

    return await send_alpha_alert(session, {
        "pair": pair,
        "score": score,
        "breakdown": breakdown,
        "rugcheck": rugcheck,
        "tldr": tldr,
        "age_hours": age_hours,
    })


async def process_profile(
    session: aiohttp.ClientSession,
    profile: dict,
    source: str,
    alerts_sent_ref: list,
):
    """Process a single token profile with scoring and safety pipeline."""
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

    # Inline spike check
    if has_inline_spike(pair) and not seen_spikes.has(addr):
        symbol = pair.get("baseToken", {}).get("symbol", "?")
        print(f"[spike-B] Inline spike detected for {symbol}")
        result = await send_spike_alert(session, pair, spike_type="inline")
        if result in (200, 204):
            db.record_alert(
                addr, str(symbol).lstrip("$"), "spikes", _price_of(pair),
                liquidity=(pair.get("liquidity") or {}).get("usd") or 0,
            )
        seen_spikes.add(addr)

    # Stage 1: hard filters
    passed, reason = passes_filters(pair, age_hours)
    if not passed:
        symbol = pair.get("baseToken", {}).get("symbol", "???")
        print(f"[filter] {symbol} rejected — {reason}")
        seen.add(addr)
        return

    symbol = pair.get("baseToken", {}).get("symbol", "???")

    # Stage 2: base scoring from market data
    score, breakdown = compute_base_score(pair, age_hours)

    if score < GEM_THRESHOLD:
        print(f"[score] {symbol} rejected — score {score} < {GEM_THRESHOLD}")
        seen.add(addr)
        return

    # Stage 3: RugCheck on-chain safety
    rugcheck = await fetch_rugcheck_report(session, addr)
    print(f"[rugcheck] {symbol}: {_rugcheck_debug_line(rugcheck)}")
    safety_delta, safety_breakdown, hard_fail = compute_safety_score(rugcheck)

    if hard_fail:
        print(f"[score] {symbol} rejected — RugCheck hard fail")
        seen.add(addr)
        return

    score += safety_delta
    breakdown += safety_breakdown
    score = max(0, min(100, score))

    # Add to spike watchlist
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

    # Stage 4: route by score
    if score >= ALPHA_THRESHOLD:
        description = profile.get("description") or ""
        result = await send_alpha_with_tldr(
            session, pair, score, breakdown, rugcheck, age_hours, description
        )
        alpha_promoted.add(addr)
        channel = "alpha"
    else:
        result = await send_gem_alert(
            session,
            {"pair": pair, "age_hours": age_hours, "source": source}
        )
        channel = "gems"

    seen.add(addr)

    if result in (200, 204):
        db.record_alert(
            addr, str(symbol).lstrip("$"), channel, _price_of(pair),
            score=score, liquidity=liq,
        )
        print(
            f"[alert] ✅ ${symbol.lstrip('$')} [{channel}] | "
            f"score={score} | age={age_hours:.1f}h | liq=${liq:,.0f}"
        )
        alerts_sent_ref[0] += 1


async def scan_tokens(session: aiohttp.ClientSession):
    """Fetch new token profiles from both endpoints and process each."""
    started = time.monotonic()
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
    elapsed = time.monotonic() - started
    print(f"[scan] Done in {elapsed:.0f}s. Alerts: {alerts_sent_ref[0]}, cleaned: {cleaned} entries")


async def scan_watchlist(session: aiohttp.ClientSession):
    """
    Watchlist pass with two jobs:
    1. Volume spike detection (Approach A)
    2. Re-scoring: promote matured tokens to #alpha-picks
    """
    if not watchlist:
        return

    print(f"[watchlist] Scanning {len(watchlist)} tokens...")
    spikes_found = 0
    promotions = 0
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

        # Drop rugged tokens
        if liq < FILTERS["spike_min_liquidity"]:
            watchlist.pop(addr, None)
            continue

        # Update stored pair snapshot
        watchlist[addr] = pair

        # --- Job 1: volume spike detection ---
        if vol_m5 >= FILTERS["spike_min_vol_m5"]:
            prev = vol_history.get(addr)
            if prev is not None:
                prev_vol_m5 = prev["vol_m5"]
                if prev_vol_m5 > 0:
                    multiplier = vol_m5 / prev_vol_m5
                    if multiplier >= FILTERS["spike_multiplier"] and not seen_spikes.has(addr):
                        symbol = pair.get("baseToken", {}).get("symbol", "???")
                        print(f"[spike-A] {symbol} — {multiplier:.1f}x vol spike detected")
                        result = await send_spike_alert(
                            session,
                            pair,
                            spike_type="watchlist",
                            prev_vol_m5=prev_vol_m5,
                        )
                        if result in (200, 204):
                            db.record_alert(
                                addr, str(symbol).lstrip("$"), "spikes",
                                _price_of(pair), liquidity=liq,
                            )
                        seen_spikes.add(addr)
                        spikes_found += 1

        vol_history.set(addr, vol_m5, vol_h1)

        # --- Job 2: re-score and promote to alpha ---
        if alpha_promoted.has(addr):
            continue

        created_at = pair.get("pairCreatedAt")
        age_hours = (time.time() - created_at / 1000) / 3600 if created_at else 999

        score, breakdown = compute_base_score(pair, age_hours)

        # Only spend a RugCheck call when the base score is close to alpha
        if score < RESCORE_TRIGGER:
            continue

        rugcheck = await fetch_rugcheck_report(session, addr)
        safety_delta, safety_breakdown, hard_fail = compute_safety_score(rugcheck)

        if hard_fail:
            watchlist.pop(addr, None)
            continue

        score += safety_delta
        breakdown += safety_breakdown
        score = max(0, min(100, score))

        if score >= ALPHA_THRESHOLD:
            symbol = pair.get("baseToken", {}).get("symbol", "???")
            print(f"[promote] {symbol} matured to alpha — score={score}")
            breakdown.append("🔄 Promoted from watchlist re-score")
            result = await send_alpha_with_tldr(
                session, pair, score, breakdown, rugcheck, age_hours
            )
            alpha_promoted.add(addr)
            if result in (200, 204):
                db.record_alert(
                    addr, str(symbol).lstrip("$"), "alpha",
                    _price_of(pair), score=score, liquidity=liq,
                )
                promotions += 1

    vol_history.cleanup()
    print(
        f"[watchlist] Done. Spikes: {spikes_found}, promotions: {promotions}, "
        f"tracking: {len(watchlist)} tokens"
    )


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


async def run_periodic(
    name: str,
    interval_seconds: int,
    fn,
    session: aiohttp.ClientSession,
    initial_delay: int = 0,
):
    """
    Run a scan function forever with a true interval between runs.
    Each scanner is an independent task — a slow scan in one
    monitor never delays the others.
    """
    if initial_delay:
        await asyncio.sleep(initial_delay)

    while True:
        try:
            await fn(session)
        except asyncio.CancelledError:
            print(f"[{name}] Task cancelled")
            return
        except Exception as e:
            print(f"[{name}] Unexpected error: {e}")

        await asyncio.sleep(interval_seconds)


async def main():
    print("=" * 50)
    print("Trench Scanner — starting (parallel monitors)")
    print(f"Chain: {FILTERS['chain_id']} + robinhood")
    print(f"Scan interval: {SCAN_INTERVAL_SECONDS}s")
    print(f"Robinhood interval: {ROBINHOOD_SCAN_INTERVAL_SECONDS}s")
    print(f"Watchlist interval: {WATCHLIST_SCAN_INTERVAL_SECONDS}s")
    print(f"Funding interval: {FUNDING_SCAN_INTERVAL_SECONDS}s")
    print(f"Boost interval: {BOOST_SCAN_INTERVAL_SECONDS}s")
    print(f"Takeover interval: {TAKEOVER_SCAN_INTERVAL_SECONDS}s")
    print(f"Jupiter interval: {JUPITER_SCAN_INTERVAL_SECONDS}s")
    print(f"Multichain interval: {MULTICHAIN_SCAN_INTERVAL_SECONDS}s")
    print(f"NFT intervals: new={NFT_NEW_SCAN_INTERVAL_SECONDS}s, spikes={NFT_SPIKES_SCAN_INTERVAL_SECONDS}s, trending={NFT_TRENDING_SCAN_INTERVAL_SECONDS}s")  # NEW NFT
    print(f"Liquidations: WebSocket (real-time)")
    print(f"Liquidation radar: whale position polling (60s)")
    print(f"Migrations: WebSocket (real-time)")
    print(f"Performance tracking: every {PERF_TRACK_INTERVAL_SECONDS}s, report gated to 6h (restart-proof)")
    print(f"Database: {db.DB_PATH}")
    print(f"Alpha threshold: {ALPHA_THRESHOLD} | Gem threshold: {GEM_THRESHOLD}")
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
        "DISCORD_WEBHOOK_ROBINHOOD",
        "DISCORD_WEBHOOK_ALPHA",
        "DISCORD_WEBHOOK_MIGRATIONS",
        "DISCORD_WEBHOOK_RADAR",
        "DISCORD_WEBHOOK_MULTICHAIN",
        "DISCORD_WEBHOOK_NFT_NEW",       # NEW NFT
        "DISCORD_WEBHOOK_NFT_SPIKES",    # NEW NFT
        "DISCORD_WEBHOOK_NFT_TRENDING",  # NEW NFT
        "OPENSEA_API_KEY",               # NEW NFT
    ]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {missing}")

    async with aiohttp.ClientSession(
        headers={"User-Agent": "TrenchScanner/1.0"}
    ) as session:
        # Initial delays stagger the startup so all monitors don't
        # hammer DexScreener at the same second.
        await asyncio.gather(
            run_periodic("scan", SCAN_INTERVAL_SECONDS, scan_tokens, session),
            run_periodic("watchlist", WATCHLIST_SCAN_INTERVAL_SECONDS, scan_watchlist, session, initial_delay=30),
            run_periodic("robinhood", ROBINHOOD_SCAN_INTERVAL_SECONDS, scan_robinhood, session, initial_delay=10),
            run_periodic("funding", FUNDING_SCAN_INTERVAL_SECONDS, scan_funding, session, initial_delay=5),
            run_periodic("boost", BOOST_SCAN_INTERVAL_SECONDS, scan_boosts, session, initial_delay=15),
            run_periodic("takeover", TAKEOVER_SCAN_INTERVAL_SECONDS, scan_takeovers, session, initial_delay=20),
            run_periodic("jupiter", JUPITER_SCAN_INTERVAL_SECONDS, scan_jupiter, session, initial_delay=25),
            run_periodic("multichain", MULTICHAIN_SCAN_INTERVAL_SECONDS, scan_multichain, session, initial_delay=45),
            run_periodic("nft-new", NFT_NEW_SCAN_INTERVAL_SECONDS, scan_nft_new_collections, session, initial_delay=50),        # NEW NFT
            run_periodic("nft-spikes", NFT_SPIKES_SCAN_INTERVAL_SECONDS, scan_nft_volume_spikes, session, initial_delay=55),    # NEW NFT
            run_periodic("nft-trending", NFT_TRENDING_SCAN_INTERVAL_SECONDS, scan_nft_trending, session, initial_delay=60),     # NEW NFT
            run_periodic("narratives", NARRATIVE_INTERVAL_SECONDS, scan_narratives, session, initial_delay=40),
            run_periodic("perf-track", PERF_TRACK_INTERVAL_SECONDS, track_performance, session, initial_delay=90),
            run_periodic("perf-report", PERF_REPORT_POLL_SECONDS, maybe_send_performance_report, session, initial_delay=120),
            run_liquidation_monitor(session),
            run_pumpportal_client(session),
            run_whale_radar_monitor(session),
        )


if __name__ == "__main__":
    asyncio.run(main())