# multichain_monitor.py
# Multi-chain discovery monitor: watches trending + new pools across several
# chains at once and alerts when a pool crosses activity + liquidity floors.
#
# This is NOT a "buy this gem" firehose. It's a "noise is waking up on chain X"
# signal — the same philosophy as whale_radar_monitor (what's ABOUT to matter),
# read at the pool level: a pool with rising unique buyers and real reserve is
# where capital is showing up before the crowd notices.
#
# Self-contained (own webhook + embed), same pattern as robinhood_monitor /
# migration_monitor — discord_client.py is untouched.
#
# Data source: GeckoTerminal trending_pools + new_pools (free, 30/min shared).
# Both endpoints already carry reserve + unique buyers/sellers, so ONE call per
# (chain, endpoint) gives full data — no per-token enrichment call needed.
#
# --- Two structural properties worth stating up front ---
# 1. CHAIN ROTATION: scan_multichain scans ONE chain per call, advancing a
#    module-level cursor. Called every 60s from scanner.py, the 4-chain cycle
#    completes every 240s — same per-chain freshness as before, but only 2 gecko
#    calls (trending + new) hit the shared 30/min budget per 60s window instead
#    of 8 in a single burst. That burst was the main remaining 429-storm source
#    once jupiter enrichment was gated.
# 2. PER-CHAIN WEBHOOK + FILTERS: Base is a large, liquid chain that fires far
#    more than the quiet chains, so it gets its OWN channel (DISCORD_WEBHOOK_BASE)
#    and its OWN, stricter floors — otherwise its volume drowns the quiet-chain
#    signals and the shared floor is too loose for it. Any chain without an
#    override uses the default webhook + default floors.
#
# Hard lessons baked into the filters (from live FEFER data on Stable + first
# live run on Base/Monad/Plasma):
#   - Dust/fake pools report absurd "liquidity" with $0 volume -> liquidity
#     floor + volume floor reject them.
#   - Ticker impersonators (FEFERFARM, FEFERR, FEFEREX) share a name but not a
#     contract -> dedup + identity are keyed on the token ADDRESS, never symbol.
#   - The meme is sometimes the QUOTE token ("WgUSDT / FEFER") -> we pick the
#     non-boring side of the pair as the token of interest.
#   - Infrastructure pools (USDT0/WXPL, WBTC/MON, USDC/MON) have BOTH sides
#     boring -> they are not signals and are skipped entirely.

import asyncio
import aiohttp
import os
import time
from datetime import datetime, timezone

import db
from geckoterminal_client import fetch_trending_pools, fetch_new_pools

# Default webhook for chains without their own. Base routes to its own channel.
WEBHOOK_MULTICHAIN = os.environ.get("DISCORD_WEBHOOK_MULTICHAIN", "")

# Default activity + liquidity floors. Loose (fresh, quiet chains), tuned on
# real data later. A chain can override any subset of these via CHAINS[..]
# ["filter_overrides"]; unspecified keys fall back to these.
FILTERS = {
    "min_reserve_usd": 10_000,      # dust/fake pools have groschen reserve
    "min_volume_h24": 20_000,       # a pool nobody trades is not a signal
    "min_unique_buyers_h1": 15,     # real interest, not one bot doing 300 txns
    "max_price_usd": 1_000_000,     # 20-digit price = artifact pool, reject
    "dedup_ttl_seconds": 86_400,    # alert a given token once per chain per day
}

# Base-specific, stricter floors. Base is liquid enough that the default floor
# lets through near-launch churn; these were picked from live Base alerts
# (reserve $15-45k / vol $24-51k / buyers 40-120 range) to surface "something is
# waking up" rather than every fresh pool. Starting values — tune on data.
# Only the three activity floors are raised; max_price_usd and dedup_ttl_seconds
# inherit from FILTERS (they are guards, not signal strength).
BASE_FILTER_OVERRIDES = {
    "min_reserve_usd": 25_000,
    "min_volume_h24": 50_000,
    "min_unique_buyers_h1": 35,
}

# Chains to watch. Key = GeckoTerminal network id (verified live against
# /networks). All four are EVM (0x addresses). Sui ('sui-network') is Move and
# is deliberately left out of v1 — its address format differs and needs its own
# checkpoint. 'label' is display-only; 'dexscreener' is the chainId used to
# build a DexScreener link (matches GeckoTerminal id for these chains).
# 'webhook_env' + 'filter_overrides' are optional per-chain routing/tuning.
CHAINS: dict[str, dict] = {
    "stable":  {"label": "Stable",  "dexscreener": "stable"},
    "plasma":  {"label": "Plasma",  "dexscreener": "plasma"},
    "base":    {
        "label": "Base",
        "dexscreener": "base",
        "webhook_env": "DISCORD_WEBHOOK_BASE",
        "filter_overrides": BASE_FILTER_OVERRIDES,
    },
    "monad":   {"label": "Monad",   "dexscreener": "monad"},
}

# Stable rotation order + a module-level cursor so each call scans one chain.
_CHAIN_ORDER = list(CHAINS.keys())
_chain_index = 0

# Symbols that are NEVER the "interesting" token — stablecoins, wrapped/native
# gas tokens, and liquid-staked natives. When one side of a pair is boring, the
# other side is the token of interest; when BOTH are boring, it's an infra pool
# and gets skipped. Compared upper-cased. Extend as new chains bring new quotes.
BORING_QUOTES = {
    "USDT", "USDT0", "GUSDT", "WGUSDT", "USDC", "USDC.E", "DAI",
    "WETH", "ETH", "WBTC", "BTC", "SOL", "WSOL",
    "WXPL", "XPL", "WMON", "MON", "APRMON", "STABLE", "USD1",
    "CBBTC", "WSTETH", "WEETH",
}

# In-memory dedup keyed on "network:token_address". Restart-persisted via kv so
# a redeploy doesn't replay a wave of alerts (same lesson as the perf report).
_KV_SEEN_KEY = "multichain_seen"
_seen: dict[str, float] = {}
_seen_loaded = False


def _filters_for(network: str) -> dict:
    """Default floors merged with any per-chain overrides (Base = stricter)."""
    merged = dict(FILTERS)
    override = CHAINS.get(network, {}).get("filter_overrides") or {}
    merged.update(override)
    return merged


def _webhook_for(network: str) -> str:
    """
    Resolve the Discord webhook for a chain. A chain with its own 'webhook_env'
    (Base) uses that; if that env var is unset we fall back to the default
    webhook rather than dropping the chain, so a missing DISCORD_WEBHOOK_BASE
    degrades to the shared channel instead of losing Base signals silently.
    """
    env = CHAINS.get(network, {}).get("webhook_env")
    if env:
        specific = os.environ.get(env, "")
        if specific:
            return specific
    return WEBHOOK_MULTICHAIN


def _load_seen():
    """Load persisted dedup map once, dropping entries past TTL."""
    global _seen, _seen_loaded
    if _seen_loaded:
        return
    stored = db.kv_get_json(_KV_SEEN_KEY, default={}) or {}
    cutoff = time.time() - FILTERS["dedup_ttl_seconds"]
    _seen = {k: ts for k, ts in stored.items() if ts > cutoff}
    _seen_loaded = True
    print(f"[multichain] Loaded {len(_seen)} dedup entries from db")


def _persist_seen():
    db.kv_set_json(_KV_SEEN_KEY, _seen)


def _is_deduped(key: str) -> bool:
    ts = _seen.get(key)
    if ts is None:
        return False
    if time.time() - ts > FILTERS["dedup_ttl_seconds"]:
        del _seen[key]
        return False
    return True


def _mark_seen(key: str):
    _seen[key] = time.time()


def _select_interesting(pool: dict) -> tuple[str | None, str | None]:
    """
    Decide which side of the pair is the token of interest (the meme), returning
    (address, symbol). The meme is the NON-boring side. If BOTH sides are boring
    (stablecoin/native infra pool — USDT0/WXPL, WBTC/MON, etc.) there is nothing
    interesting to alert on -> return (None, None) so the caller skips it.
    """
    base_addr = pool.get("base_address")
    base_sym = (pool.get("base_symbol") or "").upper()
    quote_addr = pool.get("quote_address")
    quote_sym = (pool.get("quote_symbol") or "").upper()

    base_boring = base_sym in BORING_QUOTES
    quote_boring = quote_sym in BORING_QUOTES

    # Both sides boring = infrastructure pool, not a signal. Skip entirely.
    if base_boring and quote_boring:
        return None, None
    # Base is boring but quote isn't -> the meme is the quote token.
    if base_boring and not quote_boring and quote_addr:
        return quote_addr, pool.get("quote_symbol")
    # Quote is boring but base isn't (normal case) -> base is the meme.
    if quote_boring and not base_boring and base_addr:
        return base_addr, pool.get("base_symbol")
    # Neither boring (e.g. two memes paired) -> default to base.
    if base_addr:
        return base_addr, pool.get("base_symbol")
    if quote_addr:
        return quote_addr, pool.get("quote_symbol")
    return None, None


def _passes(pool: dict, filters: dict) -> tuple[bool, str]:
    """Activity + liquidity floors (per-chain). Returns (ok, reject_reason)."""
    reserve = pool.get("reserve_usd") or 0
    vol_h24 = pool.get("volume_h24") or 0
    buyers_h1 = pool.get("buyers_h1") or 0
    price = pool.get("price_usd")

    if reserve < filters["min_reserve_usd"]:
        return False, f"reserve_low ${reserve:,.0f}"
    if vol_h24 < filters["min_volume_h24"]:
        return False, f"vol_low ${vol_h24:,.0f}"
    if buyers_h1 < filters["min_unique_buyers_h1"]:
        return False, f"buyers_low {buyers_h1}"
    # Artifact guard: a pool priced in the millions per token is a broken/fake
    # pool (saw a 20-digit priceUsd on a STABLE/FEFER dust pool).
    if price is not None and price > filters["max_price_usd"]:
        return False, f"price_artifact ${price:,.0f}"
    return True, ""


def _fmt_usd(value: float | None) -> str:
    if not value:
        return "$0"
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value/1_000:.1f}K"
    return f"${value:.0f}"


def _fmt_px(px: float | None) -> str:
    if px is None:
        return "N/A"
    if px >= 1:
        return f"${px:,.4f}"
    return f"${px:.8f}".rstrip("0").rstrip(".")


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "n/a"
    return f"{v:+.1f}%"


async def _send_alert(session: aiohttp.ClientSession, pool: dict,
                      token_addr: str, token_sym: str | None,
                      chain_label: str, source: str, webhook: str) -> int | None:
    """Build + POST the multi-chain discovery embed. Returns HTTP status."""
    sym = (token_sym or "???").lstrip("$")
    reserve = pool.get("reserve_usd") or 0
    vol_h24 = pool.get("volume_h24") or 0
    buyers_h1 = pool.get("buyers_h1")
    sellers_h1 = pool.get("sellers_h1")
    buyers_h24 = pool.get("buyers_h24")
    sellers_h24 = pool.get("sellers_h24")
    mcap = pool.get("market_cap_usd") or pool.get("fdv_usd") or 0
    dex_id = pool.get("dex_id") or "?"
    network = pool.get("network")
    pool_addr = pool.get("pool_address")

    gecko_url = (
        f"https://www.geckoterminal.com/{network}/pools/{pool_addr}"
        if pool_addr else ""
    )
    ds_chain = CHAINS.get(network, {}).get("dexscreener", network)
    ds_url = (
        f"https://dexscreener.com/{ds_chain}/{pool_addr}" if pool_addr else ""
    )

    source_label = "🆕 New pool" if source == "new" else "🔥 Trending"
    links = []
    if gecko_url:
        links.append(f"[GeckoTerminal]({gecko_url})")
    if ds_url:
        links.append(f"[DexScreener]({ds_url})")
    links_str = " · ".join(links) if links else "—"

    unique_h1 = (
        f"{buyers_h1 if buyers_h1 is not None else '?'} buyers / "
        f"{sellers_h1 if sellers_h1 is not None else '?'} sellers"
    )
    unique_h24 = (
        f"{buyers_h24 if buyers_h24 is not None else '?'} buyers / "
        f"{sellers_h24 if sellers_h24 is not None else '?'} sellers"
    )

    embed = {
        "embeds": [{
            "title": f"🌐 {chain_label.upper()} — ${sym}",
            "description": (
                f"**{pool.get('name') or sym}**\n"
                f"{source_label} · DEX: {dex_id}\n"
                f"{links_str}"
            ),
            "color": 0x00B0FF,
            "fields": [
                {"name": "📋 CA", "value": f"`{token_addr}`", "inline": False},
                {"name": "💵 Price", "value": _fmt_px(pool.get("price_usd")), "inline": True},
                {"name": "📦 MCap/FDV", "value": _fmt_usd(mcap), "inline": True},
                {"name": "💧 Reserve", "value": _fmt_usd(reserve), "inline": True},
                {"name": "📊 Vol 24h", "value": _fmt_usd(vol_h24), "inline": True},
                {"name": "📈 1h / 24h", "value": f"{_fmt_pct(pool.get('price_change_h1'))} / {_fmt_pct(pool.get('price_change_h24'))}", "inline": True},
                {"name": "👥 Unique 1h", "value": unique_h1, "inline": True},
                {"name": "👥 Unique 24h", "value": unique_h24, "inline": False},
            ],
            "footer": {"text": "Trench Scanner • Multi-Chain Radar • early/thin — DYOR"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(webhook, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[multichain] Discord send error: {resp.status} {text}")
        return resp.status


async def _scan_one_chain(session: aiohttp.ClientSession, network: str) -> tuple[int, int, int]:
    """
    Scan a single chain's trending + new pools and alert on ones that clear that
    chain's floors. Returns (alerts, rejects, skipped_infra). Pulls the two
    endpoints sequentially (rate limiter serializes anyway); with one chain per
    call this is just 2 gecko calls per invocation. Richest pool per token wins
    (sorted by reserve desc, so the first pool seen for a token is its deepest).
    """
    meta = CHAINS[network]
    webhook = _webhook_for(network)
    if not webhook:
        print(f"[multichain] No webhook for {meta['label']} — skipping "
              f"(set DISCORD_WEBHOOK_MULTICHAIN or its own env)")
        return 0, 0, 0

    filters = _filters_for(network)

    trending = await fetch_trending_pools(session, network)
    new = await fetch_new_pools(session, network)

    # Tag source so the embed can say where it came from; trending first so a
    # pool present in both is labeled trending (the stronger signal).
    tagged = [(p, "trending") for p in trending] + [(p, "new") for p in new]

    # Sort by reserve desc so the deepest pool for a given token is seen first;
    # later (thinner) pools for the same token are deduped out.
    tagged.sort(key=lambda t: (t[0].get("reserve_usd") or 0), reverse=True)

    alerts = 0
    rejects = 0
    skipped_infra = 0
    batch_seen: set[str] = set()

    for pool, source in tagged:
        token_addr, token_sym = _select_interesting(pool)
        if not token_addr:
            # Both sides boring (infra pool) or no usable address — skip.
            skipped_infra += 1
            continue

        key = f"{network}:{token_addr.lower()}"
        if key in batch_seen or _is_deduped(key):
            continue

        ok, reason = _passes(pool, filters)
        if not ok:
            rejects += 1
            continue

        batch_seen.add(key)
        result = await _send_alert(
            session, pool, token_addr, token_sym, meta["label"], source, webhook
        )
        if result in (200, 204):
            _mark_seen(key)
            alerts += 1
            print(
                f"[multichain] ✅ ${(token_sym or '???').lstrip('$')} "
                f"[{meta['label']}] {source} | "
                f"reserve={_fmt_usd(pool.get('reserve_usd'))} | "
                f"vol24h={_fmt_usd(pool.get('volume_h24'))} | "
                f"buyers1h={pool.get('buyers_h1')}"
            )

    return alerts, rejects, skipped_infra


async def scan_multichain(session: aiohttp.ClientSession):
    """
    Scan ONE chain per call, rotating through _CHAIN_ORDER. Called every 60s
    from scanner.py, the full 4-chain cycle completes every 240s — same
    per-chain freshness as the old all-at-once 240s pass, but only 2 gecko calls
    per 60s window instead of an 8-call burst, which is what was driving the
    remaining 429 storm.
    """
    global _chain_index

    if not WEBHOOK_MULTICHAIN and not any(
        os.environ.get(c.get("webhook_env", ""), "") for c in CHAINS.values()
    ):
        print("[multichain] No multichain webhooks set — skipping")
        return

    _load_seen()

    network = _CHAIN_ORDER[_chain_index % len(_CHAIN_ORDER)]
    _chain_index += 1
    meta = CHAINS[network]

    print(
        f"[multichain] {datetime.now().strftime('%H:%M:%S')} — "
        f"scanning {meta['label']} "
        f"({(_chain_index - 1) % len(_CHAIN_ORDER) + 1}/{len(_CHAIN_ORDER)})..."
    )

    alerts, rejects, skipped_infra = await _scan_one_chain(session, network)

    if alerts:
        _persist_seen()
    print(
        f"[multichain] {meta['label']} done. Alerts: {alerts}, "
        f"rejected (floors): {rejects}, skipped (infra): {skipped_infra}"
    )