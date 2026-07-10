# robinhood_monitor.py

import asyncio
import aiohttp
import os
import time
from datetime import datetime, timezone

DEXSCREENER_BASE = "https://api.dexscreener.com"
WEBHOOK_ROBINHOOD = os.environ.get("DISCORD_WEBHOOK_ROBINHOOD", "")

# Expected chainId string on DexScreener — verified via chain page URL.
# If no tokens ever match, check the [robinhood] chainIds log line below.
ROBINHOOD_CHAIN_ID = "robinhood"

# Robinhood Chain is brand new — thresholds are looser than Solana filters
RH_FILTERS = {
    "min_liquidity_usd": 5_000,
    "max_liquidity_usd": 500_000,
    "min_volume_h24": 20_000,
    "min_txns_h24": 100,
    "max_age_hours": 72,
    "min_price_change_h1": 0.0,      # no momentum requirement on a fresh chain
    "max_buy_ratio": 0.92,
    "min_buy_ratio": 0.15,
    "min_liq_to_mcap_ratio": 0.01,
    "honeypot_min_buys_threshold": 50,
    "dedup_ttl_seconds": 86_400,
}

seen_robinhood: dict[str, float] = {}
_logged_chain_ids = False  # log available chainIds once for verification


def _is_deduped(address: str) -> bool:
    ts = seen_robinhood.get(address)
    if ts is None:
        return False
    if time.time() - ts > RH_FILTERS["dedup_ttl_seconds"]:
        del seen_robinhood[address]
        return False
    return True


def _mark_seen(address: str):
    seen_robinhood[address] = time.time()


def _cleanup():
    now = time.time()
    expired = [
        k for k, v in seen_robinhood.items()
        if now - v > RH_FILTERS["dedup_ttl_seconds"]
    ]
    for k in expired:
        del seen_robinhood[k]


def _fmt_usd(value: float) -> str:
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value/1_000:.1f}K"
    return f"${value:.0f}"


def _fmt_ratio(buys: int, sells: int) -> str:
    total = buys + sells
    if total == 0:
        return "N/A"
    pct = buys / total * 100
    bar_filled = round(pct / 10)
    bar = "🟢" * bar_filled + "🔴" * (10 - bar_filled)
    return f"{bar} {pct:.0f}% buys"


def _fmt_socials(info: dict) -> str:
    if not info:
        return "None"
    parts = []
    for social in (info.get("socials") or []):
        platform = social.get("platform", "").lower()
        handle = social.get("handle", "")
        if platform == "twitter" and handle:
            parts.append(f"[𝕏](https://twitter.com/{handle})")
        elif platform == "telegram" and handle:
            parts.append(f"[TG](https://t.me/{handle})")
    for site in (info.get("websites") or []):
        url = site.get("url", "")
        if url:
            parts.append(f"[Web]({url})")
    return " · ".join(parts) if parts else "None"


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    """Safe GET with retry on 429."""
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    wait = 2 ** attempt * 5
                    print(f"[robinhood] Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                print(f"[robinhood] HTTP {resp.status} for {url}")
                return None
        except asyncio.TimeoutError:
            print(f"[robinhood] Timeout, attempt {attempt+1}/3")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[robinhood] Error: {e}")
            return None
    return None


def _passes_filters(pair: dict, age_hours: float) -> tuple[bool, str]:
    """Returns (True, "") if pair passes, (False, reason) if rejected."""
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol = (pair.get("volume") or {}).get("h24") or 0
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total_txns = buys + sells
    ch_h1 = (pair.get("priceChange") or {}).get("h1") or 0
    mcap = pair.get("marketCap") or pair.get("fdv") or 0

    if liq < RH_FILTERS["min_liquidity_usd"]:
        return False, f"liq_low ${liq:,.0f}"
    if liq > RH_FILTERS["max_liquidity_usd"]:
        return False, f"liq_high ${liq:,.0f}"
    if vol < RH_FILTERS["min_volume_h24"]:
        return False, f"vol_low ${vol:,.0f}"
    if total_txns < RH_FILTERS["min_txns_h24"]:
        return False, f"txns_low {total_txns}"
    if age_hours > RH_FILTERS["max_age_hours"]:
        return False, f"too_old {age_hours:.1f}h"
    if ch_h1 < RH_FILTERS["min_price_change_h1"]:
        return False, f"no_momentum {ch_h1:.1f}%"

    if buys >= RH_FILTERS["honeypot_min_buys_threshold"] and sells == 0:
        return False, f"honeypot {buys} buys / 0 sells"

    if total_txns > 0:
        buy_ratio = buys / total_txns
        if buy_ratio > RH_FILTERS["max_buy_ratio"]:
            return False, f"sus_buys {buy_ratio:.0%}"
        if buy_ratio < RH_FILTERS["min_buy_ratio"]:
            return False, f"dump {buy_ratio:.0%} buys"

    if mcap > 0:
        liq_ratio = liq / mcap
        if liq_ratio < RH_FILTERS["min_liq_to_mcap_ratio"]:
            return False, f"rug_risk liq/mcap={liq_ratio:.2%}"

    return True, ""


async def _send_robinhood_alert(
    session: aiohttp.ClientSession,
    pair: dict,
    age_hours: float,
    source: str,
):
    """Send Robinhood Chain gem alert to #robinhood-gems channel."""
    symbol = pair.get("baseToken", {}).get("symbol", "???").lstrip("$")
    name = pair.get("baseToken", {}).get("name", symbol)
    addr = pair.get("baseToken", {}).get("address", "")

    price = pair.get("priceUsd") or "N/A"
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol_h24 = (pair.get("volume") or {}).get("h24") or 0
    vol_m5 = (pair.get("volume") or {}).get("m5") or 0
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    ch_m5 = (pair.get("priceChange") or {}).get("m5") or 0
    ch_h1 = (pair.get("priceChange") or {}).get("h1") or 0
    ch_h6 = (pair.get("priceChange") or {}).get("h6") or 0
    ch_h24 = (pair.get("priceChange") or {}).get("h24") or 0
    mcap = pair.get("marketCap") or pair.get("fdv") or 0
    dex_url = pair.get("url", "")
    dex_id = pair.get("dexId", "?")
    info = pair.get("info") or {}

    vol_liq_ratio = vol_h24 / liq if liq > 0 else 0

    if ch_h1 >= 100:
        color = 0xFF0000
    elif ch_h1 >= 30:
        color = 0xFF8800
    else:
        color = 0x00CC66

    source_label = "🆕 New listing" if source == "latest" else "🔄 Recent update"

    embed = {
        "embeds": [{
            "title": f"🏹 ROBINHOOD GEM — ${symbol}",
            "description": (
                f"**{name}**\n"
                f"{source_label} · DEX: {dex_id}\n"
                f"[📊 DexScreener]({dex_url})"
            ),
            "color": color,
            "fields": [
                {"name": "📋 CA", "value": f"`{addr}`", "inline": False},
                {"name": "💵 Price", "value": f"${price}", "inline": True},
                {"name": "📦 MCap", "value": _fmt_usd(mcap), "inline": True},
                {"name": "⏱️ Age", "value": f"{age_hours:.1f}h", "inline": True},
                {"name": "💧 Liquidity", "value": _fmt_usd(liq), "inline": True},
                {"name": "📊 Vol 24h", "value": _fmt_usd(vol_h24), "inline": True},
                {"name": "⚡ Vol 5m", "value": _fmt_usd(vol_m5), "inline": True},
                {"name": "📈 5m", "value": f"{ch_m5:+.1f}%", "inline": True},
                {"name": "📈 1h", "value": f"{ch_h1:+.1f}%", "inline": True},
                {"name": "📈 6h / 24h", "value": f"{ch_h6:+.1f}% / {ch_h24:+.1f}%", "inline": True},
                {"name": "🔄 Buy/Sell ratio", "value": _fmt_ratio(buys, sells), "inline": False},
                {"name": "📉 Vol/Liq ratio", "value": f"{vol_liq_ratio:.1f}x", "inline": True},
                {"name": "🔗 Socials", "value": _fmt_socials(info), "inline": False},
            ],
            "footer": {"text": "Trench Scanner • Robinhood Chain"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_ROBINHOOD, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[robinhood] Discord send error: {resp.status} {text}")
        return resp.status


async def scan_robinhood(session: aiohttp.ClientSession):
    """Scan DexScreener for new tokens on Robinhood Chain."""
    global _logged_chain_ids

    print(f"[robinhood] {datetime.now().strftime('%H:%M:%S')} — scanning Robinhood Chain...")

    latest, recent = await asyncio.gather(
        _fetch_json(session, f"{DEXSCREENER_BASE}/token-profiles/latest/v1"),
        _fetch_json(session, f"{DEXSCREENER_BASE}/token-profiles/recent-updates/v1"),
    )

    all_profiles_raw = [
        *[(p, "latest") for p in (latest or [])],
        *[(p, "update") for p in (recent or [])],
    ]

    # One-time diagnostic: log all distinct chainIds so we can verify
    # the exact Robinhood chainId string used by the API
    if not _logged_chain_ids and all_profiles_raw:
        chain_ids = sorted({p.get("chainId", "?") for p, _ in all_profiles_raw})
        print(f"[robinhood] chainIds seen in API: {', '.join(chain_ids)}")
        _logged_chain_ids = True

    # Filter to Robinhood chain + dedup within batch
    profiles: list[tuple[dict, str]] = []
    seen_batch: set[str] = set()

    for profile, source in all_profiles_raw:
        if profile.get("chainId") != ROBINHOOD_CHAIN_ID:
            continue
        addr = profile.get("tokenAddress")
        if addr and addr not in seen_batch:
            seen_batch.add(addr)
            profiles.append((profile, source))

    print(f"[robinhood] Profiles to check: {len(profiles)}")

    alerts_sent = 0

    for profile, source in profiles:
        addr = profile.get("tokenAddress")
        if not addr or _is_deduped(addr):
            continue

        await asyncio.sleep(1.0)

        pairs_data = await _fetch_json(
            session,
            f"{DEXSCREENER_BASE}/token-pairs/v1/{ROBINHOOD_CHAIN_ID}/{addr}"
        )

        if not pairs_data:
            _mark_seen(addr)
            continue

        valid_pairs = [p for p in pairs_data if p.get("liquidity")]
        if not valid_pairs:
            _mark_seen(addr)
            continue

        pair = max(valid_pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0))

        created_at = pair.get("pairCreatedAt")
        age_hours = (time.time() - created_at / 1000) / 3600 if created_at else 999

        passed, reason = _passes_filters(pair, age_hours)

        if not passed:
            symbol = pair.get("baseToken", {}).get("symbol", "???")
            print(f"[robinhood] {symbol} rejected — {reason}")
            _mark_seen(addr)
            continue

        result = await _send_robinhood_alert(session, pair, age_hours, source)
        _mark_seen(addr)

        if result in (200, 204):
            symbol = pair.get("baseToken", {}).get("symbol", "???")
            liq = (pair.get("liquidity") or {}).get("usd") or 0
            print(f"[robinhood] ✅ ${symbol.lstrip('$')} [{source}] | age={age_hours:.1f}h | liq=${liq:,.0f}")
            alerts_sent += 1

    _cleanup()
    print(f"[robinhood] Done. Alerts: {alerts_sent}")