# discord_client.py

import aiohttp
import os
from datetime import datetime, timezone

WEBHOOK_GEMS = os.environ["DISCORD_WEBHOOK_GEMS"]
WEBHOOK_NARRATIVES = os.environ["DISCORD_WEBHOOK_NARRATIVES"]
WEBHOOK_SPIKES = os.environ["DISCORD_WEBHOOK_SPIKES"]


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


async def send_gem_alert(session: aiohttp.ClientSession, token_data: dict):
    pair = token_data["pair"]
    source = token_data.get("source", "?")

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
    age_hours = token_data["age_hours"]
    info = pair.get("info") or {}
    boosts = (pair.get("boosts") or {}).get("active", 0)

    vol_liq_ratio = vol_h24 / liq if liq > 0 else 0
    boost_str = f"🚀 {boosts} active" if boosts else "None"

    if ch_h1 >= 100:
        color = 0xFF0000
    elif ch_h1 >= 30:
        color = 0xFF8800
    else:
        color = 0x00FF88

    source_label = "🆕 New listing" if source == "latest" else "🔄 Recent update"

    embed = {
        "embeds": [{
            "title": f"🚀 EARLY GEM — ${symbol}",
            "description": (
                f"**{name}**\n"
                f"{source_label}\n"
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
                {"name": "🚀 Boosts", "value": boost_str, "inline": True},
                {"name": "🔗 Socials", "value": _fmt_socials(info), "inline": False},
            ],
            "footer": {"text": "Trench Scanner • DexScreener"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_GEMS, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Send error: {resp.status} {text}")
        return resp.status


async def send_spike_alert(
    session: aiohttp.ClientSession,
    pair: dict,
    spike_type: str,
    prev_vol_m5: float | None = None,
):
    """
    spike_type: "watchlist" (A) or "inline" (B)
    prev_vol_m5: previous snapshot value, only for watchlist spikes
    """
    symbol = pair.get("baseToken", {}).get("symbol", "???").lstrip("$")
    name = pair.get("baseToken", {}).get("name", symbol)
    addr = pair.get("baseToken", {}).get("address", "")

    price = pair.get("priceUsd") or "N/A"
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol_h24 = (pair.get("volume") or {}).get("h24") or 0
    vol_h1 = (pair.get("volume") or {}).get("h1") or 0
    vol_m5 = (pair.get("volume") or {}).get("m5") or 0
    txns_h24 = (pair.get("txns") or {}).get("h24") or {}
    txns_m5 = (pair.get("txns") or {}).get("m5") or {}
    buys_h24 = txns_h24.get("buys", 0)
    sells_h24 = txns_h24.get("sells", 0)
    buys_m5 = txns_m5.get("buys", 0)
    sells_m5 = txns_m5.get("sells", 0)
    ch_m5 = (pair.get("priceChange") or {}).get("m5") or 0
    ch_h1 = (pair.get("priceChange") or {}).get("h1") or 0
    mcap = pair.get("marketCap") or pair.get("fdv") or 0
    dex_url = pair.get("url", "")
    info = pair.get("info") or {}

    # Spike multiplier for watchlist type
    if spike_type == "watchlist" and prev_vol_m5 and prev_vol_m5 > 0:
        multiplier = vol_m5 / prev_vol_m5
        spike_label = f"⚡ Watchlist spike — {multiplier:.1f}x vol surge"
        spike_detail = f"Previous 5m vol: {_fmt_usd(prev_vol_m5)} → Now: {_fmt_usd(vol_m5)}"
    else:
        m5_to_h1 = (vol_m5 / vol_h1 * 100) if vol_h1 > 0 else 0
        spike_label = f"⚡ Inline spike — {m5_to_h1:.0f}% of 1h vol in last 5m"
        spike_detail = f"Vol 5m: {_fmt_usd(vol_m5)} / Vol 1h: {_fmt_usd(vol_h1)}"

    embed = {
        "embeds": [{
            "title": f"⚡ VOLUME SPIKE — ${symbol}",
            "description": (
                f"**{name}**\n"
                f"{spike_label}\n"
                f"[📊 DexScreener]({dex_url})"
            ),
            "color": 0xFFFF00,
            "fields": [
                {"name": "📋 CA", "value": f"`{addr}`", "inline": False},

                # Spike detail
                {"name": "📊 Spike detail", "value": spike_detail, "inline": False},

                # Price & market
                {"name": "💵 Price", "value": f"${price}", "inline": True},
                {"name": "📦 MCap", "value": _fmt_usd(mcap), "inline": True},
                {"name": "💧 Liquidity", "value": _fmt_usd(liq), "inline": True},

                # Volume breakdown
                {"name": "⚡ Vol 5m", "value": _fmt_usd(vol_m5), "inline": True},
                {"name": "📊 Vol 1h", "value": _fmt_usd(vol_h1), "inline": True},
                {"name": "📊 Vol 24h", "value": _fmt_usd(vol_h24), "inline": True},

                # Price changes
                {"name": "📈 5m", "value": f"{ch_m5:+.1f}%", "inline": True},
                {"name": "📈 1h", "value": f"{ch_h1:+.1f}%", "inline": True},

                # Txns
                {"name": "🔄 Txns 5m", "value": f"{buys_m5}↑ {sells_m5}↓", "inline": True},
                {"name": "🔄 Buy/Sell 24h", "value": _fmt_ratio(buys_h24, sells_h24), "inline": False},

                # Socials
                {"name": "🔗 Socials", "value": _fmt_socials(info), "inline": False},
            ],
            "footer": {"text": "Trench Scanner • Volume Spike"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_SPIKES, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Spike send error: {resp.status} {text}")
        return resp.status


async def send_narrative_update(session: aiohttp.ClientSession, metas: list[dict]):
    if not metas:
        return

    fields = []
    for meta in metas[:5]:
        ch_h1 = (meta.get("marketCapChange") or {}).get("h1", 0) or 0
        ch_h24 = (meta.get("marketCapChange") or {}).get("h24", 0) or 0
        mcap = meta.get("marketCap", 0) or 0
        vol = meta.get("volume", 0) or 0
        token_count = meta.get("tokenCount", 0)
        icon = "🔥" if ch_h1 > 10 else "📡"
        fields.append({
            "name": f"{icon} {meta.get('name', '?')}",
            "value": (
                f"MCap: {_fmt_usd(mcap)} · Vol: {_fmt_usd(vol)}\n"
                f"1h: {ch_h1:+.1f}% · 24h: {ch_h24:+.1f}% · Tokens: {token_count}"
            ),
            "inline": False,
        })

    embed = {
        "embeds": [{
            "title": "📡 TRENDING NARRATIVES",
            "color": 0xFF6600,
            "fields": fields,
            "footer": {"text": "Trench Scanner • DexScreener Metas"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_NARRATIVES, json=embed) as resp:
        return resp.status