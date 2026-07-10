# discord_client.py

import aiohttp
import os
from datetime import datetime, timezone

WEBHOOK_GEMS = os.environ["DISCORD_WEBHOOK_GEMS"]
WEBHOOK_NARRATIVES = os.environ["DISCORD_WEBHOOK_NARRATIVES"]
WEBHOOK_SPIKES = os.environ["DISCORD_WEBHOOK_SPIKES"]
WEBHOOK_FUNDING = os.environ["DISCORD_WEBHOOK_FUNDING"]
WEBHOOK_LIQUIDATIONS = os.environ["DISCORD_WEBHOOK_LIQUIDATIONS"]
WEBHOOK_BOOSTED = os.environ["DISCORD_WEBHOOK_BOOSTED"]
WEBHOOK_TAKEOVERS = os.environ["DISCORD_WEBHOOK_TAKEOVERS"]
WEBHOOK_JUPITER = os.environ["DISCORD_WEBHOOK_JUPITER"]


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


def _fmt_boost_links(links: list) -> str:
    if not links:
        return "None"
    parts = []
    for link in links:
        label = link.get("label") or link.get("type", "Link")
        url = link.get("url", "")
        if url:
            parts.append(f"[{label}]({url})")
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
                {"name": "📊 Spike detail", "value": spike_detail, "inline": False},
                {"name": "💵 Price", "value": f"${price}", "inline": True},
                {"name": "📦 MCap", "value": _fmt_usd(mcap), "inline": True},
                {"name": "💧 Liquidity", "value": _fmt_usd(liq), "inline": True},
                {"name": "⚡ Vol 5m", "value": _fmt_usd(vol_m5), "inline": True},
                {"name": "📊 Vol 1h", "value": _fmt_usd(vol_h1), "inline": True},
                {"name": "📊 Vol 24h", "value": _fmt_usd(vol_h24), "inline": True},
                {"name": "📈 5m", "value": f"{ch_m5:+.1f}%", "inline": True},
                {"name": "📈 1h", "value": f"{ch_h1:+.1f}%", "inline": True},
                {"name": "🔄 Txns 5m", "value": f"{buys_m5}↑ {sells_m5}↓", "inline": True},
                {"name": "🔄 Buy/Sell 24h", "value": _fmt_ratio(buys_h24, sells_h24), "inline": False},
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


async def send_funding_alert(session: aiohttp.ClientSession, data: dict):
    coin = data["coin"]
    funding = data["funding"]
    annual_rate = data["annual_rate"]
    oi_usd = data["oi_usd"]
    oi_change_pct = data.get("oi_change_pct")
    mark_px = data["mark_px"]
    day_volume = data["day_volume"]
    signal = data["signal"]
    premium = data["premium"]

    if signal == "high":
        color = 0xFF0000
        title = f"🔴 HIGH FUNDING — {coin}"
        signal_desc = (
            f"Overcrowded **LONG** — longs paying shorts\n"
            f"Rate: **{funding:.4%}/h** ({annual_rate:.1f}% APR)\n"
            f"Potential short squeeze setup"
        )
    elif signal == "low":
        color = 0x00AAFF
        title = f"🔵 NEGATIVE FUNDING — {coin}"
        signal_desc = (
            f"Overcrowded **SHORT** — shorts paying longs\n"
            f"Rate: **{funding:.4%}/h** ({annual_rate:.1f}% APR)\n"
            f"Potential long squeeze setup"
        )
    else:
        color = 0xFFAA00
        title = f"⚠️ OI SPIKE — {coin}"
        direction = "↑" if (oi_change_pct or 0) > 0 else "↓"
        pct_str = f"{abs(oi_change_pct or 0):.1%}"
        signal_desc = (
            f"Open Interest moved **{direction}{pct_str}** since last scan\n"
            f"Current funding: **{funding:.4%}/h** ({annual_rate:.1f}% APR)\n"
            f"Large positions opening now"
        )

    def _funding_bar(rate: float) -> str:
        clamped = max(-0.001, min(0.001, rate))
        normalized = (clamped + 0.001) / 0.002
        filled = round(normalized * 10)
        return "🟥" * filled + "🟦" * (10 - filled) + f"  {rate:+.4%}/h"

    oi_change_str = (
        f"{oi_change_pct:+.1%} since last scan"
        if oi_change_pct is not None
        else "First snapshot"
    )

    embed = {
        "embeds": [{
            "title": title,
            "description": signal_desc,
            "color": color,
            "fields": [
                {"name": "📊 Funding rate", "value": _funding_bar(funding), "inline": False},
                {"name": "💵 Mark price", "value": f"${mark_px:,.4f}", "inline": True},
                {"name": "📦 Open Interest", "value": _fmt_usd(oi_usd), "inline": True},
                {"name": "📈 OI change", "value": oi_change_str, "inline": True},
                {"name": "📊 24h Volume", "value": _fmt_usd(day_volume), "inline": True},
                {"name": "📉 Premium", "value": f"{premium:+.4%}", "inline": True},
                {
                    "name": "🔗 Trade",
                    "value": f"[Hyperliquid](https://app.hyperliquid.xyz/trade/{coin})",
                    "inline": True
                },
            ],
            "footer": {"text": "Trench Scanner • Hyperliquid Funding"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_FUNDING, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Funding alert error: {resp.status} {text}")
        return resp.status


async def send_liquidation_alert(session: aiohttp.ClientSession, data: dict):
    coin = data["coin"]
    side = data["side"]
    price = data["price"]
    size = data["size"]
    usd_value = data["usd_value"]
    is_liquidation = data["is_liquidation"]

    if side == "A":
        side_label = "SHORT liquidated 🔴"
        color = 0xFF0000
    else:
        side_label = "LONG liquidated 🟢"
        color = 0x00FF88

    if is_liquidation:
        title = f"💥 LIQUIDATION — {coin}"
        event_type = "Liquidation"
    else:
        title = f"🐋 LARGE TRADE — {coin}"
        event_type = "Large trade"

    if usd_value >= 1_000_000:
        size_label = f"${usd_value/1_000_000:.2f}M"
    else:
        size_label = f"${usd_value/1_000:.0f}K"

    embed = {
        "embeds": [{
            "title": title,
            "description": (
                f"{'🔴' if side == 'A' else '🟢'} **{side_label}**\n"
                f"Size: **{size_label}**\n"
                f"[📊 Hyperliquid](https://app.hyperliquid.xyz/trade/{coin})"
            ),
            "color": color,
            "fields": [
                {"name": "💥 Type", "value": event_type, "inline": True},
                {"name": "💵 Price", "value": f"${price:,.4f}", "inline": True},
                {"name": "📦 Size", "value": f"{size:,.2f} {coin}", "inline": True},
                {"name": "💰 USD Value", "value": size_label, "inline": True},
                {"name": "📊 Side", "value": side_label, "inline": True},
                {
                    "name": "🔗 Chart",
                    "value": f"[View on Hyperliquid](https://app.hyperliquid.xyz/trade/{coin})",
                    "inline": True
                },
            ],
            "footer": {"text": "Trench Scanner • Hyperliquid Liquidations"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_LIQUIDATIONS, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Liquidation alert error: {resp.status} {text}")
        return resp.status


async def send_boost_alert(session: aiohttp.ClientSession, token_data: dict):
    boost = token_data["boost"]
    pair = token_data.get("pair")

    addr = boost.get("tokenAddress", "")
    dex_url = boost.get("url", "")
    amount = boost.get("amount", 0) or 0
    total_amount = boost.get("totalAmount", 0) or 0
    description = boost.get("description", "") or ""
    links = boost.get("links") or []
    source = boost.get("source", "latest")

    if pair:
        symbol = pair.get("baseToken", {}).get("symbol", "???").lstrip("$")
        name = pair.get("baseToken", {}).get("name", symbol)
        price = pair.get("priceUsd") or "N/A"
        liq = (pair.get("liquidity") or {}).get("usd") or 0
        vol_h24 = (pair.get("volume") or {}).get("h24") or 0
        mcap = pair.get("marketCap") or pair.get("fdv") or 0
        ch_h1 = (pair.get("priceChange") or {}).get("h1") or 0
        ch_h24 = (pair.get("priceChange") or {}).get("h24") or 0
        txns = (pair.get("txns") or {}).get("h24") or {}
        buys = txns.get("buys", 0)
        sells = txns.get("sells", 0)
    else:
        symbol = addr[:8] + "..."
        name = "Unknown"
        price = "N/A"
        liq = mcap = vol_h24 = ch_h1 = ch_h24 = buys = sells = 0

    if total_amount >= 500:
        tier = "🔥🔥🔥 MEGA BOOST"
        color = 0xFF0000
    elif total_amount >= 200:
        tier = "🔥🔥 HEAVY BOOST"
        color = 0xFF6600
    elif total_amount >= 50:
        tier = "🔥 BOOSTED"
        color = 0xFFAA00
    else:
        tier = "⚡ NEW BOOST"
        color = 0xFFFF00

    source_label = "🆕 New boost" if source == "latest" else "🏆 Top boosted"
    desc_display = (description[:200] + "...") if len(description) > 200 else description

    fields = [{"name": "📋 CA", "value": f"`{addr}`", "inline": False}]

    if desc_display:
        fields.append({"name": "📝 Description", "value": desc_display, "inline": False})

    fields += [
        {"name": "⚡ New boosts", "value": str(amount), "inline": True},
        {"name": "🔥 Total boosts", "value": str(total_amount), "inline": True},
        {"name": "📊 Source", "value": source_label, "inline": True},
    ]

    if pair:
        fields += [
            {"name": "💵 Price", "value": f"${price}", "inline": True},
            {"name": "📦 MCap", "value": _fmt_usd(mcap), "inline": True},
            {"name": "💧 Liquidity", "value": _fmt_usd(liq), "inline": True},
            {"name": "📊 Vol 24h", "value": _fmt_usd(vol_h24), "inline": True},
            {"name": "📈 1h", "value": f"{ch_h1:+.1f}%", "inline": True},
            {"name": "📈 24h", "value": f"{ch_h24:+.1f}%", "inline": True},
            {"name": "🔄 Buy/Sell 24h", "value": _fmt_ratio(buys, sells), "inline": False},
        ]

    boost_links = _fmt_boost_links(links)
    if boost_links != "None":
        fields.append({"name": "🔗 Links", "value": boost_links, "inline": False})

    embed = {
        "embeds": [{
            "title": f"🚀 {tier} — ${symbol}",
            "description": f"**{name}**\n[📊 DexScreener]({dex_url})",
            "color": color,
            "fields": fields,
            "footer": {"text": "Trench Scanner • DexScreener Boosts"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_BOOSTED, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Boost alert error: {resp.status} {text}")
        return resp.status


async def send_takeover_alert(session: aiohttp.ClientSession, token_data: dict):
    """Send community takeover alert."""
    takeover = token_data["takeover"]
    pair = token_data.get("pair")

    addr = takeover.get("tokenAddress", "")
    dex_url = takeover.get("url", "")
    description = takeover.get("description", "") or ""
    claim_date = takeover.get("claimDate", "") or ""
    links = takeover.get("links") or []

    if pair:
        symbol = pair.get("baseToken", {}).get("symbol", "???").lstrip("$")
        name = pair.get("baseToken", {}).get("name", symbol)
        price = pair.get("priceUsd") or "N/A"
        liq = (pair.get("liquidity") or {}).get("usd") or 0
        vol_h24 = (pair.get("volume") or {}).get("h24") or 0
        mcap = pair.get("marketCap") or pair.get("fdv") or 0
        ch_h1 = (pair.get("priceChange") or {}).get("h1") or 0
        ch_h24 = (pair.get("priceChange") or {}).get("h24") or 0
        txns = (pair.get("txns") or {}).get("h24") or {}
        buys = txns.get("buys", 0)
        sells = txns.get("sells", 0)
    else:
        symbol = addr[:8] + "..."
        name = "Unknown"
        price = "N/A"
        liq = mcap = vol_h24 = ch_h1 = ch_h24 = buys = sells = 0

    claim_str = claim_date[:10] if len(claim_date) >= 10 else "Unknown"
    desc_display = (description[:200] + "...") if len(description) > 200 else description

    fields = [
        {"name": "📋 CA", "value": f"`{addr}`", "inline": False},
        {"name": "📅 Claimed", "value": claim_str, "inline": True},
    ]

    if desc_display:
        fields.append({"name": "📝 Description", "value": desc_display, "inline": False})

    if pair:
        fields += [
            {"name": "💵 Price", "value": f"${price}", "inline": True},
            {"name": "📦 MCap", "value": _fmt_usd(mcap), "inline": True},
            {"name": "💧 Liquidity", "value": _fmt_usd(liq), "inline": True},
            {"name": "📊 Vol 24h", "value": _fmt_usd(vol_h24), "inline": True},
            {"name": "📈 1h", "value": f"{ch_h1:+.1f}%", "inline": True},
            {"name": "📈 24h", "value": f"{ch_h24:+.1f}%", "inline": True},
            {"name": "🔄 Buy/Sell 24h", "value": _fmt_ratio(buys, sells), "inline": False},
        ]

    takeover_links = _fmt_boost_links(links)
    if takeover_links != "None":
        fields.append({"name": "🔗 Links", "value": takeover_links, "inline": False})

    embed = {
        "embeds": [{
            "title": f"🏴 COMMUNITY TAKEOVER — ${symbol}",
            "description": (
                f"**{name}**\n"
                f"Community claimed this project\n"
                f"[📊 DexScreener]({dex_url})"
            ),
            "color": 0x9B59B6,
            "fields": fields,
            "footer": {"text": "Trench Scanner • DexScreener Takeovers"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_TAKEOVERS, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Takeover alert error: {resp.status} {text}")
        return resp.status


async def send_jupiter_alert(session: aiohttp.ClientSession, token_data: dict):
    """Send Jupiter high-volume token alert."""
    token = token_data["token"]
    price_data = token_data.get("price_data")
    daily_volume = token_data.get("daily_volume", 0)

    symbol = (token.get("symbol") or "???").lstrip("$")
    name = token.get("name") or symbol
    addr = token.get("address") or ""
    created_at = token.get("created_at") or ""

    # Price from Jupiter price API
    if price_data:
        price = price_data.get("usdPrice") or price_data.get("price") or "N/A"
        confidence = price_data.get("extraInfo", {}).get("confidenceLevel") or "unknown"
        price_str = f"${float(price):,.6f}" if price != "N/A" else "N/A"
    else:
        price_str = "N/A"
        confidence = "unknown"

    # Token metadata
    tags = token.get("tags") or []
    tags_str = ", ".join(tags[:5]) if tags else "None"

    # Volume tier label
    if daily_volume >= 1_000_000:
        tier = "🔥🔥🔥 MEGA VOLUME"
        color = 0xFF0000
    elif daily_volume >= 500_000:
        tier = "🔥🔥 HIGH VOLUME"
        color = 0xFF6600
    else:
        tier = "🔥 ACTIVE TOKEN"
        color = 0xFFAA00

    fields = [
        {"name": "📋 CA", "value": f"`{addr}`", "inline": False},
        {"name": "💵 Price", "value": price_str, "inline": True},
        {"name": "📊 Jupiter Vol 24h", "value": _fmt_usd(daily_volume), "inline": True},
        {"name": "🎯 Confidence", "value": confidence, "inline": True},
    ]

    if tags_str != "None":
        fields.append({"name": "🏷️ Tags", "value": tags_str, "inline": False})

    if created_at:
        fields.append({"name": "📅 Listed", "value": created_at[:10], "inline": True})

    fields.append({
        "name": "🔗 Trade",
        "value": f"[Jupiter](https://jup.ag/swap/SOL-{addr})",
        "inline": True
    })

    embed = {
        "embeds": [{
            "title": f"🪐 {tier} — ${symbol}",
            "description": (
                f"**{name}**\n"
                f"🆕 Just entered Jupiter top 20 by volume"
            ),
            "color": color,
            "fields": fields,
            "footer": {"text": "Trench Scanner • Jupiter API"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_JUPITER, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[discord] Jupiter alert error: {resp.status} {text}")
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