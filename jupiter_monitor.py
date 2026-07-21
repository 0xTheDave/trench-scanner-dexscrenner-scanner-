# jupiter_monitor.py

import asyncio
import aiohttp
import os
import time
from datetime import datetime, timezone

import db
from discord_client import send_jupiter_alert
from haiku_client import get_or_classify_token_type
from geckoterminal_client import fetch_pool_only, fetch_ohlcv
from chart_renderer import render_chart_async

JUPITER_BASE = "https://lite-api.jup.ag"
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY", "")

# GeckoTerminal network id for Solana is the stable string "solana" — no
# resolve needed. Used for unique-traders enrichment + OHLCV chart.
GECKO_NETWORK = "solana"

# Two independent 1h leaderboards, both top 50:
# - toptraded: raw trading volume (can include wash trading)
# - toporganicscore: Jupiter's organic volume score (bot/wash filtered)
# A token can enter one or both — the alert shows which.
CATEGORIES = {
    "toptraded": f"{JUPITER_BASE}/tokens/v2/toptraded/1h?limit=50",
    "organic": f"{JUPITER_BASE}/tokens/v2/toporganicscore/1h?limit=50",
}

DEDUP_TTL = 6 * 3600                # 1h windows move fast — 6h dedup, not 24h
MIN_JUPITER_VOLUME = 50_000         # minimum 24h volume
MIN_JUPITER_VOLUME_1H = 10_000      # minimum 1h volume — a top-list entrant
                                    # with ~$5k/h is leaderboard churn, not a signal

# Rate-limit valve: OHLCV is the 2nd GeckoTerminal call per alert. To keep
# total gecko load well under the ~30/min ceiling, a chart is only rendered
# for stronger signals — every alert still gets the unique-traders (pool) call.
# "organic" entry is Jupiter's wash/bot-filtered list, a stronger signal by
# construction; a toptraded-only entrant needs high 1h volume to earn a chart.
# Lower CHART_MIN_VOLUME_1H if you want charts on more toptraded entrants.
# tune on data
CHART_MIN_VOLUME_1H = 150_000

alerted_jupiter: dict[str, float] = {}

# Per-category previous top set — alert only on NEW entrants per category
previous_top: dict[str, set[str]] = {name: set() for name in CATEGORIES}

# Baseline and dedup are persisted so a restart doesn't reset the
# baseline (which would suppress real entrants for one full cycle)
_state_loaded = False

# Majors, stables and liquid staking tokens rotate in/out of 1h
# leaderboards constantly — entering the list is not news for them.
EXCLUDED_SYMBOLS = (
    "USDC", "USDT", "USDS", "USD1", "PYUSD", "USDE", "USX",
    "SOL", "WSOL", "WETH", "BTC", "WBTC", "CBBTC", "JUP",
    "JITOSOL", "JUPSOL", "MSOL", "BSOL", "BNSOL", "INF", "JLP",
)


def _load_state():
    global _state_loaded
    saved_top = db.kv_get_json("jupiter_previous_top")
    if saved_top:
        for category in CATEGORIES:
            previous_top[category] = set(saved_top.get(category, []))
        restored = sum(len(s) for s in previous_top.values())
        print(f"[jupiter] Restored baseline from DB ({restored} entries)")

    saved_alerted = db.kv_get_json("jupiter_alerted")
    if saved_alerted:
        now = time.time()
        alerted_jupiter.update({
            k: v for k, v in saved_alerted.items() if now - v < DEDUP_TTL
        })

    _state_loaded = True


def _save_state():
    db.kv_set_json(
        "jupiter_previous_top",
        {category: sorted(addresses) for category, addresses in previous_top.items()},
    )
    db.kv_set_json("jupiter_alerted", alerted_jupiter)


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

    entry = data.get(address) or (data.get("data") or {}).get(address)
    return entry


def _extract_volume(token: dict) -> float:
    """
    Extract 24h volume from token stats — v2 schema nests
    buy/sell volume under stats24h.
    """
    stats = token.get("stats24h") or {}
    buy_vol = stats.get("buyVolume") or 0
    sell_vol = stats.get("sellVolume") or 0
    total = float(buy_vol) + float(sell_vol)

    if total > 0:
        return total

    # Fallbacks for other schema variants
    for field in ("daily_volume", "v24hUSD", "volume24h"):
        val = token.get(field)
        if val:
            try:
                return float(val)
            except (ValueError, TypeError):
                continue
    return 0.0


def _extract_volume_1h(token: dict) -> float:
    """Extract 1h volume if stats1h is present (0.0 when unavailable)."""
    stats = token.get("stats1h") or {}
    buy_vol = stats.get("buyVolume") or 0
    sell_vol = stats.get("sellVolume") or 0
    try:
        return float(buy_vol) + float(sell_vol)
    except (ValueError, TypeError):
        return 0.0


def _extract_address(token: dict) -> str:
    """v2 schema uses 'id' for mint address; older variants use 'address'."""
    return token.get("id") or token.get("address") or ""


def _extract_socials(token: dict) -> str | None:
    """Best-effort social links from the 'extensions' field, if present."""
    ext = token.get("extensions") or {}
    parts = []
    if ext.get("twitter"):
        parts.append(f"Twitter: {ext['twitter']}")
    if ext.get("website"):
        parts.append(f"Website: {ext['website']}")
    if ext.get("telegram"):
        parts.append(f"Telegram: {ext['telegram']}")
    return ", ".join(parts) if parts else None


async def scan_jupiter(session: aiohttp.ClientSession):
    """
    Scan two Jupiter 1h leaderboards (top 50 each).
    Alert only on NEW entrants; the alert tags which list(s) the token
    entered. First run per category only records the baseline —
    unless a baseline was restored from the database.
    """
    if not _state_loaded:
        _load_state()

    print(f"[jupiter] {datetime.now().strftime('%H:%M:%S')} — scanning Jupiter 1h leaderboards...")

    # addr -> {"token": dict, "entered": [category names]}
    new_entrants: dict[str, dict] = {}

    for category, url in CATEGORIES.items():
        data = await fetch_json(session, url)

        if not data:
            print(f"[jupiter] {category}: no data received")
            continue

        tokens = data if isinstance(data, list) else data.get("tokens", [])
        if not tokens:
            print(f"[jupiter] {category}: empty token list")
            continue

        current = {_extract_address(t): t for t in tokens if _extract_address(t)}
        current_set = set(current.keys())

        if len(previous_top[category]) == 0:
            print(f"[jupiter] {category}: first run — baseline set with {len(current_set)} tokens")
            previous_top[category] = current_set
            continue

        fresh = current_set - previous_top[category]
        print(f"[jupiter] {category}: {len(tokens)} tokens, new entrants: {len(fresh)}")

        for addr in fresh:
            if addr in new_entrants:
                new_entrants[addr]["entered"].append(category)
            else:
                new_entrants[addr] = {"token": current[addr], "entered": [category]}

        previous_top[category] = current_set

        await asyncio.sleep(0.5)

    alerts_sent = 0
    skipped_quiet = 0

    for addr, entry in new_entrants.items():
        token = entry["token"]
        entered = entry["entered"]

        if _is_deduped(addr):
            continue

        symbol = (token.get("symbol") or "").upper()
        if symbol in EXCLUDED_SYMBOLS:
            continue

        daily_volume = _extract_volume(token)
        if daily_volume < MIN_JUPITER_VOLUME:
            continue

        vol_1h = _extract_volume_1h(token)

        # Entrants with negligible hourly volume are leaderboard churn.
        # Only applied when stats1h is actually present (vol_1h > 0),
        # so schema variants without 1h stats are not silently dropped.
        if 0 < vol_1h < MIN_JUPITER_VOLUME_1H:
            skipped_quiet += 1
            continue

        await asyncio.sleep(0.5)
        price_data = await fetch_token_price(session, addr)

        # Inject entry-source labels into tags so the Discord embed
        # shows WHICH leaderboard(s) the token entered, without
        # changing discord_client. "organic" entry = stronger signal.
        source_tags = [f"🆕 {cat}-1h" for cat in entered]
        if vol_1h > 0:
            source_tags.append(f"vol1h ${vol_1h:,.0f}")
        original_tags = token.get("tags") or []
        token_for_alert = {
            **token,
            "address": addr,
            "tags": source_tags + list(original_tags),
        }

        print(
            f"[jupiter] Alert: {symbol} entered {'+'.join(entered)} | "
            f"vol24h=${daily_volume:,.0f} | vol1h=${vol_1h:,.0f} | "
            f"addr={addr[:8]}..."
        )

        classification = await get_or_classify_token_type(session, addr, {
            "symbol": symbol,
            "name": token.get("name") or symbol,
            "description": None,
            "socials": _extract_socials(token),
        })

        # GeckoTerminal enrichment: one pool call gives unique traders +
        # pool_address (fetched for EVERY alert). The OHLCV/chart call is the
        # rate-limit valve — only spent on stronger signals (organic entry, or
        # high 1h volume). Both are best-effort — any failure yields None and
        # the alert still goes out (unique traders 'n/a', no chart). Solana
        # network id is the stable string, so no resolve needed.
        enrichment = await fetch_pool_only(session, GECKO_NETWORK, addr)
        chart_png = None
        render_chart = ("organic" in entered) or (vol_1h >= CHART_MIN_VOLUME_1H)
        pool_addr = (enrichment or {}).get("pool_address")
        if pool_addr and render_chart:
            ohlcv = await fetch_ohlcv(session, GECKO_NETWORK, pool_addr)
            if ohlcv:
                chart_png = await render_chart_async(ohlcv, symbol, "5m")

        await send_jupiter_alert(session, {
            "token": token_for_alert,
            "price_data": price_data,
            "daily_volume": daily_volume,
            "classification": classification,
            "enrichment": enrichment,
            "chart_png": chart_png,
        })

        # Record for performance tracking (needs a numeric entry price)
        alert_price = 0.0
        if price_data:
            try:
                alert_price = float(price_data.get("usdPrice") or price_data.get("price") or 0)
            except (ValueError, TypeError):
                alert_price = 0.0
        db.record_alert(addr, symbol, "jupiter", alert_price)

        _mark_alerted(addr)
        alerts_sent += 1

    _save_state()
    _cleanup_dedup()
    print(f"[jupiter] Done. Alerts: {alerts_sent}, skipped quiet: {skipped_quiet}")