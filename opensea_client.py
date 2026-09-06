# opensea_client.py
# Minimal OpenSea API v2 client for the NFT monitors.
# Free-tier key budget: 600 reads/hour shared across ALL endpoints, so every
# request goes through a global lock with a minimum interval (same pattern
# as geckoterminal_client). Verified live 2026-08-10:
# - /chains, /collections (order_by=created_date|one_day_volume|
#   one_day_change|seven_day_change), /collections/{slug},
#   /collections/{slug}/stats all work
# - unsupported order_by values return HTTP 400 with an explicit error
# - total_supply can be negative/garbage -> prefer unique_item_count
# - stats for brand-new collections are all zeros (no floor/owners yet)

import asyncio
import os
import time

import aiohttp

OPENSEA_BASE = "https://api.opensea.io/api/v2"

# 600 reads/hour = one request per 6s; 7s keeps headroom for retries.
_MIN_REQUEST_INTERVAL = 7.0
_lock = asyncio.Lock()
_last_request_ts = 0.0


def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


async def _get(
    session: aiohttp.ClientSession,
    path: str,
    params: dict | None = None,
) -> dict | None:
    """GET with global min-interval lock and a single 429 retry."""
    global _last_request_ts

    api_key = os.environ.get("OPENSEA_API_KEY", "")
    if not api_key:
        print("[opensea] OPENSEA_API_KEY not set — skipping request")
        return None

    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    url = f"{OPENSEA_BASE}{path}"

    async with _lock:
        wait = _MIN_REQUEST_INTERVAL - (time.monotonic() - _last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)

        for attempt in range(2):
            try:
                async with session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    _last_request_ts = time.monotonic()
                    if resp.status == 200:
                        return await resp.json(content_type=None)
                    if resp.status == 429 and attempt == 0:
                        retry_after = _to_float(resp.headers.get("Retry-After"), 15.0)
                        wait_s = min(max(retry_after, 10.0), 60.0)
                        print(f"[opensea] 429 for {path}, waiting {wait_s:.0f}s")
                        await asyncio.sleep(wait_s)
                        continue
                    print(f"[opensea] HTTP {resp.status} for {path}")
                    return None
            except asyncio.TimeoutError:
                print(f"[opensea] Timeout for {path}")
                return None
            except Exception as exc:
                print(f"[opensea] Error for {path}: {type(exc).__name__}: {exc}")
                return None
    return None


def _parse_collection(raw: dict) -> dict:
    contracts = raw.get("contracts") or []
    address = ""
    chain = ""
    if contracts:
        address = (contracts[0].get("address") or "").lower()
        chain = contracts[0].get("chain") or ""
    return {
        "slug": raw.get("collection") or "",
        "name": (raw.get("name") or "").strip(),
        "description": raw.get("description") or "",
        "image_url": raw.get("image_url"),
        "banner_image_url": raw.get("banner_image_url"),
        "address": address,
        "chain": chain,
        "safelist_status": raw.get("safelist_status") or "",
        "category": raw.get("category") or "",
        "is_disabled": bool(raw.get("is_disabled")),
        "is_nsfw": bool(raw.get("is_nsfw")),
        "opensea_url": raw.get("opensea_url") or "",
        "project_url": raw.get("project_url") or "",
        "twitter_username": raw.get("twitter_username") or "",
        "discord_url": raw.get("discord_url") or "",
        "telegram_url": raw.get("telegram_url") or "",
        "instagram_username": raw.get("instagram_username") or "",
        "wiki_url": raw.get("wiki_url") or "",
    }


async def fetch_chains(session: aiohttp.ClientSession) -> list[str]:
    """Return list of chain slugs supported by OpenSea."""
    data = await _get(session, "/chains")
    if not data:
        return []
    return [c.get("chain") for c in (data.get("chains") or []) if c.get("chain")]


async def fetch_collections(
    session: aiohttp.ClientSession,
    chain: str,
    order_by: str,
    limit: int = 50,
) -> list[dict] | None:
    """List collections on a chain. Returns None on request failure,
    [] on a genuinely empty result (dead chain).
    Verified order_by values: created_date, one_day_volume, market_cap,
    one_day_change, seven_day_change, seven_day_volume, thirty_day_volume."""
    data = await _get(
        session,
        "/collections",
        params={"chain": chain, "order_by": order_by, "limit": str(limit)},
    )
    if data is None:
        return None
    return [_parse_collection(c) for c in (data.get("collections") or [])]


async def fetch_collection_details(
    session: aiohttp.ClientSession, slug: str
) -> dict | None:
    raw = await _get(session, f"/collections/{slug}")
    if not raw:
        return None
    currency = ((raw.get("pricing_currencies") or {}).get("listing_currency") or {})
    royalty_pct = sum(
        _to_float(f.get("fee")) for f in (raw.get("fees") or [])
    )
    return {
        # total_supply is unreliable (negative values observed live);
        # unique_item_count is the trustworthy field.
        "item_count": int(_to_float(raw.get("unique_item_count"))),
        "created_date": raw.get("created_date") or "",
        "listing_currency": currency.get("symbol") or "",
        "royalty_pct": royalty_pct,
    }


async def fetch_collection_stats(
    session: aiohttp.ClientSession, slug: str
) -> dict | None:
    raw = await _get(session, f"/collections/{slug}/stats")
    if not raw:
        return None
    total = raw.get("total") or {}
    intervals = {i.get("interval"): i for i in (raw.get("intervals") or [])}
    one_day = intervals.get("one_day") or {}
    seven_day = intervals.get("seven_day") or {}
    return {
        "floor_price": _to_float(total.get("floor_price")),
        "floor_symbol": total.get("floor_price_symbol") or "",
        "num_owners": int(_to_float(total.get("num_owners"))),
        "total_volume": _to_float(total.get("volume")),
        "total_sales": int(_to_float(total.get("sales"))),
        "one_day_volume": _to_float(one_day.get("volume")),
        "one_day_sales": int(_to_float(one_day.get("sales"))),
        "seven_day_volume": _to_float(seven_day.get("volume")),
        "seven_day_sales": int(_to_float(seven_day.get("sales"))),
    }