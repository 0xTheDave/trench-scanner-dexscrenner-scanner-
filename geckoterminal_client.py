# geckoterminal_client.py
# GeckoTerminal Public API client (Beta, no auth, 30 req/min universal limit).
# Used to enrich alerts with data DexScreener doesn't expose in the same form:
#   - unique buyers/sellers (wallet-level, not raw txn counts)
#   - token logo (image_url)
#   - OHLCV candles for chart rendering
#
# Also powers the multi-chain discovery monitor via trending_pools / new_pools,
# which return buyers/sellers + reserve in the SAME response — so discovery on a
# new chain costs one call per (chain, endpoint), no per-token enrichment call.
#
# Docs: https://apiguide.geckoterminal.com/  — RESTful JSON, versioned via
# the Accept header. Rate limit 30/min, enforced globally here.

import asyncio
import aiohttp
import time

API_BASE = "https://api.geckoterminal.com/api/v2"
# Pinning the API version per GeckoTerminal's own recommendation (Beta, subject
# to change). Update if they bump the version and responses shift.
ACCEPT_HEADER = {"Accept": "application/json;version=20230302"}

# 30 req/min universal free limit, but the docs note the effective limit
# "varies depending on the traffic size" — so the real ceiling can dip below
# 30. Normal spacing is 3.0s (20/min): observed 429s cluster in the first batch
# after a restart and when jupiter + robinhood fire together, so the extra
# margin below 30 is spent buying headroom for those bursts, not steady state.
# After any 429 we widen to a longer cooldown window (see _trip_cooldown), since
# every request — INCLUDING the 429 itself — counts toward the minute budget.
_MIN_REQUEST_INTERVAL = 3.0
_COOLDOWN_INTERVAL = 6.0        # widened spacing (~10/min) while cooling down
_COOLDOWN_DURATION = 45.0       # how long the widened spacing lasts after a 429
_rate_lock = asyncio.Lock()
_last_request_ts = 0.0
_cooldown_until = 0.0

# On a 429, wait this long (unless the server sends a *sane* Retry-After) then
# retry once. GeckoTerminal has been seen returning a near-zero Retry-After via
# Cloudflare; honoring that literally makes the retry fire instantly into the
# same 429 and just burns another slot — so we floor it.
_RATE_LIMIT_BACKOFF_SECONDS = 5.0
_MIN_RATE_LIMIT_BACKOFF = 3.0

# Network-id cache: GeckoTerminal uses string ids ("eth", "solana", ...), not
# numeric chain ids. We resolve Robinhood's id once by scanning the networks
# list, since it's not guaranteed to be a predictable string.
_network_id_cache: dict[str, str] = {}


def _trip_cooldown():
    """Widen request spacing for the next _COOLDOWN_DURATION seconds."""
    global _cooldown_until
    _cooldown_until = time.monotonic() + _COOLDOWN_DURATION


async def _acquire_rate_slot():
    """
    Global spacing so parallel callers can't blow the ~30/min limit.
    Spacing widens automatically for a short cooldown window after any 429,
    because the effective limit drops under load and a 429 means we're already
    over — packing more requests in at the normal rate just prolongs the storm.
    """
    global _last_request_ts
    async with _rate_lock:
        interval = (
            _COOLDOWN_INTERVAL
            if time.monotonic() < _cooldown_until
            else _MIN_REQUEST_INTERVAL
        )
        now = time.monotonic()
        wait = interval - (now - _last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_ts = time.monotonic()


async def _get(session: aiohttp.ClientSession, url: str) -> dict | None:
    """
    GET a GeckoTerminal endpoint. Returns parsed JSON dict or None on failure.
    On a 429, trips the global cooldown (widening spacing for everyone) and
    does ONE real backoff+retry — honoring Retry-After only if it's >= the
    floor, otherwise using the default. A sub-floor Retry-After is ignored on
    purpose: retrying instantly just wastes another slot and 429s again.
    """
    for attempt in range(2):  # initial try + one retry after backoff
        await _acquire_rate_slot()
        try:
            async with session.get(
                url,
                headers=ACCEPT_HEADER,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 429:
                    _trip_cooldown()
                    if attempt == 0:
                        retry_after = resp.headers.get("Retry-After")
                        delay = _RATE_LIMIT_BACKOFF_SECONDS
                        if retry_after:
                            try:
                                delay = max(float(retry_after), _MIN_RATE_LIMIT_BACKOFF)
                            except (ValueError, TypeError):
                                delay = _RATE_LIMIT_BACKOFF_SECONDS
                        print(f"[gecko] 429 — cooldown engaged, backing off {delay:.1f}s then retrying once")
                        await asyncio.sleep(delay)
                        continue
                    print("[gecko] 429 again after retry — giving up (n/a)")
                    return None
                if resp.status != 200:
                    print(f"[gecko] HTTP {resp.status} for {url}")
                    return None
                return await resp.json()
        except asyncio.TimeoutError:
            print(f"[gecko] Timeout for {url}")
            return None
        except Exception as e:
            print(f"[gecko] Error: {e}")
            return None
    return None


async def resolve_network_id(
    session: aiohttp.ClientSession,
    name_contains: str,
) -> str | None:
    """
    Find a GeckoTerminal network id by matching a substring of its display name
    (case-insensitive). E.g. name_contains='robinhood' -> the id GeckoTerminal
    uses for Robinhood Chain. Result cached. Paginates a few pages defensively.
    """
    key = name_contains.lower()
    if key in _network_id_cache:
        return _network_id_cache[key]

    for page in range(1, 6):  # networks list is paginated; cap the scan
        data = await _get(session, f"{API_BASE}/networks?page={page}")
        if not data or not data.get("data"):
            break
        for net in data["data"]:
            attrs = net.get("attributes") or {}
            name = (attrs.get("name") or "").lower()
            if key in name:
                net_id = net.get("id")
                if net_id:
                    _network_id_cache[key] = net_id
                    return net_id

    print(f"[gecko] No network id found matching '{name_contains}'")
    return None


def _pick_best_pool(pools: list) -> dict | None:
    """Pick the most liquid pool from a /pools response list."""
    valid = []
    for p in pools:
        attrs = p.get("attributes") or {}
        try:
            reserve = float(attrs.get("reserve_in_usd") or 0)
        except (ValueError, TypeError):
            reserve = 0.0
        if reserve > 0:
            valid.append((reserve, p))
    if not valid:
        return pools[0] if pools else None
    return max(valid, key=lambda x: x[0])[1]


def _parse_pool(pool: dict) -> dict | None:
    """
    Extract the fields we care about from a GeckoTerminal pool object.
    Defensive: every field defaults to None if absent (Beta API, schema drift).
    """
    if not isinstance(pool, dict):
        return None
    attrs = pool.get("attributes") or {}

    def _f(d: dict, *path):
        cur = d
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    txns_h1 = _f(attrs, "transactions", "h1") or {}
    txns_h24 = _f(attrs, "transactions", "h24") or {}

    return {
        "pool_address": attrs.get("address"),
        "name": attrs.get("name"),
        "price_usd": attrs.get("base_token_price_usd"),
        "price_change_h1": _f(attrs, "price_change_percentage", "h1"),
        "price_change_h24": _f(attrs, "price_change_percentage", "h24"),
        "volume_h24": _f(attrs, "volume_usd", "h24"),
        "reserve_usd": attrs.get("reserve_in_usd"),
        # The genuinely-additive bit vs DexScreener: unique wallets, not txn counts.
        "buyers_h1": txns_h1.get("buyers"),
        "sellers_h1": txns_h1.get("sellers"),
        "buyers_h24": txns_h24.get("buyers"),
        "sellers_h24": txns_h24.get("sellers"),
        "buys_h1": txns_h1.get("buys"),
        "sells_h1": txns_h1.get("sells"),
    }


async def fetch_pool_only(
    session: aiohttp.ClientSession,
    network: str,
    token_address: str,
) -> dict | None:
    """
    Lightweight enrichment: fetch ONLY the token's best pool (unique
    buyers/sellers etc.), skipping the separate token/logo call. Used for
    Robinhood Chain, where image_url is reliably None so the logo call would
    just waste half the rate-limit budget. One HTTP call per token.
    Returns None on any failure -> caller shows 'n/a'.
    Includes 'pool_address' so callers can fetch OHLCV for a chart.
    """
    pools_data = await _get(
        session, f"{API_BASE}/networks/{network}/tokens/{token_address}/pools"
    )
    if not pools_data or not pools_data.get("data"):
        return None
    best = _pick_best_pool(pools_data["data"])
    if not best:
        return None
    fields = _parse_pool(best)
    if fields is None:
        return None
    fields["logo_url"] = None  # not fetched on this path
    return fields


async def fetch_token_enrichment(
    session: aiohttp.ClientSession,
    network: str,
    token_address: str,
) -> dict | None:
    """
    Full enrichment: best pool's stats (incl. unique buyers/sellers) PLUS the
    token logo. Two HTTP calls. Use on chains where image_url is actually
    populated (e.g. Solana); for Robinhood prefer fetch_pool_only. Returns None
    on total failure — callers treat None as "no enrichment", never a block.
    """
    # Token info (for logo/image_url)
    logo_url = None
    token_data = await _get(
        session, f"{API_BASE}/networks/{network}/tokens/{token_address}"
    )
    if token_data:
        attrs = (token_data.get("data") or {}).get("attributes") or {}
        logo_url = attrs.get("image_url")

    # Pools for this token (pick most liquid)
    pools_data = await _get(
        session, f"{API_BASE}/networks/{network}/tokens/{token_address}/pools"
    )
    pool_fields = None
    if pools_data and pools_data.get("data"):
        best = _pick_best_pool(pools_data["data"])
        if best:
            pool_fields = _parse_pool(best)

    if pool_fields is None and logo_url is None:
        return None

    result = pool_fields or {}
    result["logo_url"] = logo_url
    return result


async def fetch_ohlcv(
    session: aiohttp.ClientSession,
    network: str,
    pool_address: str,
    timeframe: str = "minute",
    aggregate: int = 5,
    limit: int = 100,
) -> list | None:
    """
    Fetch OHLCV candles for a pool. Returns the raw ohlcv_list
    ([[ts, o, h, l, c, v], ...], newest-first) or None on failure / no data.
    Defaults to 5-minute candles, last 100. One HTTP call.
    The chart renderer decides whether there are enough candles to draw.
    """
    url = (
        f"{API_BASE}/networks/{network}/pools/{pool_address}"
        f"/ohlcv/{timeframe}?aggregate={aggregate}&limit={limit}&currency=usd"
    )
    data = await _get(session, url)
    if not data:
        return None
    ohlcv_list = ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list")
    return ohlcv_list or None


# ---------------------------------------------------------------------------
# Multi-chain discovery: trending_pools / new_pools.
#
# Both endpoints return a LIST of pools, each already carrying reserve, volume,
# price change AND unique buyers/sellers — the same enrichment fetch_pool_only
# makes a separate call for. So one call here = full data for ~20 pools.
#
# The base/quote token addresses live in relationships (not attributes), as ids
# shaped "{network}_{address}". Network ids never contain "_" (they use "-",
# e.g. "sui-network"), so splitting on the first "_" is safe for EVM chains.
# ---------------------------------------------------------------------------

def _extract_token_address(token_id: str | None) -> str | None:
    """
    Pull the raw token address out of a relationships id like
    'stable_0xeaf7...' -> '0xeaf7...'. Returns None if malformed.
    """
    if not token_id or "_" not in token_id:
        return None
    return token_id.split("_", 1)[1]


def _parse_pool_name_symbols(name: str | None) -> tuple[str | None, str | None]:
    """
    Best-effort symbol extraction from a pool display name like
    'FEFER / USDT0 1%' -> ('FEFER', 'USDT0'). The fee suffix (' 1%', ' 0.3%')
    is stripped from the quote side. Returns (None, None) if unparseable.
    """
    if not name or "/" not in name:
        return None, None
    left, _, right = name.partition("/")
    base_sym = left.strip() or None
    # Quote side may carry a trailing fee tier token like "USDT0 1%".
    quote_sym = right.strip().split(" ")[0] or None if right.strip() else None
    return base_sym, quote_sym


def _parse_discovery_pool(pool: dict, network: str) -> dict | None:
    """
    Parse one pool object from trending_pools / new_pools into a flat dict.
    Includes BOTH sides of the pair (address + symbol) so the caller can decide
    which token is the 'interesting' one (the meme), since the meme is sometimes
    the quote token, not the base (e.g. 'WgUSDT / FEFER').
    Every field defaults to None on absence — Beta API, schema drift tolerated.
    """
    if not isinstance(pool, dict):
        return None
    attrs = pool.get("attributes") or {}
    rels = pool.get("relationships") or {}

    def _f(d: dict, *path):
        cur = d
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    base_id = _f(rels, "base_token", "data", "id")
    quote_id = _f(rels, "quote_token", "data", "id")
    dex_id = _f(rels, "dex", "data", "id")

    base_addr = _extract_token_address(base_id)
    quote_addr = _extract_token_address(quote_id)
    base_sym, quote_sym = _parse_pool_name_symbols(attrs.get("name"))

    txns_h1 = _f(attrs, "transactions", "h1") or {}
    txns_h24 = _f(attrs, "transactions", "h24") or {}

    def _num(v):
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    return {
        "network": network,
        "pool_address": attrs.get("address"),
        "name": attrs.get("name"),
        "dex_id": dex_id,
        "base_address": base_addr,
        "base_symbol": base_sym,
        "quote_address": quote_addr,
        "quote_symbol": quote_sym,
        "price_usd": _num(attrs.get("base_token_price_usd")),
        "fdv_usd": _num(attrs.get("fdv_usd")),
        "market_cap_usd": _num(attrs.get("market_cap_usd")),
        "reserve_usd": _num(attrs.get("reserve_in_usd")),
        "volume_h1": _num(_f(attrs, "volume_usd", "h1")),
        "volume_h6": _num(_f(attrs, "volume_usd", "h6")),
        "volume_h24": _num(_f(attrs, "volume_usd", "h24")),
        "price_change_h1": _num(_f(attrs, "price_change_percentage", "h1")),
        "price_change_h6": _num(_f(attrs, "price_change_percentage", "h6")),
        "price_change_h24": _num(_f(attrs, "price_change_percentage", "h24")),
        "buys_h24": txns_h24.get("buys"),
        "sells_h24": txns_h24.get("sells"),
        "buyers_h1": txns_h1.get("buyers"),
        "sellers_h1": txns_h1.get("sellers"),
        "buyers_h24": txns_h24.get("buyers"),
        "sellers_h24": txns_h24.get("sellers"),
        "pool_created_at": attrs.get("pool_created_at"),
    }


async def fetch_trending_pools(
    session: aiohttp.ClientSession,
    network: str,
) -> list[dict]:
    """
    Fetch trending pools for a network (what's moving NOW — the 'noise' signal).
    Returns a list of parsed discovery-pool dicts (may be empty). One HTTP call.
    Never raises: any failure yields [].
    """
    data = await _get(session, f"{API_BASE}/networks/{network}/trending_pools")
    if not data or not data.get("data"):
        return []
    out = []
    for pool in data["data"]:
        parsed = _parse_discovery_pool(pool, network)
        if parsed:
            out.append(parsed)
    return out


async def fetch_new_pools(
    session: aiohttp.ClientSession,
    network: str,
) -> list[dict]:
    """
    Fetch newly-created pools for a network (fresh launches). Returns a list of
    parsed discovery-pool dicts (may be empty). One HTTP call. Never raises.
    """
    data = await _get(session, f"{API_BASE}/networks/{network}/new_pools")
    if not data or not data.get("data"):
        return []
    out = []
    for pool in data["data"]:
        parsed = _parse_discovery_pool(pool, network)
        if parsed:
            out.append(parsed)
    return out


def format_unique_traders(enrichment: dict | None) -> str:
    """Short label for unique buyers/sellers. 'n/a' when data missing."""
    if not enrichment:
        return "n/a"
    b = enrichment.get("buyers_h1")
    s = enrichment.get("sellers_h1")
    if b is None and s is None:
        return "n/a"
    return f"{b if b is not None else '?'} buyers / {s if s is not None else '?'} sellers (1h)"


# ---------------------------------------------------------------------------
# Diagnostic: run `python geckoterminal_client.py` to see live what
# GeckoTerminal returns for a real Robinhood-chain token, so we can compare
# against DexScreener before deciding how deeply to integrate.
# Test token: $IN (INSIDERS.BOT) from a live robinhood-gems alert.
# ---------------------------------------------------------------------------
async def _diagnostic():
    test_token = "0x6F572E8020247324D7B9dc15c297a32e4187dF1C"  # $IN on Robinhood Chain

    async with aiohttp.ClientSession() as session:
        print("=" * 60)
        print("GeckoTerminal diagnostic — Robinhood Chain coverage check")
        print("=" * 60)

        net_id = await resolve_network_id(session, "robinhood")
        print(f"\nResolved Robinhood network id: {net_id!r}")
        if not net_id:
            print("-> GeckoTerminal does NOT expose a Robinhood network. "
                  "Stick with DexScreener for that chain.")
            return

        print(f"\nFetching pool-only enrichment for $IN ({test_token[:10]}...) on '{net_id}'...")
        enr = await fetch_pool_only(session, net_id, test_token)

        if not enr:
            print("-> No data returned. GeckoTerminal may not index this token yet.")
            return

        print("\n--- What GeckoTerminal returned ---")
        for k, v in enr.items():
            print(f"  {k}: {v}")

        print("\n--- The additive fields vs DexScreener ---")
        print(f"  Unique traders (1h): {format_unique_traders(enr)}")

        pool_addr = enr.get("pool_address")
        if pool_addr:
            print(f"\nFetching OHLCV for pool {pool_addr[:10]}...")
            ohlcv = await fetch_ohlcv(session, net_id, pool_addr)
            print(f"  OHLCV candles returned: {len(ohlcv) if ohlcv else 0}")


if __name__ == "__main__":
    asyncio.run(_diagnostic())