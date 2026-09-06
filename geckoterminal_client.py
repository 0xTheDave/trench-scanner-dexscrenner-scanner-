# geckoterminal_client.py
# GeckoTerminal Public API client (Beta, no auth).
# Used to enrich alerts with data DexScreener doesn't expose in the same form:
#   - unique buyers/sellers (wallet-level, not raw txn counts)
#   - token logo (image_url)
#   - OHLCV candles for chart rendering
#   - trending / new pool discovery across chains (multichain_monitor)
#
# Also under evaluation as a possibly-better data source for Robinhood Chain
# (chain 4663), which GeckoTerminal indexes. Whether it actually beats
# DexScreener there is verified empirically by the __main__ diagnostic below,
# not assumed.
#
# Docs: https://apiguide.geckoterminal.com/  — RESTful JSON, versioned via
# the Accept header.
#
# RATE LIMIT — REVISED 2026-08-28. The client was built assuming 30 req/min
# (the limit stated in GeckoTerminal's 2024 changelog). That assumption was
# WRONG for the current free tier: the DEX API page now advertises paid plans
# as a 25x increase "from 10 calls/min to 250 calls/min", i.e. the free public
# ceiling is 10/min. This explains why chronic 429s survived the first fix:
# a 25/min sliding-window quota is 2.5x over the real limit, 3.0s spacing
# (20/min) is 2x over it, and even the post-429 cooldown spacing of 6.0s
# (10/min) sat exactly ON the limit with zero margin. Every layer was tuned
# against a ceiling that doesn't exist. All three constants are now set below
# 10/min, with the sliding window as the binding constraint.
#
# RATE LIMITING (two layers, both in _acquire_rate_slot):
#   1. Spacing: minimum _MIN_REQUEST_INTERVAL between consecutive requests
#      (widens to _COOLDOWN_INTERVAL for a window after any 429).
#   2. Sliding-window quota: at most _WINDOW_MAX_REQUESTS in any 60s window.
#      This is the hard ceiling; spacing alone caps steady-state throughput
#      but says nothing about a burst clustering inside one 60s window. Four
#      independent consumers share this limiter (jupiter enrichment, multichain
#      discovery, migration charts, robinhood charts), so bursts overlap by
#      default. A 429 counts toward the window too (the request was still
#      made), so its timestamp is recorded like any other.
#
# BUDGET REALITY at 9/min: ~540 calls/hour total across ALL consumers. That is
# tight enough that demand-side cuts (fewer enrich calls, slower multichain
# rotation, chart gating) are the next lever if starvation shows up in logs —
# see the [gecko] stats line, which reports waits and starvation directly.
#
# NOTE ON NUMERIC FIELDS: GeckoTerminal returns most numeric attributes
# (reserve_in_usd, base_token_price_usd, volume_usd.*, price_change_percentage.*)
# as STRINGS. Both parsers below coerce them to float at the source, so every
# consumer (multichain floors/embeds, jupiter liquidity instrumentation, chart
# paths) can rely on numbers. A raw string reaching an f-string ':f' format was
# crashing multichain mid-scan ("Unknown format code 'f' for object of type
# 'str'") and would have silently stored string liquidity in the DB.
#
# POOL SELECTION — REVISED 2026-09-05. See _rank_pools for the full rationale.
# Short version: ranking by reserve_in_usd is unsafe on Robinhood Chain, where
# that field intermittently returns NEGATIVE values. When it does, the candidate
# list empties and selection silently falls through to pools[0] — GeckoTerminal's
# own ordering by CURRENT state, which for an old alert points at today's live
# pool rather than the one that traded at alert time. Pools are now ranked by
# trading volume instead.

import asyncio
import aiohttp
import time
from collections import deque

API_BASE = "https://api.geckoterminal.com/api/v2"
# Pinning the API version per GeckoTerminal's own recommendation (Beta, subject
# to change). Update if they bump the version and responses shift.
ACCEPT_HEADER = {"Accept": "application/json;version=20230302"}

# Free-tier ceiling is 10/min (see header note). Spacing of 7.0s caps steady
# state at ~8.5/min, leaving headroom under 10 without relying on the window.
# After a 429 we widen to 12.0s (5/min) — genuinely below the limit, unlike the
# previous 6.0s cooldown which sat exactly on it and so never actually recovered.
_MIN_REQUEST_INTERVAL = 7.0
_COOLDOWN_INTERVAL = 12.0       # widened spacing (~5/min) while cooling down
_COOLDOWN_DURATION = 60.0       # how long the widened spacing lasts after a 429
_rate_lock = asyncio.Lock()
_last_request_ts = 0.0
_cooldown_until = 0.0

# Sliding-window quota (layer 2) — the hard ceiling. 9 per rolling 60s window
# keeps one slot of margin under the 10/min free limit, absorbing clock skew
# and the fact that the server's window boundaries don't align with ours.
_WINDOW_SECONDS = 60.0
_WINDOW_MAX_REQUESTS = 9
_request_times: deque[float] = deque()

# Observability. The previous fix could not be verified from logs because a
# quota wait was silent and indistinguishable from normal operation. These
# counters make the next verification decidable: if 429s stop but starvation
# climbs, the limiter is working and demand must be cut; if 429s persist even
# at 9/min, the limit is lower still (or IP-shared) and only demand cuts help.
_STATS_INTERVAL = 300.0
_stats_last_report = 0.0
_stat_requests = 0
_stat_quota_waits = 0
_stat_quota_wait_seconds = 0.0
_stat_429s = 0

# On a 429, wait this long (unless the server sends a *sane* Retry-After) then
# retry once. GeckoTerminal has been seen returning a near-zero Retry-After via
# Cloudflare; honoring that literally makes the retry fire instantly into the
# same 429 and just burns another slot — so we floor it. Floor raised in line
# with the corrected limit: a 3s retry re-entered the same exhausted window.
_RATE_LIMIT_BACKOFF_SECONDS = 12.0
_MIN_RATE_LIMIT_BACKOFF = 8.0

# Network-id cache: GeckoTerminal uses string ids ("eth", "solana", ...), not
# numeric chain ids. We resolve Robinhood's id once by scanning the networks
# list, since it's not guaranteed to be a predictable string.
_network_id_cache: dict[str, str] = {}


def _to_float(v):
    """
    Coerce a GeckoTerminal numeric-as-string value to float.
    Returns None when the value is absent or not a number — callers decide
    whether None means 'n/a' or falls back to 0.
    """
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _trip_cooldown():
    """Widen request spacing for the next _COOLDOWN_DURATION seconds."""
    global _cooldown_until
    _cooldown_until = time.monotonic() + _COOLDOWN_DURATION


def _evict_old(now: float):
    """Drop request timestamps that have aged out of the sliding window."""
    cutoff = now - _WINDOW_SECONDS
    while _request_times and _request_times[0] <= cutoff:
        _request_times.popleft()


def _maybe_report_stats(now: float):
    """
    Periodic limiter health line. Called from inside the rate lock, so the
    counters are consistent. Prints at most every _STATS_INTERVAL seconds.
    Reading it: 'quota waits' is how often a caller was blocked by the window
    (healthy — the limiter doing its job); 'avg wait' rising toward 60s means
    demand exceeds the budget and consumers are starving; '429s' should be 0.
    """
    global _stats_last_report
    if now - _stats_last_report < _STATS_INTERVAL:
        return
    _stats_last_report = now
    avg_wait = (
        _stat_quota_wait_seconds / _stat_quota_waits if _stat_quota_waits else 0.0
    )
    print(
        f"[gecko] stats: {_stat_requests} requests | "
        f"{_stat_quota_waits} quota waits (avg {avg_wait:.1f}s) | "
        f"{_stat_429s} rate-limited | window {len(_request_times)}/{_WINDOW_MAX_REQUESTS}"
    )


def note_rate_limited():
    """Record a 429 for the stats line. Called by _get on every 429 response."""
    global _stat_429s
    _stat_429s += 1


async def _acquire_rate_slot():
    """
    Global admission control so parallel callers can't blow the free limit.
    Two layers, both enforced here under the same lock:

      1. Spacing — at least `interval` seconds since the last request
         (interval widens to _COOLDOWN_INTERVAL during a post-429 cooldown).
      2. Sliding-window quota — no more than _WINDOW_MAX_REQUESTS in any
         trailing _WINDOW_SECONDS window. If the window is full, wait exactly
         until the oldest request ages out, then re-check.

    Sleeping happens while holding the lock, on purpose: that serialises all
    consumers behind one queue instead of letting them wake simultaneously and
    race into the same slot. Every admitted request — success OR 429 — is
    recorded, because a 429 still consumed a slot against the real limit.
    """
    global _last_request_ts, _stat_requests, _stat_quota_waits, _stat_quota_wait_seconds
    async with _rate_lock:
        while True:
            now = time.monotonic()

            # Layer 1: spacing (with cooldown widening).
            interval = (
                _COOLDOWN_INTERVAL
                if now < _cooldown_until
                else _MIN_REQUEST_INTERVAL
            )
            spacing_wait = interval - (now - _last_request_ts)

            # Layer 2: sliding-window quota.
            _evict_old(now)
            if len(_request_times) >= _WINDOW_MAX_REQUESTS:
                # Window full — must wait until the oldest request exits it.
                quota_wait = (_request_times[0] + _WINDOW_SECONDS) - now
            else:
                quota_wait = 0.0

            wait = max(spacing_wait, quota_wait)
            if wait > 0:
                if quota_wait > spacing_wait and quota_wait > 0:
                    _stat_quota_waits += 1
                    _stat_quota_wait_seconds += quota_wait
                await asyncio.sleep(wait)
                continue  # re-evaluate after sleeping (state may have shifted)

            # Admitted: record this request against both layers.
            now = time.monotonic()
            _last_request_ts = now
            _request_times.append(now)
            _stat_requests += 1
            _maybe_report_stats(now)
            return


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
                    note_rate_limited()
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


def _pool_volume(attrs: dict, key: str) -> float:
    """Volume for a window ('h1' or 'h24'). Missing/negative -> 0.0."""
    v = _to_float((attrs.get("volume_usd") or {}).get(key))
    if v is None or v < 0:
        return 0.0
    return v


def _rank_pools(pools: list) -> tuple[dict | None, dict]:
    """
    Rank a /pools response list and return (chosen_pool, selection_metadata).

    Order of preference:
      1. volume_usd.h1  — the window that matches "traded around the alert"
      2. volume_usd.h24 — for tokens too quiet to register an h1 figure.
                          Observed live: h1 absent on ALL 20 pools of a token
                          that still had h24 volume, so this is a real path,
                          not a theoretical one.
      3. reserve_in_usd — last resort; UNSAFE on Robinhood Chain, where this
                          field intermittently returns negative values. Kept
                          for other chains, where it behaves.
      4. pools[0]       — GeckoTerminal's own ordering; recorded as
                          'fallback_first' so its frequency is measurable
                          instead of invisible.

    WHY VOLUME. On Robinhood Chain a token typically carries one pool with real
    turnover plus a spray of high-fee trap pools (observed on $IN: 1 pool with
    volume, 19 without, fees up to 87%). Volume separates them cleanly (live
    pool ~645k USD vs trap pool ~43 USD, distributions do not overlap), it is
    available at alert time, and unlike a fee-based rule it is not tied to one
    DEX — the fee rule was a Uniswap artifact and would have silenced 88% of
    non-BaseToken alerts.

    WHY THE METADATA. The diagnostic that motivated this change (n=38) returned
    a 100% hit rate, but a follow-up check showed the rule was often choosing
    the ONLY pool with any volume rather than discriminating between several.
    A 100% score on a field of one candidate is a tautology, not evidence. The
    metadata records n_candidates and the margin over the runner-up so this
    resolves itself from production logs instead of costing another historical
    reconstruction. Treat the rule as provisional until those counters say
    otherwise.
    """
    meta = {
        "method": "none",
        "n_pools": len(pools or []),
        "n_candidates": 0,
        "top_volume_h1": None,
        "runner_up_volume_h1": None,
        "chosen_volume": None,
    }
    if not pools:
        return None, meta

    scored_h1, scored_h24, scored_reserve = [], [], []
    for p in pools:
        attrs = p.get("attributes") or {}
        v1 = _pool_volume(attrs, "h1")
        v24 = _pool_volume(attrs, "h24")
        reserve = _to_float(attrs.get("reserve_in_usd")) or 0.0
        if v1 > 0:
            scored_h1.append((v1, p))
        if v24 > 0:
            scored_h24.append((v24, p))
        if reserve > 0:
            scored_reserve.append((reserve, p))

    h1_sorted = sorted(scored_h1, key=lambda x: x[0], reverse=True)
    if h1_sorted:
        meta["top_volume_h1"] = h1_sorted[0][0]
        if len(h1_sorted) > 1:
            meta["runner_up_volume_h1"] = h1_sorted[1][0]

    for bucket, name in (
        (h1_sorted, "volume_h1"),
        (sorted(scored_h24, key=lambda x: x[0], reverse=True), "volume_h24"),
        (sorted(scored_reserve, key=lambda x: x[0], reverse=True), "reserve"),
    ):
        if bucket:
            meta["method"] = name
            meta["n_candidates"] = len(bucket)
            meta["chosen_volume"] = bucket[0][0]
            return bucket[0][1], meta

    meta["method"] = "fallback_first"
    return pools[0], meta


def _pick_best_pool(pools: list) -> dict | None:
    """
    Backwards-compatible wrapper: returns just the pool, discarding metadata.
    Prefer _rank_pools in new code so the selection metadata is preserved.
    """
    pool, _meta = _rank_pools(pools)
    return pool


def _parse_pool(pool: dict) -> dict | None:
    """
    Extract the fields we care about from a GeckoTerminal pool object.
    Defensive: every field defaults to None if absent (Beta API, schema drift).
    Numeric fields are coerced to float here — the API sends them as strings.
    This matters downstream: jupiter's _extract_liquidity stores reserve_usd in
    the DB for liquidity-band analysis, which needs real numbers, not strings.
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
        "pool_created_at": attrs.get("pool_created_at"),
        "price_usd": _to_float(attrs.get("base_token_price_usd")),
        "price_change_h1": _to_float(_f(attrs, "price_change_percentage", "h1")),
        "price_change_h24": _to_float(_f(attrs, "price_change_percentage", "h24")),
        "volume_h1": _to_float(_f(attrs, "volume_usd", "h1")),
        "volume_h24": _to_float(_f(attrs, "volume_usd", "h24")),
        "reserve_usd": _to_float(attrs.get("reserve_in_usd")),
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
    Includes 'pool_address' so callers can fetch OHLCV for a chart, and
    'pool_selection' so callers can persist/log how the pool was chosen.
    """
    pools_data = await _get(
        session, f"{API_BASE}/networks/{network}/tokens/{token_address}/pools"
    )
    if not pools_data or not pools_data.get("data"):
        return None
    best, meta = _rank_pools(pools_data["data"])
    if not best:
        return None
    fields = _parse_pool(best)
    if fields is None:
        return None
    fields["pool_selection"] = meta
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

    # Pools for this token (ranked by volume)
    pools_data = await _get(
        session, f"{API_BASE}/networks/{network}/tokens/{token_address}/pools"
    )
    pool_fields = None
    if pools_data and pools_data.get("data"):
        best, meta = _rank_pools(pools_data["data"])
        if best:
            pool_fields = _parse_pool(best)
            if pool_fields is not None:
                pool_fields["pool_selection"] = meta

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
    before_timestamp: int | None = None,
) -> list | None:
    """
    Fetch OHLCV candles for a pool. Returns the raw ohlcv_list
    ([[ts, o, h, l, c, v], ...], newest-first) or None on failure / no data.
    Defaults to 5-minute candles, last 100. One HTTP call.
    The chart renderer decides whether there are enough candles to draw.

    before_timestamp bounds the window from above — needed by
    performance_tracker to measure a PINNED pool at a past timestamp rather
    than reading its current state.

    None means "this request returned no candles" and nothing more. It is NOT a
    fact about the token, and log lines must not describe it as one.
    """
    url = (
        f"{API_BASE}/networks/{network}/pools/{pool_address}"
        f"/ohlcv/{timeframe}?aggregate={aggregate}&limit={limit}&currency=usd"
    )
    if before_timestamp is not None:
        url += f"&before_timestamp={int(before_timestamp)}"
    data = await _get(session, url)
    if not data:
        return None
    ohlcv_list = ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list")
    return ohlcv_list or None


def format_unique_traders(enrichment: dict | None) -> str:
    """Short label for unique buyers/sellers. 'n/a' when data missing."""
    if not enrichment:
        return "n/a"
    b = enrichment.get("buyers_h1")
    s = enrichment.get("sellers_h1")
    if b is None and s is None:
        return "n/a"
    return f"{b if b is not None else '?'} buyers / {s if s is not None else '?'} sellers (1h)"


def format_pool_selection(enrichment: dict | None) -> str:
    """
    One-line summary of how the pool was chosen, for logs.
    'margin=sole' means only one pool had volume — the rule did not actually
    choose between candidates. Watch how often that appears before treating
    volume ranking as validated.
    """
    meta = (enrichment or {}).get("pool_selection") or {}
    if not meta:
        return "pool=n/a"
    top = meta.get("top_volume_h1")
    runner = meta.get("runner_up_volume_h1")
    if not runner:
        margin = "sole"
    elif top:
        margin = f"{top / runner:.1f}x"
    else:
        margin = "?"
    return (
        f"pool_by={meta.get('method')} "
        f"cand={meta.get('n_candidates')}/{meta.get('n_pools')} "
        f"margin={margin}"
    )


# ---------------------------------------------------------------------------
# Multi-chain pool discovery (trending + new pools) for multichain_monitor.
#
# Different response SHAPE from the single-token /pools path above, so it needs
# its own parser (_parse_discovery_pool), NOT _parse_pool:
#   - base/quote token symbol+address are NOT in the pool's own attributes.
#     They live in the top-level "included" array and must be requested via
#     ?include=base_token,quote_token,dex (verified against GeckoTerminal API
#     docs: attributes named in `include` are returned under the top-level
#     `included` key). Each pool's `relationships` holds the ids that index
#     into `included`.
#   - multichain_monitor needs BOTH sides of the pair (base_address/base_symbol,
#     quote_address/quote_symbol) so _select_interesting can pick the non-boring
#     side. _parse_pool never exposed those — hence this separate parser that
#     returns the FLAT dict shape multichain_monitor._passes/_select_interesting
#     /_send_alert read from.
# Endpoints: /networks/{network}/trending_pools and /new_pools, up to 20 pools.
# ---------------------------------------------------------------------------

def _index_included(included: list) -> tuple[dict, dict]:
    """
    Build {id: attributes} lookup maps from the response's top-level `included`
    array, split by resource type. Returns (tokens_by_id, dexes_by_id).
    `included` mixes types ("token", "dex"); each entry is keyed by its own id
    (e.g. "base_0x...", the same id the pool's relationships reference).
    """
    tokens_by_id: dict[str, dict] = {}
    dexes_by_id: dict[str, dict] = {}
    for item in included or []:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        item_type = item.get("type")
        attrs = item.get("attributes") or {}
        if not item_id:
            continue
        if item_type == "token":
            tokens_by_id[item_id] = attrs
        elif item_type == "dex":
            dexes_by_id[item_id] = attrs
    return tokens_by_id, dexes_by_id


def _related_id(pool: dict, rel_name: str) -> str | None:
    """Pull a related resource's id from a pool's relationships block."""
    rel = ((pool.get("relationships") or {}).get(rel_name) or {}).get("data") or {}
    return rel.get("id")


def _parse_discovery_pool(
    pool: dict,
    tokens_by_id: dict,
    dexes_by_id: dict,
    network: str,
) -> dict | None:
    """
    Parse ONE pool from a trending_pools / new_pools response into the flat dict
    shape multichain_monitor expects. Resolves base/quote token symbol+address
    from the `included` maps via the pool's relationships. Every field defaults
    to None/0 defensively (Beta API, schema drift). Returns None if the pool has
    no usable base token id at all.
    ALL numeric fields are coerced via _to_float — the API sends them as
    strings, and a raw string reaching _fmt_pct's ':+.1f' was killing the
    Base/Stable scans mid-pass.
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

    base_id = _related_id(pool, "base_token")
    quote_id = _related_id(pool, "quote_token")
    dex_id_ref = _related_id(pool, "dex")

    base_attrs = tokens_by_id.get(base_id or "", {})
    quote_attrs = tokens_by_id.get(quote_id or "", {})
    dex_attrs = dexes_by_id.get(dex_id_ref or "", {})

    txns_h1 = _f(attrs, "transactions", "h1") or {}
    txns_h24 = _f(attrs, "transactions", "h24") or {}

    return {
        "network": network,
        "pool_address": attrs.get("address"),
        "name": attrs.get("name"),
        "dex_id": dex_attrs.get("name") or dex_id_ref,
        # Both sides of the pair — the whole point of this parser.
        "base_address": base_attrs.get("address"),
        "base_symbol": base_attrs.get("symbol"),
        "quote_address": quote_attrs.get("address"),
        "quote_symbol": quote_attrs.get("symbol"),
        # Market data (base_token_price_usd is the pool's own attribute).
        "price_usd": _to_float(attrs.get("base_token_price_usd")),
        "price_change_h1": _to_float(_f(attrs, "price_change_percentage", "h1")),
        "price_change_h24": _to_float(_f(attrs, "price_change_percentage", "h24")),
        "volume_h24": _to_float(_f(attrs, "volume_usd", "h24")) or 0,
        "reserve_usd": _to_float(attrs.get("reserve_in_usd")) or 0,
        "market_cap_usd": _to_float(attrs.get("market_cap_usd")),
        "fdv_usd": _to_float(attrs.get("fdv_usd")),
        # Unique wallets — the genuinely-additive discovery signal.
        "buyers_h1": txns_h1.get("buyers"),
        "sellers_h1": txns_h1.get("sellers"),
        "buyers_h24": txns_h24.get("buyers"),
        "sellers_h24": txns_h24.get("sellers"),
    }


async def _fetch_discovery(
    session: aiohttp.ClientSession,
    network: str,
    endpoint: str,
) -> list[dict]:
    """
    Shared fetch+parse for trending_pools / new_pools. Requests base_token,
    quote_token and dex under `included` so the parser can resolve pair sides.
    Returns a list of flat pool dicts (possibly empty); never raises.
    """
    url = (
        f"{API_BASE}/networks/{network}/{endpoint}"
        f"?include=base_token,quote_token,dex"
    )
    data = await _get(session, url)
    if not data or not data.get("data"):
        return []
    tokens_by_id, dexes_by_id = _index_included(data.get("included") or [])
    out: list[dict] = []
    for pool in data["data"]:
        parsed = _parse_discovery_pool(pool, tokens_by_id, dexes_by_id, network)
        if parsed and parsed.get("base_address"):
            out.append(parsed)
    return out


async def fetch_trending_pools(
    session: aiohttp.ClientSession,
    network: str,
) -> list[dict]:
    """
    Trending pools on a network (GeckoTerminal's own trending ranking, web
    visits + on-chain activity). One HTTP call, up to 20 pools, flat dicts.
    Returns [] on any failure — multichain_monitor treats [] as "quiet chain".
    """
    return await _fetch_discovery(session, network, "trending_pools")


async def fetch_new_pools(
    session: aiohttp.ClientSession,
    network: str,
) -> list[dict]:
    """
    Newest pools on a network (freshly created). One HTTP call, up to 20 pools,
    flat dicts. Returns [] on any failure.
    """
    return await _fetch_discovery(session, network, "new_pools")


# ---------------------------------------------------------------------------
# Diagnostic: run `python geckoterminal_client.py` to see live what
# GeckoTerminal returns for a real Robinhood-chain token, plus the full pool
# ranking so the selection can be eyeballed against GeckoTerminal's own page.
# Test token: $IN (INSIDERS.BOT) from a live robinhood-gems alert.
# ---------------------------------------------------------------------------
async def _diagnostic():
    test_token = "0x6F572E8020247324D7B9dc15c297a32e4187dF1C"  # $IN on Robinhood Chain

    async with aiohttp.ClientSession() as session:
        print("=" * 64)
        print("GeckoTerminal diagnostic — Robinhood Chain coverage check")
        print("=" * 64)

        net_id = await resolve_network_id(session, "robinhood")
        print(f"\nResolved Robinhood network id: {net_id!r}")
        if not net_id:
            print("-> GeckoTerminal does NOT expose a Robinhood network. "
                  "Stick with DexScreener for that chain.")
            return

        pools_data = await _get(
            session, f"{API_BASE}/networks/{net_id}/tokens/{test_token}/pools")
        if not pools_data or not pools_data.get("data"):
            print("-> No pools returned. GeckoTerminal may not index this token yet.")
            return

        print(f"\nPools returned: {len(pools_data['data'])}")
        print(f"{'name':<28} {'vol h1':>12} {'vol h24':>12} {'reserve':>12}")
        for p in pools_data["data"]:
            a = p.get("attributes") or {}
            print(f"{(a.get('name') or '?')[:28]:<28} "
                  f"{_pool_volume(a, 'h1'):>12,.0f} "
                  f"{_pool_volume(a, 'h24'):>12,.0f} "
                  f"{(_to_float(a.get('reserve_in_usd')) or 0):>12,.0f}")

        chosen, meta = _rank_pools(pools_data["data"])
        print(f"\nChosen: {(chosen.get('attributes') or {}).get('name')}")
        print(f"Selection metadata: {meta}")

        enr = await fetch_pool_only(session, net_id, test_token)
        print(f"\n{format_pool_selection(enr)}")
        print(f"Unique traders: {format_unique_traders(enr)}")

        pool_addr = (enr or {}).get("pool_address")
        if pool_addr:
            print(f"\nFetching OHLCV for pool {pool_addr[:10]}...")
            ohlcv = await fetch_ohlcv(session, net_id, pool_addr)
            print(f"  OHLCV candles returned: {len(ohlcv) if ohlcv else 0}")


if __name__ == "__main__":
    asyncio.run(_diagnostic())