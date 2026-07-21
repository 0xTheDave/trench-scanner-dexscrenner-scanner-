# rugcheck_client.py

import asyncio
import aiohttp
import time

RUGCHECK_BASE = "https://api.rugcheck.xyz"

# NOTE: no API key on purpose. The public /v1/tokens/{mint}/report
# endpoint returns 200 without auth; sending the old FluxRPC key
# in X-API-KEY causes HTTP 401.

# Public endpoint allows ~1 req/s — enforced globally with an async lock
# so parallel scanner tasks can't burst past the limit.
_MIN_REQUEST_INTERVAL = 1.0
_rate_lock = asyncio.Lock()
_last_request_ts = 0.0


async def _acquire_rate_slot():
    """Wait until at least _MIN_REQUEST_INTERVAL passed since last request."""
    global _last_request_ts
    async with _rate_lock:
        now = time.monotonic()
        wait = _MIN_REQUEST_INTERVAL - (now - _last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_ts = time.monotonic()


async def fetch_rugcheck_report(
    session: aiohttp.ClientSession,
    mint: str,
) -> dict | None:
    """
    Fetch and parse a RugCheck token report.
    Returns normalized dict or None if unavailable.
    Parsed defensively — schema fields may vary per token.
    """
    url = f"{RUGCHECK_BASE}/v1/tokens/{mint}/report"

    for attempt in range(2):
        await _acquire_rate_slot()
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(2)
                    continue
                if resp.status != 200:
                    print(f"[rugcheck] HTTP {resp.status} for {mint[:8]}...")
                    return None
                data = await resp.json()
                return _parse_report(data)
        except asyncio.TimeoutError:
            print(f"[rugcheck] Timeout for {mint[:8]}...")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[rugcheck] Error: {e}")
            return None
    return None


def _parse_report(data: dict) -> dict:
    """
    Normalize RugCheck report into flat fields.
    All fields default to None when not present.
    Verified against live public endpoint responses (2026-07).
    """
    result = {
        "mint_authority_active": None,
        "freeze_authority_active": None,
        "lp_locked_pct": None,
        "top10_holders_pct": None,
        "risk_score": None,               # for display (normalised preferred)
        "risk_score_normalised": None,    # 0-100, higher = riskier; scoring uses ONLY this
        "rugged": None,
        "total_holders": None,
        "risks": [],
    }

    # Mint / freeze authority live under token object.
    # Keys are always present; value is null when authority is revoked.
    token = data.get("token") or {}
    if "mintAuthority" in token:
        result["mint_authority_active"] = token.get("mintAuthority") is not None
    if "freezeAuthority" in token:
        result["freeze_authority_active"] = token.get("freezeAuthority") is not None

    # Walk markets once: collect AMM liquidity token accounts (to exclude
    # from holder concentration) and find the MOST LIQUID market's
    # lpLockedPct. Taking max() across all markets was wrong — one tiny
    # fully-locked side pool would fake 100% while the main pool is open.
    markets = data.get("markets") or []
    liquidity_accounts: set[str] = set()
    best_market_usd = -1.0
    best_lp_pct = None
    fallback_pcts = []

    for market in markets:
        for key in ("liquidityAAccount", "liquidityBAccount"):
            acc = market.get(key)
            if acc:
                liquidity_accounts.add(str(acc))

        lp = market.get("lp") or {}
        pct = lp.get("lpLockedPct")
        if pct is None:
            continue

        try:
            pct = float(pct)
        except (ValueError, TypeError):
            continue

        fallback_pcts.append(pct)

        try:
            market_usd = float(lp.get("baseUSD") or 0) + float(lp.get("quoteUSD") or 0)
        except (ValueError, TypeError):
            market_usd = 0.0

        if market_usd > best_market_usd:
            best_market_usd = market_usd
            best_lp_pct = pct

    if best_lp_pct is not None:
        result["lp_locked_pct"] = best_lp_pct
    elif fallback_pcts:
        result["lp_locked_pct"] = max(fallback_pcts)

    # Top holders concentration — sum the first 10 REAL holders,
    # skipping AMM liquidity accounts so pool reserves don't inflate
    # the number on fresh tokens.
    top_holders = data.get("topHolders") or []
    if top_holders:
        total_pct = 0.0
        counted = 0
        for holder in top_holders:
            if counted >= 10:
                break
            addr = str(holder.get("address") or "")
            if addr and addr in liquidity_accounts:
                continue
            pct = holder.get("pct")
            if pct is not None:
                try:
                    total_pct += float(pct)
                    counted += 1
                except (ValueError, TypeError):
                    continue
        if counted > 0:
            result["top10_holders_pct"] = total_pct

    # Risk score — explicit None checks because score_normalised == 0
    # is a VALID (safest) value and must not fall through to raw score.
    # Raw score is a different, unbounded scale (BONK: raw=101, norm=7).
    score_norm = data.get("score_normalised")
    if score_norm is not None:
        result["risk_score_normalised"] = score_norm
        result["risk_score"] = score_norm
    elif data.get("score") is not None:
        result["risk_score"] = data.get("score")

    # Rugged flag — instant disqualifier, consumed by scoring
    if isinstance(data.get("rugged"), bool):
        result["rugged"] = data["rugged"]

    # Total holder count (informational for now)
    total_holders = data.get("totalHolders")
    if isinstance(total_holders, int):
        result["total_holders"] = total_holders

    # Risk flags list with severity level annotation
    for risk in (data.get("risks") or []):
        name = risk.get("name") or risk.get("description")
        level = risk.get("level")
        if name:
            result["risks"].append(f"{name} ({level})" if level else name)

    return result