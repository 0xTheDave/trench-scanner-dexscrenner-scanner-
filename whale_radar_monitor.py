# whale_radar_monitor.py
# Polls top Hyperliquid whales' open positions and alerts on LIQUIDATION
# CLUSTERS — price zones where enough leveraged notional sits near its
# liquidation price to act as a magnet. Longs clustered below current price =
# downside flush setup; shorts clustered above = squeeze-up setup.
# All data from Hyperliquid's public API — no key, no auth.

import asyncio
import time
import aiohttp

import db
from discord_client import send_radar_alert

HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_LEADERBOARD = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

# The leaderboard lives on a SEPARATE static host (stats-data.hyperliquid.xyz),
# not the api.hyperliquid.xyz JSON-RPC endpoint. It serves a pre-generated JSON
# file and only answers GET. A POST to a static object returns HTTP 403
# (classic S3/CloudFront AccessDenied), which is what we were hitting — NOT a
# User-Agent block. Verified live: GET returns 200 (even with a bot UA), POST
# returns 403. Browser headers are kept as a harmless safeguard in case a WAF
# is ever added in front of the CDN, but they are not what fixed the 403.
# Schema unchanged: leaderboardRows[].ethAddress / accountValue /
# windowPerformances.
_LEADERBOARD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://app.hyperliquid.xyz",
    "Referer": "https://app.hyperliquid.xyz/",
}

# --- Whale list ---
TOP_N_WHALES = 150
WHALE_REFRESH_SECONDS = 6 * 3600
WHALE_CACHE_KEY = "whale_wallets_cache"  # kv fallback if leaderboard is down

# --- Radar loop ---
RADAR_INTERVAL = 60
# Print a one-line liveness summary at most this often, so a quiet market
# (no clusters) is visibly distinguishable from a stalled loop or a
# clearinghouse outage.
HEARTBEAT_INTERVAL = 300

# --- Cluster detection thresholds (tune on data) ---
# A single position must be at least this large (USD) to count toward a cluster.
MIN_POSITION_USD = 100_000
# Only positions within this distance to their liquidation price are "at risk".
CLUSTER_MAX_DISTANCE = 0.05
# A cluster only alerts once aggregate notional at risk crosses this bar.
CLUSTER_MIN_NOTIONAL = 2_000_000
# Re-alert an existing cluster only if it grows by at least this fraction.
CLUSTER_ALERT_GROWTH = 0.30

# --- Concurrency / politeness ---
# clearinghouseState has weight 2; REST budget is 1200 weight/min per IP.
# 150 wallets/cycle = 300 weight/min, well under budget. Cap concurrency
# to avoid firing all 150 requests in the same instant.
MAX_CONCURRENT = 8

# --- Dedup: last alert per (coin, direction) ---
DEDUP_TTL = 1800  # 30 min
# key -> (ts, notional_at_last_alert)
_alerted: dict[str, tuple[float, float]] = {}


def _dedup_key(coin: str, direction: str) -> str:
    return f"{coin}:{direction}"


def _should_alert(coin: str, direction: str, notional: float) -> bool:
    """Alert on first sight, after TTL, or on meaningful cluster growth."""
    prev = _alerted.get(_dedup_key(coin, direction))
    now = time.time()
    if prev is None:
        return True
    prev_ts, prev_notional = prev
    if now - prev_ts >= DEDUP_TTL:
        return True
    if prev_notional > 0 and notional >= prev_notional * (1 + CLUSTER_ALERT_GROWTH):
        return True
    return False


def _mark_alerted(coin: str, direction: str, notional: float):
    _alerted[_dedup_key(coin, direction)] = (time.time(), notional)


def _cleanup_dedup():
    now = time.time()
    for k in [k for k, (ts, _) in _alerted.items() if now - ts > DEDUP_TTL]:
        del _alerted[k]


def _log_heartbeat(
    wallets: int, states_ok: int, states_err: int,
    clusters: list[dict], alerting: list[dict],
):
    """
    One-line liveness summary. Shows that the loop is running, whether
    clearinghouse calls are succeeding, and how close the largest cluster is
    to the alert threshold — which directly informs threshold tuning.
    """
    if clusters:
        top = clusters[0]  # _build_clusters returns them sorted desc by notional
        top_str = (
            f"{top['coin']} {top['direction']} "
            f"${top['total_notional']:,.0f} ({top['position_count']}pos)"
        )
    else:
        top_str = "none"
    print(
        f"[radar] heartbeat: {wallets} whales | "
        f"states {states_ok} ok / {states_err} err | "
        f"clusters {len(clusters)} ({len(alerting)} >= "
        f"${CLUSTER_MIN_NOTIONAL / 1e6:.1f}M) | top: {top_str}"
    )


async def fetch_whale_wallets(session: aiohttp.ClientSession) -> list[str]:
    """
    Fetch the Hyperliquid leaderboard and return the top-N wallet addresses
    by account value. Falls back to a cached list (kv store) on failure, so a
    restart during an outage still has whales to track.

    This is a GET against a static CDN host (a POST returns 403). The timeout
    is generous because the payload is large (~33 MB).
    """
    try:
        async with session.get(
            HYPERLIQUID_LEADERBOARD,
            headers=_LEADERBOARD_HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                print(f"[radar] Leaderboard HTTP {resp.status}, using cache")
                return db.kv_get_json(WHALE_CACHE_KEY, []) or []
            data = await resp.json()
    except Exception as e:
        print(f"[radar] Leaderboard fetch error: {e}, using cache")
        return db.kv_get_json(WHALE_CACHE_KEY, []) or []

    rows = data.get("leaderboardRows", []) if isinstance(data, dict) else []
    parsed = []
    for row in rows:
        addr = row.get("ethAddress")
        try:
            acct_value = float(row.get("accountValue", 0) or 0)
        except (TypeError, ValueError):
            acct_value = 0.0
        if addr and acct_value > 0:
            parsed.append((addr, acct_value))

    parsed.sort(key=lambda x: x[1], reverse=True)
    wallets = [addr for addr, _ in parsed[:TOP_N_WHALES]]

    if wallets:
        db.kv_set_json(WHALE_CACHE_KEY, wallets)
        print(f"[radar] Loaded {len(wallets)} whale wallets from leaderboard")
    else:
        wallets = db.kv_get_json(WHALE_CACHE_KEY, []) or []
        print(f"[radar] Leaderboard empty, using {len(wallets)} cached wallets")

    return wallets


async def fetch_mids(session: aiohttp.ClientSession) -> dict[str, float]:
    """Fetch mid prices for all coins. allMids has weight 2."""
    try:
        async with session.post(
            HYPERLIQUID_API,
            json={"type": "allMids"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                print(f"[radar] allMids HTTP {resp.status}")
                return {}
            data = await resp.json()
    except Exception as e:
        print(f"[radar] allMids error: {e}")
        return {}

    mids = {}
    for coin, px in (data or {}).items():
        try:
            mids[coin] = float(px)
        except (TypeError, ValueError):
            continue
    return mids


async def fetch_clearinghouse_state(
    session: aiohttp.ClientSession, address: str, sem: asyncio.Semaphore
):
    """
    Fetch a single wallet's positions. clearinghouseState has weight 2.
    Returns (address, state_dict) on HTTP 200 — even an empty portfolio comes
    back as a dict, so a None state unambiguously means a failed call (non-200,
    timeout, or exception). That distinction is what lets the heartbeat tell a
    clearinghouse outage apart from a genuinely quiet market.
    """
    async with sem:
        try:
            async with session.post(
                HYPERLIQUID_API,
                json={"type": "clearinghouseState", "user": address},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return address, None
                return address, await resp.json()
        except Exception:
            return address, None


def _position_risk(position: dict, mids: dict[str, float]) -> dict | None:
    """
    Return risk info for one position if it sits within CLUSTER_MAX_DISTANCE of
    liquidation and is large enough, else None.
    Long positions liquidate DOWN (liq below mark); shorts liquidate UP.
    """
    coin = position.get("coin", "")
    # HIP-3 assets are prefixed like "xyz:XYZ100" and aren't in the base
    # allMids map — skip them for now.
    if not coin or ":" in coin:
        return None

    try:
        szi = float(position.get("szi", 0) or 0)
        liq_px = float(position.get("liquidationPx") or 0)
        notional = float(position.get("positionValue", 0) or 0)
    except (TypeError, ValueError):
        return None

    if szi == 0 or liq_px <= 0 or notional < MIN_POSITION_USD:
        return None

    mark = mids.get(coin)
    if not mark or mark <= 0:
        return None

    if szi > 0:  # long -> liquidates below current price
        direction = "long"
        distance = (mark - liq_px) / mark
    else:        # short -> liquidates above current price
        direction = "short"
        distance = (liq_px - mark) / mark

    if distance < 0 or distance > CLUSTER_MAX_DISTANCE:
        return None

    return {
        "coin": coin,
        "direction": direction,
        "notional": notional,
        "liq_px": liq_px,
        "distance": distance,
        "leverage": (position.get("leverage") or {}).get("value"),
    }


def _build_clusters(states: list, mids: dict[str, float]) -> list[dict]:
    """
    Aggregate at-risk positions across all whales into per-(coin, direction)
    clusters. Returns ALL clusters sorted by notional (desc) — the alert
    threshold is applied by the caller, so the heartbeat can still see
    sub-threshold clusters for tuning.
    """
    buckets: dict[tuple, list] = {}
    for address, state in states:
        if not state:
            continue
        for ap in state.get("assetPositions", []):
            pos = ap.get("position") or {}
            risk = _position_risk(pos, mids)
            if risk is None:
                continue
            key = (risk["coin"], risk["direction"])
            buckets.setdefault(key, []).append({**risk, "address": address})

    clusters = []
    for (coin, direction), positions in buckets.items():
        total_notional = sum(p["notional"] for p in positions)
        liq_prices = [p["liq_px"] for p in positions]
        largest = max(positions, key=lambda p: p["notional"])
        clusters.append({
            "coin": coin,
            "direction": direction,
            "total_notional": total_notional,
            "position_count": len(positions),
            "liq_low": min(liq_prices),
            "liq_high": max(liq_prices),
            "mark_px": mids.get(coin, 0),
            "largest": largest,
        })

    clusters.sort(key=lambda c: c["total_notional"], reverse=True)
    return clusters


async def run_whale_radar_monitor(session: aiohttp.ClientSession):
    """
    Main radar loop. Refreshes the whale list every WHALE_REFRESH_SECONDS,
    polls each whale's open positions every RADAR_INTERVAL, aggregates
    liquidation clusters, and alerts on ones large enough to move price.
    A periodic heartbeat keeps the loop's health observable during quiet
    markets.
    """
    whales = await fetch_whale_wallets(session)
    last_whale_refresh = time.time()
    last_heartbeat = 0.0  # 0 => emit a heartbeat on the very first cycle
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    while True:
        try:
            if time.time() - last_whale_refresh >= WHALE_REFRESH_SECONDS:
                whales = await fetch_whale_wallets(session)
                last_whale_refresh = time.time()

            if not whales:
                print("[radar] No whale wallets available, retrying in 60s")
                await asyncio.sleep(RADAR_INTERVAL)
                continue

            mids = await fetch_mids(session)
            if not mids:
                await asyncio.sleep(RADAR_INTERVAL)
                continue

            tasks = [fetch_clearinghouse_state(session, addr, sem) for addr in whales]
            states = await asyncio.gather(*tasks)

            states_ok = sum(1 for _, s in states if s is not None)
            states_err = len(states) - states_ok

            # Total clearinghouse failure looks identical to a quiet market at
            # the cluster level (all None -> no positions -> no clusters). Call
            # it out loudly instead: this is the /info 403 case to watch for.
            if states and states_ok == 0:
                print(
                    f"[radar] ⚠ All {len(states)} clearinghouse calls failed — "
                    f"/info may be blocking (403?). Retrying in {RADAR_INTERVAL}s"
                )
                await asyncio.sleep(RADAR_INTERVAL)
                continue

            clusters = _build_clusters(states, mids)
            alerting = [
                c for c in clusters
                if c["total_notional"] >= CLUSTER_MIN_NOTIONAL
            ]

            for cluster in alerting:
                if not _should_alert(
                    cluster["coin"], cluster["direction"], cluster["total_notional"]
                ):
                    continue
                print(
                    f"[radar] 🎯 {cluster['coin']} {cluster['direction'].upper()} "
                    f"cluster ${cluster['total_notional']:,.0f} "
                    f"({cluster['position_count']} positions)"
                )
                await send_radar_alert(session, cluster)
                _mark_alerted(
                    cluster["coin"], cluster["direction"], cluster["total_notional"]
                )

            _cleanup_dedup()

            if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                _log_heartbeat(len(whales), states_ok, states_err, clusters, alerting)
                last_heartbeat = time.time()

        except asyncio.CancelledError:
            print("[radar] Monitor cancelled")
            return
        except Exception as e:
            print(f"[radar] Loop error: {e}")

        await asyncio.sleep(RADAR_INTERVAL)