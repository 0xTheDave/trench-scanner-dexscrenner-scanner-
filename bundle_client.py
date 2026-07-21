# bundle_client.py
# Trench Radar (trench.bot) bundle scanner client — pump.fun only.
#
# UNOFFICIAL, UNDOCUMENTED endpoint. Live verification during development
# (2026-07-20) showed:
#   - A malformed/non-pump.fun mint gets a fast HTTP 502.
#   - A real, freshly-launched pump.fun mint (pulled live from pump.fun's
#     own feed) also returned HTTP 502, but only after ~60s.
# No 200 response was observed in testing, so the backend currently looks
# either overloaded or broken on trench.bot's side — the URL pattern itself
# resolves and is reachable (TLS/DNS/nginx all respond normally).
# Because of that, the JSON schema below is a best-effort guess based on
# Trench Radar's own terminology ("total bundled %" vs "current held %")
# and common bundle-checker field naming, not a confirmed live schema.
# The parser accepts several key spellings defensively and dumps the raw
# response once on the first real 200 it ever sees, so the field names can
# be confirmed and tightened later.
#
# The bundle fetch runs OFF the alert critical path (see migration_monitor:
# alert is sent immediately, bundle is patched in via embed edit afterwards),
# so a generous timeout here is safe — nothing blocks on it.

import asyncio
import aiohttp
import time

TRENCH_BASE = "https://trench.bot"
BUNDLE_ADVANCED_URL = f"{TRENCH_BASE}/api/bundle/bundle_advanced/{{mint}}"

# Generous timeout: the endpoint has been observed taking ~60s before even
# failing, so a real 200 may also be slow. Safe because this runs in a
# background enrich task, never in the alert send path.
REQUEST_TIMEOUT_SECONDS = 25

# No published rate limit (unofficial API) — conservative guess, enforced
# globally so parallel migration handlers can't burst the backend.
# Tighten or loosen based on 429s/502s observed in production logs.
_MIN_REQUEST_INTERVAL = 1.5
_rate_lock = asyncio.Lock()
_last_request_ts = 0.0

# High held% across few wallets = classic "bundlers still control supply,
# dump risk" pattern. Heuristic thresholds — false positives are expected,
# this is a warning signal only, never a hard fail (see module brief).
# tune on data
HIGH_RISK_HELD_PCT = 15.0
HIGH_RISK_MAX_WALLETS = 10

_raw_response_logged = False  # one-time schema dump, see _parse_bundle_report


def is_pumpfun_mint(mint: str) -> bool:
    """Bundle data only exists for pump.fun-launched tokens (mint vanity suffix)."""
    return bool(mint) and mint.endswith("pump")


async def _acquire_rate_slot():
    """Wait until at least _MIN_REQUEST_INTERVAL passed since last request."""
    global _last_request_ts
    async with _rate_lock:
        now = time.monotonic()
        wait = _MIN_REQUEST_INTERVAL - (now - _last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_ts = time.monotonic()


def _parse_bundle_report(data: dict, mint: str) -> dict | None:
    """
    Normalize a bundle_advanced response into flat fields.
    All fields default to None when not present or unrecognized —
    see module docstring for why the schema is a best guess.
    """
    global _raw_response_logged
    if not _raw_response_logged:
        print(f"[bundle] RAW response for {mint[:8]}... (verify field names): {data}")
        _raw_response_logged = True

    if not isinstance(data, dict):
        return None

    def _first_float(*keys) -> float | None:
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return None

    def _first_int(*keys) -> int | None:
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    continue
        return None

    result = {
        "held_pct": _first_float(
            "held_percentage", "current_held_percentage", "percent_held",
            "held_pct", "current_held_pct",
        ),
        "total_bundled_pct": _first_float(
            "total_percentage_bundled", "bundled_percentage", "total_bundled_percentage",
            "percent_bundled", "total_percent_bundled",
        ),
        "wallet_count": _first_int(
            "total_holders", "bundle_count", "wallet_count", "num_wallets", "total_wallets",
        ),
        "sol_spent": _first_float(
            "total_sol_spent", "sol_spent", "total_holding_amount", "sol_invested",
        ),
    }

    if all(v is None for v in result.values()):
        print(f"[bundle] Unrecognized schema for {mint[:8]}... — no known fields matched")
        return None

    return result


async def fetch_bundle_report(
    session: aiohttp.ClientSession,
    mint: str,
) -> dict | None:
    """
    Fetch and parse a Trench Radar bundle report for a pump.fun mint.
    Returns None for non-pump.fun mints, or whenever the (unofficial,
    currently unstable) API is unavailable, slow, or returns something
    unparseable. Never raises — callers should treat None as "no data".
    """
    if not is_pumpfun_mint(mint):
        return None

    await _acquire_rate_slot()
    try:
        async with session.get(
            BUNDLE_ADVANCED_URL.format(mint=mint),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
        ) as resp:
            if resp.status != 200:
                print(f"[bundle] HTTP {resp.status} for {mint[:8]}...")
                return None
            data = await resp.json()
            return _parse_bundle_report(data, mint)
    except asyncio.TimeoutError:
        print(f"[bundle] Timeout for {mint[:8]}...")
        return None
    except aiohttp.ContentTypeError:
        print(f"[bundle] Non-JSON response for {mint[:8]}...")
        return None
    except Exception as e:
        print(f"[bundle] Error: {e}")
        return None


def assess_bundle_risk(report: dict | None) -> tuple[str, bool]:
    """
    Turn a parsed bundle report into a short warning label + high_risk flag.
    Returns ("n/a", False) when there's no usable data — this is the
    graceful-degradation path, never a crash and never a blocked alert.
    """
    if report is None:
        return "n/a", False

    held_pct = report.get("held_pct")
    if held_pct is None:
        return "n/a", False

    wallet_count = report.get("wallet_count")
    high_risk = (
        held_pct >= HIGH_RISK_HELD_PCT
        and wallet_count is not None
        and wallet_count <= HIGH_RISK_MAX_WALLETS
    )

    label = f"{held_pct:.1f}% held"
    if wallet_count is not None:
        label += f" by {wallet_count} wallets"

    return (f"⚠️ {label} — bundlers still control supply" if high_risk else label), high_risk