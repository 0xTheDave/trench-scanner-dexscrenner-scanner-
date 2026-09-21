# dexscreener_pairs.py
# ONE place that decides which DexScreener pair prices a token.
#
# Two defects, both verified against live responses, motivate every rule here:
#
#   QUOTE-SIDE PAIRS (2026-09-16). /token-pairs/v1 returns pairs where the
#   token is the QUOTE side, and priceUsd always prices the BASE token. Taking
#   the most liquid pair without checking the side stored another asset's
#   price. Live: GTA6 in 2 of 7 pairs, GLDX in 14 of 30.
#
#   BROKEN priceUsd WITH INFLATED LIQUIDITY (2026-09-19). Some pairs report a
#   priceUsd off by a large constant factor AND a liquidity figure far above
#   every honest pool, so "most liquid" lands exactly on them. Live: RAY/JUP
#   reported priceUsd 9341.32 with $147M liquidity while RAY traded at 1.88
#   across 28 other pairs — a 4968x multiplier. The same multiplier appeared
#   on PAXG, BONK, PYTH and LIT measurements, and on migrations entry prices,
#   where it also let under-liquidity tokens pass the alert threshold.
#
# The median over base-side pairs is the defence: broken pairs are a small
# minority, so they cannot move it. With fewer than MIN_PAIRS_FOR_MEDIAN pairs
# there is nothing to vote with, and the only remaining check is whether the
# pair is quoted in a well-known asset.
#
# This module is shared by performance_tracker (exit prices) and
# pumpportal_client (migration entry price, liquidity gate, chart pair).
# Keeping one copy is deliberate: a second copy of this rule WILL drift, and a
# drift here is invisible in the output until someone audits months of data.

import statistics

PRICE_OUTLIER_FACTOR = 10.0
MIN_PAIRS_FOR_MEDIAN = 3
# Symbols rather than mints: the same asset appears under several wrapped
# mints and this check only needs to be indicative.
TRUSTED_QUOTE_SYMBOLS = ("SOL", "WSOL", "USDC", "USDT")

# Verdicts returned alongside the pair.
OK = "ok"                    # cross-checked against other pairs, or trusted quote
UNVERIFIED = "unverified"    # too few pairs to check and no trusted quote
NOPAIR = "nopair"            # valid payload, no base-side pair with liquidity
BAD_PAYLOAD = "bad_payload"  # not a list of pair objects


def as_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def same_address(candidate: str | None, target: str) -> bool:
    """
    Address equality with chain-appropriate case rules.

    Solana mints are base58 and case-SENSITIVE: lowercasing could merge two
    different mints. EVM addresses (0x) are case-INSENSITIVE and APIs return
    them in mixed checksum case, so they are compared lowercased.
    """
    if not candidate or not target:
        return False
    if target.startswith("0x") or candidate.startswith("0x"):
        return candidate.lower() == target.lower()
    return candidate == target


def select_base_pair(payload, address: str, log_prefix: str = "") -> tuple[dict | None, float | None, str]:
    """
    Pick the pair that prices `address`, and say how much it can be trusted.

    Returns (pair, price_usd, verdict). The pair is returned as well as the
    price because callers need its liquidity, pairAddress and token metadata.

    Selection: among pairs where `address` is the BASE token and liquidity is
    positive, drop those whose priceUsd is more than PRICE_OUTLIER_FACTOR away
    from the median (only when there are enough pairs to take one), then take
    the most liquid survivor. When the median is unavailable, prefer pairs
    quoted in a trusted asset; if none qualifies, the most liquid pair is
    returned with the UNVERIFIED verdict — a price the caller may use but
    should be able to tell apart afterwards.
    """
    if not isinstance(payload, list):
        return None, None, BAD_PAYLOAD

    priced: list[tuple[dict, float, float]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        if not same_address((entry.get("baseToken") or {}).get("address"), address):
            continue
        liquidity = as_float((entry.get("liquidity") or {}).get("usd"))
        price = as_float(entry.get("priceUsd"))
        if liquidity and liquidity > 0 and price and price > 0:
            priced.append((entry, price, liquidity))

    if not priced:
        return None, None, NOPAIR

    verdict = OK
    if len(priced) >= MIN_PAIRS_FOR_MEDIAN:
        reference = statistics.median([price for _, price, _ in priced])
        candidates = [
            item for item in priced
            if reference / PRICE_OUTLIER_FACTOR <= item[1] <= reference * PRICE_OUTLIER_FACTOR
        ]
        # An empty result would mean the median itself is unusable; keep every
        # pair rather than inventing a price.
        candidates = candidates or priced
    else:
        candidates = [
            item for item in priced
            if ((item[0].get("quoteToken") or {}).get("symbol") or "").upper() in TRUSTED_QUOTE_SYMBOLS
        ]
        if not candidates:
            candidates = priced
            verdict = UNVERIFIED

    best = max(candidates, key=lambda item: item[2])
    top_liquidity = max(priced, key=lambda item: item[2])
    if top_liquidity[0] is not best[0] and log_prefix:
        print(f"{log_prefix} outlier pair rejected for {address}: "
              f"priceUsd {top_liquidity[1]:.6g} (liq {top_liquidity[2]:.0f}) -> "
              f"{best[1]:.6g} (liq {best[2]:.0f})")
    return best[0], best[1], verdict