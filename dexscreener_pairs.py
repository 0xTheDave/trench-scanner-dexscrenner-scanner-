# dexscreener_pairs.py
# ONE place that decides which DexScreener pair prices a token.
#
# Three defects, all verified against live responses, motivate every rule here:
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
#   STALE DUST POOLS (2026-09-21). On dead pump.fun tokens, meteora pools with
#   $0-443 liquidity keep a price from before the collapse and still trade a
#   little. Two of them outvote the one live pumpswap pool in an unweighted
#   median, and a dust pool quoted in SOL beat a live pool quoted in an
#   untrusted asset on the trusted-quote path. Live: GpvqPj... pumpswap
#   2.563e-6 @ $2,855 lost to meteora 1.504e-4 @ $2 (59x); 5TEXBN... pumpswap
#   3.153e-6 @ $3,406 lost to meteora 2.27e-4 @ $443 (72x); Cnfyns... raydium
#   /STONK 1.65e-5 @ $11,672 lost to meteora/SOL 3.296e-5 @ ~$0. Volume does
#   NOT separate dust from live pools (dust pools had 24h volume); liquidity
#   does. Hence the voting floor below.
#
# The median over base-side pairs above the liquidity floor is the defence:
# broken pairs are a small minority of meaningful pools, so they cannot move
# it, and dust pools do not get a vote. With fewer than MIN_PAIRS_FOR_MEDIAN
# voters there is nothing to vote with, and the only remaining check is
# whether the pair is quoted in a well-known asset. When no pair clears the
# floor, the most liquid pair is returned as UNVERIFIED: that is the live pool
# in every dust case seen, and inflated broken pairs never sit below the floor.
#
# Known limitation: if exactly two pairs clear the floor, one broken and one
# honest, and both are quoted in a trusted asset, the most liquid one wins,
# which is the broken one. Not observed live so far.
#
# This module is shared by performance_tracker (exit prices) and
# pumpportal_client (migration entry price, liquidity gate, chart pair).
# Keeping one copy is deliberate: a second copy of this rule WILL drift, and a
# drift here is invisible in the output until someone audits months of data.

import statistics

PRICE_OUTLIER_FACTOR = 10.0
MIN_PAIRS_FOR_MEDIAN = 3
# Calibrated on live data 2026-09-21: largest dust pool $443, smallest live
# pool $2,082. The margin is roughly 2x on each side; revisit if either end
# moves.
MIN_VOTING_LIQUIDITY_USD = 1000.0
# Symbols rather than mints: the same asset appears under several wrapped
# mints and this check only needs to be indicative.
TRUSTED_QUOTE_SYMBOLS = ("SOL", "WSOL", "USDC", "USDT")

# Verdicts returned alongside the pair.
OK = "ok"                    # cross-checked against other pairs, or trusted quote
UNVERIFIED = "unverified"    # too few pairs to check and no trusted quote, or no pair above the floor
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
    positive, only those with liquidity >= MIN_VOTING_LIQUIDITY_USD vote and
    can win. If none does, the most liquid pair is returned as UNVERIFIED.
    Among the voters, drop those whose priceUsd is more than
    PRICE_OUTLIER_FACTOR away from the median (only when there are enough
    voters to take one), then take the most liquid survivor. When the median
    is unavailable, prefer voters quoted in a trusted asset; if none
    qualifies, the most liquid voter is returned with the UNVERIFIED verdict —
    a price the caller may use but should be able to tell apart afterwards.
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

    top_liquidity = max(priced, key=lambda item: item[2])

    voters = [item for item in priced if item[2] >= MIN_VOTING_LIQUIDITY_USD]
    if not voters:
        # Nothing meaningful to vote with. The most liquid pair is the live
        # pool in every dust case seen, and broken inflated pairs never sit
        # below the floor, so this is the least bad choice — but say so.
        return top_liquidity[0], top_liquidity[1], UNVERIFIED

    verdict = OK
    if len(voters) >= MIN_PAIRS_FOR_MEDIAN:
        reference = statistics.median([price for _, price, _ in voters])
        candidates = [
            item for item in voters
            if reference / PRICE_OUTLIER_FACTOR <= item[1] <= reference * PRICE_OUTLIER_FACTOR
        ]
        # An empty result would mean the median itself is unusable; keep every
        # voter rather than inventing a price.
        candidates = candidates or voters
    else:
        candidates = [
            item for item in voters
            if ((item[0].get("quoteToken") or {}).get("symbol") or "").upper() in TRUSTED_QUOTE_SYMBOLS
        ]
        if not candidates:
            candidates = voters
            verdict = UNVERIFIED

    best = max(candidates, key=lambda item: item[2])
    if top_liquidity[0] is not best[0] and log_prefix:
        print(f"{log_prefix} outlier pair rejected for {address}: "
              f"priceUsd {top_liquidity[1]:.6g} (liq {top_liquidity[2]:.0f}) -> "
              f"{best[1]:.6g} (liq {best[2]:.0f})")
    return best[0], best[1], verdict