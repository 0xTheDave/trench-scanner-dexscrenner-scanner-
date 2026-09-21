"""
audit/peek_pair_votes.py

Read-only live diagnostic. For each Solana mint, fetch DexScreener
/token-pairs/v1 and compare the pair chosen by the deployed module with the
pair chosen by a candidate rule that is NOT in the module yet.

History:
  2026-09-21a  Compared liquidity-floor and activity rules; the floor won and
               shipped in fa53633 (MIN_VOTING_LIQUIDITY_USD).
  2026-09-21b  EYXnJnQS...: a $1,193 SOL pool with a frozen price beat a
               $20k wXRP pool with 6,951 txns via the trusted-quote path, so
               the measured price ran 3.5x above the live one. A relative
               liquidity bar would fix it but breaks the inflated-liquidity
               case (fake pools are the most liquid by construction).

  2026-09-21c  Candidate `txns` (1-2 voters: most 24h transactions wins) was
               REJECTED on 80 live tokens: on GLORP it picked a pool priced
               through a fresh meme quote, 18% off five agreeing SOL pools.

Candidate rule `flag` (this file only; run it against fa53633 BEFORE the
module that contains it is deployed):
  pick exactly as the deployed module does; when there are exactly two voters
  (base-side pairs with liquidity >= MIN_VOTING_LIQUIDITY_USD) whose prices
  differ by more than FLAG_FACTOR, the verdict becomes UNVERIFIED.
Every disagreement is therefore a row whose verdict would change, and the
pick must be identical in all of them.

Usage:
  python audit/peek_pair_votes.py                    built-in mints with expectations
  python audit/peek_pair_votes.py <mint> ...         any Solana mints, full detail
  python audit/peek_pair_votes.py --from-db N "YYYY-MM-DD HH:MM"
        last N distinct Solana addresses alerted since that local time;
        prints one line per mint and full detail only where the rules disagree
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import dexscreener_pairs as dp  # noqa: E402

ENDPOINT = "https://api.dexscreener.com/token-pairs/v1/solana/{}"
DB_PATH = os.path.join(ROOT, "trench_scanner.db")
LEGACY_CHANNELS = ("jupiter", "migrations", "spikes", "alpha", "gems")
TIMEOUT_SECONDS = 20
REQUEST_SPACING_SECONDS = 0.35  # well under DexScreener's per-minute limit
FLAG_FACTOR = 2.0

# Expectations written BEFORE looking at live data, for the `flag` candidate.
DEFAULT_MINTS = {
    "GpvqPjVPqjLNxwjzjZEGirFL9tgkXmLJfPFRn9tNpump": "agree: pumpswap GuNjgCHx ok",
    "5TEXBNqSxfod4n15zqrCk1kt6nQc9KfEuqqkR2x2pump": "agree: pumpswap G9k8BmHY ok",
    "Cnfyns5wz7PJpnSr6x3Znmvpq7c9T5N9i5RshzppW3zT": "agree: raydium/STONK 5BMtPuNz unverified (single voter)",
    "CrAr4RRJMBVwRsZtT62pEhfA9H5utymC2mVx8e7FreP2": "agree: orca/USDC 7JWFfS92 ok (median path)",
    "XrwLuWhFVCdcksfzZRuBq3As2RXZGyhgGkqB8sbpump": "agree: pumpswap 2r9qnrtk ok",
    "EYXnJnQSXvsbiFx2BHggVkQVfjm1JY3aNMHbavgp9dAe": "DISAGREE: same pick BHbiNTNQ, verdict ok -> unverified",
}


def fetch(mint: str):
    request = urllib.request.Request(
        ENDPOINT.format(mint),
        headers={"User-Agent": "trench-scanner-audit/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def liquidity_of(entry: dict) -> float:
    return dp.as_float((entry.get("liquidity") or {}).get("usd")) or 0.0


def price_of(entry: dict) -> float | None:
    return dp.as_float(entry.get("priceUsd"))


def volume_24h_of(entry: dict) -> float:
    return dp.as_float((entry.get("volume") or {}).get("h24")) or 0.0


def txns_24h_of(entry: dict) -> int:
    txns = (entry.get("txns") or {}).get("h24") or {}
    return int((dp.as_float(txns.get("buys")) or 0) + (dp.as_float(txns.get("sells")) or 0))


def quote_of(entry: dict) -> str:
    return (entry.get("quoteToken") or {}).get("symbol") or "?"


def short(entry: dict | None) -> str:
    return str((entry or {}).get("pairAddress") or "-")[:8]


def age_hours_of(entry: dict) -> str:
    created = dp.as_float(entry.get("pairCreatedAt"))
    if not created:
        return "?"
    return f"{(datetime.now(timezone.utc).timestamp() - created / 1000.0) / 3600.0:.1f}"


def is_base_side(entry: dict, mint: str) -> bool:
    return dp.same_address((entry.get("baseToken") or {}).get("address"), mint)


def candidate_flag(payload, mint: str):
    """Candidate rule; see module docstring. Returns (pair, price, verdict)."""
    pair, price, verdict = dp.select_base_pair(payload, mint)
    voter_prices = [
        price_of(e) for e in payload
        if isinstance(e, dict) and is_base_side(e, mint)
        and liquidity_of(e) >= dp.MIN_VOTING_LIQUIDITY_USD and (price_of(e) or 0) > 0
    ]
    if verdict == dp.OK and len(voter_prices) == 2:
        if max(voter_prices) / min(voter_prices) > FLAG_FACTOR:
            verdict = dp.UNVERIFIED
    return pair, price, verdict


def print_pairs(payload, mint: str) -> None:
    header = (f"{'dex':<14} {'pair':<9} {'side':<6} {'quote':<8} {'priceUsd':>12} "
              f"{'liq_usd':>14} {'vol24':>14} {'txns24':>7} {'age_h':>8}")
    print(header)
    print("-" * len(header))
    rows = sorted((e for e in payload if isinstance(e, dict)), key=liquidity_of, reverse=True)
    for e in rows:
        price = price_of(e)
        print(f"{str(e.get('dexId') or '?')[:14]:<14} {short(e):<9} "
              f"{'base' if is_base_side(e, mint) else 'quote':<6} {quote_of(e)[:8]:<8} "
              f"{(f'{price:.4g}' if price is not None else 'null'):>12} "
              f"{liquidity_of(e):>14,.0f} {volume_24h_of(e):>14,.0f} "
              f"{txns_24h_of(e):>7} {age_hours_of(e):>8}")


def compare(payload, mint: str):
    cur_pair, cur_price, cur_verdict = dp.select_base_pair(payload, mint)
    new_pair, new_price, new_verdict = candidate_flag(payload, mint)
    differs = short(cur_pair) != short(new_pair) or cur_verdict != new_verdict
    ratio = None
    if cur_price and new_price:
        ratio = cur_price / new_price
    return (cur_pair, cur_price, cur_verdict), (new_pair, new_price, new_verdict), differs, ratio


def print_comparison(cur, new, ratio) -> None:
    for name, (pair, price, verdict) in (("current", cur), ("flag", new)):
        if pair is None:
            print(f"  {name:<8} {verdict}")
            continue
        print(f"  {name:<8} {short(pair):<9} {quote_of(pair)[:8]:<8} price {price:.4g} "
              f"liq {liquidity_of(pair):,.0f} txns24 {txns_24h_of(pair)} verdict {verdict}")
    if ratio:
        print(f"  current/flag price ratio: {ratio:.3g} (must be 1)")


def detail(mint: str, expectation: str | None) -> None:
    print()
    print("=" * 100)
    print(f"MINT {mint}")
    if expectation:
        print(f"EXPECTATION: {expectation}")
    try:
        payload = fetch(mint)
    except Exception as exc:  # diagnostic: report and continue
        print(f"FETCH ERROR: {type(exc).__name__}: {exc}")
        return
    if not isinstance(payload, list):
        print(f"UNEXPECTED PAYLOAD TYPE: {type(payload).__name__}: {str(payload)[:300]}")
        return
    print(f"pairs in payload: {len(payload)}")
    print_pairs(payload, mint)
    cur, new, differs, ratio = compare(payload, mint)
    print(f"rules {'DISAGREE' if differs else 'agree'}")
    print_comparison(cur, new, ratio)


def addresses_from_db(limit: int, since_local: str) -> list[tuple[str, str, str]]:
    since_ts = datetime.strptime(since_local.strip(), "%Y-%m-%d %H:%M").astimezone().timestamp()
    placeholders = ",".join("?" for _ in LEGACY_CHANNELS)
    with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            f"SELECT address, symbol, channel, MAX(alerted_at) AS last_alert FROM alerts "
            f"WHERE channel IN ({placeholders}) AND alerted_at >= ? "
            f"GROUP BY address ORDER BY last_alert DESC",
            (*LEGACY_CHANNELS, since_ts),
        ).fetchall()
    picked = [(a, s or "?", c) for a, s, c, _ in rows if a and not a.startswith("0x")]
    return picked[:limit]


def from_db(limit: int, since_local: str) -> None:
    targets = addresses_from_db(limit, since_local)
    print(f"{len(targets)} distinct Solana addresses from {', '.join(LEGACY_CHANNELS)} since {since_local} local")
    print(f"{'symbol':<12} {'channel':<11} {'pairs':>5} {'voters':>6} {'result':<9} mint")
    disagreements = []
    errors = 0
    for mint, symbol, channel in targets:
        try:
            payload = fetch(mint)
        except Exception as exc:  # diagnostic: count and continue
            errors += 1
            print(f"{symbol[:12]:<12} {channel:<11} {'-':>5} {'-':>6} {'ERROR':<9} {mint} ({type(exc).__name__})")
            time.sleep(REQUEST_SPACING_SECONDS)
            continue
        if not isinstance(payload, list):
            errors += 1
            print(f"{symbol[:12]:<12} {channel:<11} {'-':>5} {'-':>6} {'BADTYPE':<9} {mint}")
            time.sleep(REQUEST_SPACING_SECONDS)
            continue
        voters = sum(
            1 for e in payload
            if isinstance(e, dict) and is_base_side(e, mint)
            and liquidity_of(e) >= dp.MIN_VOTING_LIQUIDITY_USD and (price_of(e) or 0) > 0
        )
        cur, new, differs, ratio = compare(payload, mint)
        print(f"{symbol[:12]:<12} {channel:<11} {len(payload):>5} {voters:>6} "
              f"{('DISAGREE' if differs else 'agree'):<9} {mint}")
        if differs:
            disagreements.append((mint, symbol, channel, payload, cur, new, ratio))
        time.sleep(REQUEST_SPACING_SECONDS)

    print()
    print(f"checked {len(targets)} | errors {errors} | disagreements {len(disagreements)}")
    for mint, symbol, channel, payload, cur, new, ratio in disagreements:
        print()
        print("=" * 100)
        print(f"DISAGREEMENT {symbol} [{channel}] {mint}")
        print_pairs(payload, mint)
        print_comparison(cur, new, ratio)


def main() -> None:
    print(f"Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} local | "
          f"MIN_VOTING_LIQUIDITY_USD={getattr(dp, 'MIN_VOTING_LIQUIDITY_USD', None)}")
    args = sys.argv[1:]
    if args and args[0] == "--from-db":
        if len(args) != 3:
            print('Usage: python audit/peek_pair_votes.py --from-db N "YYYY-MM-DD HH:MM"')
            sys.exit(2)
        from_db(int(args[1]), args[2])
        return
    mints = args or list(DEFAULT_MINTS)
    for mint in mints:
        detail(mint, DEFAULT_MINTS.get(mint))
        time.sleep(REQUEST_SPACING_SECONDS)


if __name__ == "__main__":
    main()