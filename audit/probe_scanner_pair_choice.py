"""
audit/probe_scanner_pair_choice.py

Read-only live probe for queue item 3 (2026-09-22): how often does the pair
scanner.py picks for alpha / gems / spikes differ from the pair
dexscreener_pairs.select_base_pair would pick?

scanner.py (process_profile and scan_watchlist) takes
    max(pairs with liquidity, key=liquidity.usd)
with no base-side check and no cross-check. That pair drives the filters, the
score, the alert embed (symbol, price, mcap) and price_at_alert. The tracker
measures the exit with select_base_pair, so every disagreement is a return
computed across two different pairs, and a quote-side pick is an alert about
another token entirely.

For each distinct address alerted on those channels in the window, the probe
fetches /token-pairs/v1/solana/{address} once and classifies:
  same        both rules pick the same pair
  quote_side  scanner's pick has the token on the QUOTE side (wrong asset)
  base_diff   both are base-side, different pairs; the price ratio is printed
  new_nopair  select_base_pair finds no base-side pair at all
Current pairs are not the pairs at alert time, so this is an incidence
estimate, not a reconstruction of historical rows.

Usage: python audit/probe_scanner_pair_choice.py DAYS [LIMIT]
"""

import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import dexscreener_pairs as dp  # noqa: E402

DB_PATH = os.path.join(ROOT, "trench_scanner.db")
CHANNELS = ("alpha", "gems", "spikes")
ENDPOINT = "https://api.dexscreener.com/token-pairs/v1/solana/{}"
SPACING_SECONDS = 0.35
TIMEOUT_SECONDS = 20


def fetch(mint: str):
    request = urllib.request.Request(
        ENDPOINT.format(mint),
        headers={"User-Agent": "trench-scanner-audit/1.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return f"http_{exc.code}"
    except Exception as exc:  # diagnostic: report and continue
        return f"error_{type(exc).__name__}"


def liq(entry: dict) -> float:
    return dp.as_float((entry.get("liquidity") or {}).get("usd")) or 0.0


def scanner_pick(payload: list):
    """Exact copy of scanner.py's rule, for comparison only."""
    valid = [p for p in payload if isinstance(p, dict) and p.get("liquidity")]
    if not valid:
        return None
    return max(valid, key=lambda x: x.get("liquidity", {}).get("usd", 0))


def addresses(days: float, limit: int):
    since = time.time() - days * 86_400
    placeholders = ",".join("?" for _ in CHANNELS)
    with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            f"SELECT address, symbol, channel, MAX(alerted_at) AS last FROM alerts "
            f"WHERE channel IN ({placeholders}) AND alerted_at >= ? "
            f"GROUP BY address ORDER BY last DESC LIMIT ?",
            (*CHANNELS, since, limit),
        ).fetchall()
    return [(a, s or "?", c) for a, s, c, _ in rows if a and not a.startswith("0x")]


def main() -> None:
    if len(sys.argv) not in (2, 3):
        print("Usage: python audit/probe_scanner_pair_choice.py DAYS [LIMIT]")
        sys.exit(2)
    days = float(sys.argv[1])
    limit = int(sys.argv[2]) if len(sys.argv) == 3 else 150
    targets = addresses(days, limit)
    print(f"{len(targets)} distinct addresses from {', '.join(CHANNELS)} in the last {days:g} days")

    classes: Counter = Counter()
    verdicts: Counter = Counter()
    by_channel: dict[str, Counter] = {}
    details = []
    for mint, symbol, channel in targets:
        payload = fetch(mint)
        time.sleep(SPACING_SECONDS)
        if not isinstance(payload, list):
            classes["fetch_error"] += 1
            by_channel.setdefault(channel, Counter())["fetch_error"] += 1
            continue
        old = scanner_pick(payload)
        new, new_price, verdict = dp.select_base_pair(payload, mint)
        verdicts[verdict] += 1
        if old is None and new is None:
            kind = "both_none"
        elif new is None:
            kind = "new_nopair"
        elif old is None:
            kind = "old_none"
        elif old is new:
            kind = "same"
        elif not dp.same_address((old.get("baseToken") or {}).get("address"), mint):
            kind = "quote_side"
        else:
            kind = "base_diff"
        classes[kind] += 1
        by_channel.setdefault(channel, Counter())[kind] += 1
        if kind in ("quote_side", "base_diff", "new_nopair"):
            old_price = dp.as_float((old or {}).get("priceUsd"))
            ratio = (old_price / new_price) if old_price and new_price else None
            details.append((kind, symbol, channel, mint, old, old_price, new, new_price, verdict, ratio))

    checked = sum(v for k, v in classes.items() if k != "fetch_error")
    print(f"\nchecked {checked} | fetch errors {classes['fetch_error']}")
    for kind in ("same", "quote_side", "base_diff", "new_nopair", "old_none", "both_none"):
        n = classes[kind]
        print(f"  {kind:<11} {n:>4}  ({n / checked:.1%})" if checked else f"  {kind:<11} {n:>4}")
    print(f"select_base_pair verdicts: {dict(verdicts)}")
    print("\nper channel:")
    for channel, counter in sorted(by_channel.items()):
        print(f"  {channel:<7} " + " ".join(f"{k}={v}" for k, v in sorted(counter.items())))

    if details:
        print("\ndisagreements:")
        print(f"  {'kind':<11} {'symbol':<10} {'channel':<7} {'old base':<10} {'old quote':<9} "
              f"{'old price':>11} {'old liq':>12} {'new price':>11} {'new liq':>12} {'verdict':<11} {'old/new':>9}")
        for kind, symbol, channel, mint, old, old_price, new, new_price, verdict, ratio in details:
            old_base = ((old or {}).get("baseToken") or {}).get("symbol") or "-"
            old_quote = ((old or {}).get("quoteToken") or {}).get("symbol") or "-"
            print(f"  {kind:<11} {symbol[:10]:<10} {channel:<7} {old_base[:10]:<10} {old_quote[:9]:<9} "
                  f"{(f'{old_price:.4g}' if old_price else '-'):>11} {liq(old or {}):>12,.0f} "
                  f"{(f'{new_price:.4g}' if new_price else '-'):>11} {liq(new or {}):>12,.0f} "
                  f"{verdict:<11} {(f'{ratio:.3g}' if ratio else '-'):>9}  {mint}")


if __name__ == "__main__":
    main()