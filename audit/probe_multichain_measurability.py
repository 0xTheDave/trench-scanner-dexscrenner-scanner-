"""
audit/probe_multichain_measurability.py

Read-only live probe. Answers, per multichain network, BEFORE any
record_alert call is added to multichain_monitor:

  1. Does DexScreener /token-pairs/v1/{chainId}/{address} accept the chainId
     that multichain_monitor.CHAINS uses for links?
  2. Is the token of interest (chosen by multichain_monitor._select_interesting,
     imported, not copied) found on the BASE side of any DexScreener pair?
  3. Does dexscreener_pairs.select_base_pair agree with GeckoTerminal's price
     for the token of interest?
  4. When the token of interest is the QUOTE side of the GeckoTerminal pool,
     which GeckoTerminal price field actually prices it?

Why: performance_tracker infers the chain from the address format
("0x" -> robinhood), so multichain rows measured today would be looked up on
the wrong chain and stored as dexscreener_nopair, which counts as -100%.

Load: one GeckoTerminal trending_pools call per network, spaced
GECKO_SPACING_SECONDS apart (the running scanner shares the IP budget), and
up to TOKENS_PER_CHAIN DexScreener calls per network.

Usage: python audit/probe_multichain_measurability.py
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import dexscreener_pairs as dp  # noqa: E402
import multichain_monitor as mm  # noqa: E402

GECKO_URL = ("https://api.geckoterminal.com/api/v2/networks/{}/trending_pools"
             "?include=base_token,quote_token")
DEX_URL = "https://api.dexscreener.com/token-pairs/v1/{}/{}"
TOKENS_PER_CHAIN = 4
GECKO_SPACING_SECONDS = 4.0
DEX_SPACING_SECONDS = 0.35
TIMEOUT_SECONDS = 20


def get_json(url: str):
    request = urllib.request.Request(
        url, headers={"User-Agent": "trench-scanner-audit/1.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except Exception as exc:  # diagnostic: report and continue
        return f"{type(exc).__name__}", None


def as_float(value):
    return dp.as_float(value)


def token_map(included) -> dict[str, dict]:
    out = {}
    for item in included or []:
        if item.get("type") == "token":
            attrs = item.get("attributes") or {}
            out[item.get("id")] = {"address": attrs.get("address"), "symbol": attrs.get("symbol")}
    return out


def normalize(raw: dict, tokens: dict[str, dict], network: str) -> dict:
    """Shape a raw GeckoTerminal pool like the fields _select_interesting reads."""
    attrs = raw.get("attributes") or {}
    rel = raw.get("relationships") or {}
    base = tokens.get(((rel.get("base_token") or {}).get("data") or {}).get("id"), {})
    quote = tokens.get(((rel.get("quote_token") or {}).get("data") or {}).get("id"), {})
    return {
        "network": network,
        "name": attrs.get("name"),
        "pool_address": attrs.get("address"),
        "base_address": base.get("address"),
        "base_symbol": base.get("symbol"),
        "quote_address": quote.get("address"),
        "quote_symbol": quote.get("symbol"),
        "base_price": as_float(attrs.get("base_token_price_usd")),
        "quote_price": as_float(attrs.get("quote_token_price_usd")),
        "reserve_usd": as_float(attrs.get("reserve_in_usd")) or 0.0,
    }


def fmt(value) -> str:
    return f"{value:.4g}" if isinstance(value, float) else str(value)


def probe_network(network: str, meta: dict) -> dict:
    chain_id = meta.get("dexscreener", network)
    print()
    print("=" * 110)
    print(f"NETWORK {network} | DexScreener chainId '{chain_id}'")
    status, payload = get_json(GECKO_URL.format(network))
    if not isinstance(payload, dict):
        print(f"  gecko trending_pools failed: {status}")
        return {"network": network, "gecko": status}

    tokens = token_map(payload.get("included"))
    pools = [normalize(p, tokens, network) for p in payload.get("data") or []]
    pools.sort(key=lambda p: p["reserve_usd"], reverse=True)

    picked, seen = [], set()
    for pool in pools:
        addr, sym = mm._select_interesting(pool)
        if not addr or addr.lower() in seen:
            continue
        seen.add(addr.lower())
        side = "base" if addr == pool["base_address"] else "quote"
        picked.append((pool, addr, sym, side))
        if len(picked) >= TOKENS_PER_CHAIN:
            break

    print(f"  gecko pools: {len(pools)} | tokens of interest probed: {len(picked)}")
    print(f"  {'symbol':<10} {'side':<5} {'gecko_base':>11} {'gecko_quote':>11} "
          f"{'gecko_itok':>11} {'dex_http':>8} {'pairs':>5} {'base_side':>9} "
          f"{'verdict':<11} {'dex_price':>11} {'dex/gecko':>9}")
    summary = {"network": network, "http": set(), "measurable": 0, "probed": len(picked),
               "quote_side": 0, "quote_side_measurable": 0}
    for pool, addr, sym, side in picked:
        gecko_itok = pool["base_price"] if side == "base" else pool["quote_price"]
        status, dex = get_json(DEX_URL.format(chain_id, addr))
        summary["http"].add(status)
        pairs = dex if isinstance(dex, list) else []
        base_side = sum(1 for e in pairs if isinstance(e, dict)
                        and dp.same_address((e.get("baseToken") or {}).get("address"), addr))
        pair, price, verdict = dp.select_base_pair(pairs, addr) if isinstance(dex, list) else (None, None, "no_list")
        ratio = (price / gecko_itok) if price and gecko_itok else None
        if price:
            summary["measurable"] += 1
        if side == "quote":
            summary["quote_side"] += 1
            if price:
                summary["quote_side_measurable"] += 1
        print(f"  {str(sym or '?')[:10]:<10} {side:<5} {fmt(pool['base_price']):>11} "
              f"{fmt(pool['quote_price']):>11} {fmt(gecko_itok):>11} {str(status):>8} "
              f"{len(pairs):>5} {base_side:>9} {verdict:<11} {fmt(price):>11} "
              f"{(f'{ratio:.3g}' if ratio else '-'):>9}")
        time.sleep(DEX_SPACING_SECONDS)
    return summary


def main() -> None:
    print("probe: multichain measurability via DexScreener (read-only)")
    results = []
    for index, (network, meta) in enumerate(mm.CHAINS.items()):
        if index:
            time.sleep(GECKO_SPACING_SECONDS)
        results.append(probe_network(network, meta))

    print()
    print("=" * 110)
    print(f"{'network':<10} {'dex_http':<14} {'measurable':>10} {'quote_side':>10} {'quote_side_meas':>15}")
    for r in results:
        if "probed" not in r:
            print(f"{r['network']:<10} gecko failed: {r.get('gecko')}")
            continue
        http = ",".join(sorted(str(h) for h in r["http"])) or "-"
        print(f"{r['network']:<10} {http:<14} {r['measurable']:>4}/{r['probed']:<5} "
              f"{r['quote_side']:>10} {r['quote_side_measurable']:>15}")


if __name__ == "__main__":
    main()