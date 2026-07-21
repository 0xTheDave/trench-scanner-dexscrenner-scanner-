# rugcheck_diagnostic.py
# One-off diagnostic script: fetch a RugCheck report from the public
# endpoint and verify that _parse_report field paths match the actual
# JSON structure. Run locally, paste the full output back into chat.
#
# Usage:
#   python rugcheck_diagnostic.py                # uses default test mints
#   python rugcheck_diagnostic.py <mint> [mint2] # test specific mints

import asyncio
import json
import sys

import aiohttp

from rugcheck_client import _parse_report

RUGCHECK_BASE = "https://api.rugcheck.xyz"

# Default test mints: BONK (mature, mint revoked, should have full report)
# and WIF (mature, different market structure)
DEFAULT_MINTS = [
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",  # WIF
]


def _type_name(value) -> str:
    return type(value).__name__


def _inspect_key(container: dict, key: str, label: str):
    """Print whether a key exists, its type, and a short value preview."""
    if key in container:
        value = container[key]
        preview = repr(value)
        if len(preview) > 80:
            preview = preview[:80] + "..."
        print(f"  [OK]      {label}: present, type={_type_name(value)}, value={preview}")
    else:
        print(f"  [MISSING] {label}: key NOT present in JSON")


async def diagnose_mint(session: aiohttp.ClientSession, mint: str):
    print("=" * 70)
    print(f"MINT: {mint}")
    print("=" * 70)

    url = f"{RUGCHECK_BASE}/v1/tokens/{mint}/report"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            print(f"HTTP status: {resp.status}")
            if resp.status != 200:
                text = await resp.text()
                print(f"Body (first 300 chars): {text[:300]}")
                return
            data = await resp.json()
    except Exception as e:
        print(f"Request failed: {e}")
        return

    # --- 1. Top-level structure ---
    print("\n--- 1. Top-level keys ---")
    print(f"  {sorted(data.keys())}")

    # --- 2. Fields the parser relies on ---
    print("\n--- 2. Parser-critical fields ---")

    token = data.get("token")
    print(f"  token: type={_type_name(token)}")
    if isinstance(token, dict):
        print(f"  token keys: {sorted(token.keys())}")
        _inspect_key(token, "mintAuthority", "token.mintAuthority")
        _inspect_key(token, "freezeAuthority", "token.freezeAuthority")
    else:
        print("  [WARN] token is not a dict — parser will read nothing from it")

    # Alternative locations some schema versions use
    _inspect_key(data, "mintAuthority", "top-level mintAuthority (alt path)")
    _inspect_key(data, "freezeAuthority", "top-level freezeAuthority (alt path)")

    markets = data.get("markets")
    print(f"\n  markets: type={_type_name(markets)}, "
          f"count={len(markets) if isinstance(markets, list) else 'N/A'}")
    if isinstance(markets, list) and markets:
        first = markets[0]
        print(f"  markets[0] keys: {sorted(first.keys())}")
        lp = first.get("lp")
        print(f"  markets[0].lp: type={_type_name(lp)}")
        if isinstance(lp, dict):
            print(f"  markets[0].lp keys: {sorted(lp.keys())}")
            _inspect_key(lp, "lpLockedPct", "markets[0].lp.lpLockedPct")

    top_holders = data.get("topHolders")
    print(f"\n  topHolders: type={_type_name(top_holders)}, "
          f"count={len(top_holders) if isinstance(top_holders, list) else 'N/A'}")
    if isinstance(top_holders, list) and top_holders:
        print(f"  topHolders[0] keys: {sorted(top_holders[0].keys())}")
        print("  First 5 holders (checking if AMM pool is included):")
        for i, holder in enumerate(top_holders[:5]):
            addr = holder.get("address") or holder.get("owner") or "?"
            pct = holder.get("pct")
            insider = holder.get("insider")
            print(f"    #{i+1}: pct={pct} | insider={insider} | addr={str(addr)[:16]}...")

    print("\n  Score fields:")
    _inspect_key(data, "score", "score (raw)")
    _inspect_key(data, "score_normalised", "score_normalised (0-100)")
    if data.get("score_normalised") == 0:
        print("  [BUG ALERT] score_normalised == 0 — current parser falls back "
              "to raw score because of `or` (0 is falsy)")

    risks = data.get("risks")
    print(f"\n  risks: type={_type_name(risks)}, "
          f"count={len(risks) if isinstance(risks, list) else 'N/A'}")
    if isinstance(risks, list) and risks:
        print(f"  risks[0] keys: {sorted(risks[0].keys())}")
        print(f"  risks[0]: {json.dumps(risks[0], default=str)[:200]}")

    # --- 3. Run the actual parser ---
    print("\n--- 3. _parse_report() output ---")
    parsed = _parse_report(data)
    for key, value in parsed.items():
        print(f"  {key}: {value}")

    # --- 4. Sanity verdicts ---
    print("\n--- 4. Verdicts ---")
    if parsed["mint_authority_active"] is None:
        print("  [PROBLEM] mint_authority_active is None — token gets NO +10 bonus "
              "even if mint is revoked. Check section 2 above: if mintAuthority key "
              "is missing entirely, the parser condition needs fixing.")
    else:
        print(f"  mint authority parsed correctly: active={parsed['mint_authority_active']}")

    if parsed["lp_locked_pct"] is None:
        print("  [PROBLEM] lp_locked_pct is None — LP scoring inactive. "
              "Check markets[0].lp structure above.")
    else:
        print(f"  LP locked parsed correctly: {parsed['lp_locked_pct']}%")

    if parsed["top10_holders_pct"] is None:
        print("  [PROBLEM] top10_holders_pct is None — holder scoring inactive.")
    else:
        print(f"  top10 parsed: {parsed['top10_holders_pct']:.1f}% "
              "(verify vs holder list in section 2 — is the AMM pool inflating this?)")

    print()


async def main():
    mints = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_MINTS

    async with aiohttp.ClientSession(
        headers={"User-Agent": "TrenchScanner/1.0"}
    ) as session:
        for i, mint in enumerate(mints):
            if i > 0:
                # Respect 1 req/s public rate limit
                await asyncio.sleep(1.5)
            await diagnose_mint(session, mint)

    print("Diagnostic complete. Paste the FULL output back into chat.")


if __name__ == "__main__":
    asyncio.run(main())