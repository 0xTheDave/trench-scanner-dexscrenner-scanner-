"""
audit/test_scanner_pair_pick.py

Offline test for scanner.py's pair pick (2026-09-22). It does NOT import
scanner.py — importing it loads the dedup state from the database and pulls
in every monitor — but extracts the _pick_pair function from the source with
ast and runs it alone, with dexscreener_pairs as its only dependency.

POSITIVE CONTROL: run this BEFORE replacing scanner.py. The deployed file has
no _pick_pair, so the test falls back to a copy of its inline rule
(most liquid pair of any side). Expected on the deployed file: S1, S3, S4, S5
FAIL and S2 PASS. Any other pattern means this test is wrong, not scanner.py.
After replacing scanner.py, every case must pass.

Usage: python audit/test_scanner_pair_pick.py   (exit code 1 on any failure)
"""

import ast
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import dexscreener_pairs as dp  # noqa: E402

MINT = "TokenMint1111111111111111111111111111111pump"


def load_pick():
    """Return (function, source_label) for the pair pick under test."""
    path = os.path.join(ROOT, "scanner.py")
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_pick_pair":
            module = ast.Module(body=[node], type_ignores=[])
            namespace = {"dsp": dp}
            exec(compile(module, path, "exec"), namespace)
            return namespace["_pick_pair"], "scanner._pick_pair"

    def deployed_inline_rule(pairs_data, addr):
        valid_pairs = [p for p in pairs_data if p.get("liquidity")]
        if not valid_pairs:
            return None
        return max(valid_pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0))
    return deployed_inline_rule, "copy of deployed inline rule"


def pair(address, base, liquidity, quote="SOL"):
    return {"pairAddress": address, "baseToken": {"address": base},
            "quoteToken": {"symbol": quote}, "liquidity": {"usd": liquidity}}


CASES = [
    ("S1", "quote-side pair more liquid than the base-side one",
     [pair("QUOTE_SIDE", "So11111111111111111111111111111111111111112", 5_000_000, "TKN"),
      pair("BASE", MINT, 40_000)], "BASE"),
    ("S2", "Nipple replica: live wXRP pool beats frozen SOL pool (most liquid wins)",
     [pair("LIVE", MINT, 10_803, "wXRP"), pair("STALE", MINT, 1_193)], "LIVE"),
    ("S3", "only quote-side pairs -> no pair",
     [pair("QUOTE_SIDE", "OtherMint", 100_000, "TKN")], None),
    ("S4", "liquidity.usd null on one pair does not crash",
     [pair("NULL_LIQ", MINT, None), pair("BASE", MINT, 10)], "BASE"),
    ("S5", "non-list payload -> no pair",
     {"pairs": []}, None),
]


def main() -> int:
    pick, label = load_pick()
    print(f"pair pick under test: {label}")
    failures = []
    for case_id, name, payload, want in CASES:
        try:
            chosen = pick(payload, MINT)
            got = chosen.get("pairAddress") if chosen else None
        except Exception as exc:  # the deployed rule crashes on some shapes
            got = f"error:{type(exc).__name__}"
        ok = got == want
        if not ok:
            failures.append(case_id)
        print(f"{case_id} {'PASS' if ok else 'FAIL':<5} {name} | got={got!r} want={want!r}")
    print(f"failed cases: {failures if failures else 'none'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())