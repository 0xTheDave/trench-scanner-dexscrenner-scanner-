"""
audit/test_dexscreener_pairs.py

Synthetic test for dexscreener_pairs.select_base_pair with every defect seen
live planted in the payloads. Cases 1-3 and 6 replicate the 2026-09-21 stale
dust pools; cases 4-5 and 7 replicate the 2026-09-19 inflated-liquidity
broken price; cases 8-9 cover the quote-side and empty-payload rules; cases
10-11 cover two voters without a majority (EYXnJnQS stale pool, GLORP honest
spread).

POSITIVE CONTROLS, run BEFORE replacing the module:
  against 65f7953 (no liquidity floor): cases 1, 2, 3, 6, 7, 10, 11 FAIL
  against fa53633 (floor, no disagreement flag): exactly cases 7 and 10 FAIL
Any other pattern means this test is wrong, not the module. After replacing
the module, every case must pass.

Usage: python audit/test_dexscreener_pairs.py   (exit code 1 on any failure)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dexscreener_pairs as dp  # noqa: E402

TOKEN = "TokenMint1111111111111111111111111111111pump"
OTHER = "OtherMint2222222222222222222222222222222222"


def pair(address: str, quote: str, price: float, liquidity: float,
         base: str = TOKEN, volume: float = 0.0) -> dict:
    """A pair shaped like a /token-pairs/v1 entry (priceUsd is a string there)."""
    return {
        "dexId": "synthetic",
        "pairAddress": address,
        "baseToken": {"address": base, "symbol": "TKN"},
        "quoteToken": {"address": f"mint-{quote}", "symbol": quote},
        "priceUsd": str(price),
        "liquidity": {"usd": liquidity},
        "volume": {"h24": volume},
    }


# (number, name, payload, expected pairAddress, expected verdict)
CASES = [
    (1, "dust outvotes live pool (GpvqPj replica)",
     [pair("LIVE", "SOL", 2.563e-06, 2855, volume=1_238_946),
      pair("DUST_A", "SOL", 1.504e-04, 2, volume=17),
      pair("DUST_B", "SOL", 1.484e-04, 0.4, volume=34),
      pair("CURVE", "SOL", 4.796e-05, 0, volume=9_923)],
     "LIVE", dp.OK),

    (2, "dust with hundreds of dollars (5TEXBN replica)",
     [pair("LIVE", "SOL", 3.153e-06, 3406, volume=1_816_955),
      pair("DUST_A", "SOL", 2.270e-04, 443, volume=2_694),
      pair("DUST_B", "SOL", 3.943e-04, 398, volume=7_185),
      pair("CURVE", "SOL", 4.581e-05, 0, volume=9_479)],
     "LIVE", dp.OK),

    (3, "trusted-quote dust beats untrusted live pool (Cnfyns replica)",
     [pair("LIVE", "STONK", 1.650e-05, 11_672, volume=116_685),
      pair("DUST", "SOL", 3.296e-05, 0.3, volume=0)],
     "LIVE", dp.UNVERIFIED),

    (4, "inflated broken pair among honest pools (MON replica)",
     [pair("FAKE", "MET", 130.6, 1_437_480, volume=7_994),
      pair("ORCA_USDC", "USDC", 0.02594, 46_338),
      pair("ORCA_SOL", "SOL", 0.02545, 38_826),
      pair("MET_SOL", "SOL", 0.02551, 23_929),
      pair("MET_USDC", "USDC", 0.02592, 6_488),
      pair("RAY_SOL", "SOL", 0.02583, 1_545),
      pair("DUST_USDC", "USDC", 0.02599, 185),
      pair("QUOTE_SIDE", "MON", 3.6e-05, 16_882, base=OTHER)],
     "ORCA_USDC", dp.OK),

    (5, "4968x broken price with $147M liquidity (RAY/JUP replica)",
     [pair("FAKE", "JUP", 9341.32, 147_000_000),
      pair("H1", "USDC", 1.88, 5_000_000),
      pair("H2", "SOL", 1.87, 3_000_000),
      pair("H3", "SOL", 1.89, 800_000)],
     "H1", dp.OK),

    (6, "no pair above the floor: most liquid wins, flagged",
     [pair("LIVE", "SOL", 2.5e-06, 800, volume=50_000),
      pair("DUST_A", "SOL", 1.5e-04, 2, volume=10),
      pair("DUST_B", "SOL", 1.4e-04, 0.5, volume=5)],
     "LIVE", dp.UNVERIFIED),

    (7, "one broken + one honest above floor, broken quote untrusted: pick honest, flag",
     [pair("FAKE", "MET", 130.6, 1_437_480),
      pair("REAL", "SOL", 0.0255, 40_156)],
     "REAL", dp.UNVERIFIED),

    (8, "quote-side pairs never price the token",
     [pair("QUOTE_SIDE", "TKN", 999.0, 10_000_000, base=OTHER),
      pair("REAL", "SOL", 0.001, 50_000)],
     "REAL", dp.OK),

    (9, "no base-side pair",
     [pair("QUOTE_SIDE", "TKN", 999.0, 10_000_000, base=OTHER)],
     None, dp.NOPAIR),

    (10, "two voters 4.36x apart, stale trusted quote (EYXnJnQS replica): flag",
     [pair("LIVE", "wXRP", 3.712e-05, 18_002, volume=474_494),
      pair("STALE", "SOL", 1.617e-04, 1_193, volume=1_774),
      pair("DUST_A", "SOL", 1.26e-04, 61),
      pair("DUST_B", "SOL", 4.079e-05, 21)],
     "STALE", dp.UNVERIFIED),

    (11, "two voters 1.18x apart, honest spread (GLORP replica): unchanged",
     [pair("MEMEQ", "URA", 3.078e-04, 51_233, volume=571_643),
      pair("SOL_A", "SOL", 2.611e-04, 1_042, volume=5_817),
      pair("SOL_B", "SOL", 2.584e-04, 452),
      pair("SOL_C", "SOL", 2.556e-04, 421)],
     "SOL_A", dp.OK),
]

# Documented limitation, printed but not scored. Expected now: FAKE/unverified.
LIMITATION = (
    "two above floor, both trusted quote, broken one more liquid",
    [pair("FAKE", "USDC", 130.6, 1_437_480),
     pair("REAL", "SOL", 0.0255, 40_156)],
)


def run() -> int:
    floor = getattr(dp, "MIN_VOTING_LIQUIDITY_USD", None)
    factor = getattr(dp, "VOTER_DISAGREEMENT_FACTOR", None)
    print(f"module: {dp.__file__} | MIN_VOTING_LIQUIDITY_USD={floor} | VOTER_DISAGREEMENT_FACTOR={factor}")
    print(f"{'#':>2} {'result':<6} {'expected':<22} {'got':<22} case")
    failures = []
    for number, name, payload, want_pair, want_verdict in CASES:
        chosen, _, verdict = dp.select_base_pair(payload, TOKEN)
        got_pair = chosen.get("pairAddress") if chosen else None
        ok = got_pair == want_pair and verdict == want_verdict
        if not ok:
            failures.append(number)
        print(f"{number:>2} {'PASS' if ok else 'FAIL':<6} "
              f"{str(want_pair) + '/' + want_verdict:<22} "
              f"{str(got_pair) + '/' + verdict:<22} {name}")

    chosen, _, verdict = dp.select_base_pair(LIMITATION[1], TOKEN)
    print(f"INFO known limitation ({LIMITATION[0]}): picks "
          f"{chosen.get('pairAddress') if chosen else None}/{verdict}")

    print(f"failed cases: {failures if failures else 'none'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run())