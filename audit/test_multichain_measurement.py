"""
audit/test_multichain_measurement.py

Offline test for the 2026-09-21 multichain measurement change. No network, no
database writes: it imports performance_tracker and multichain_monitor and
calls their pure helpers.

  R  routing   — which DexScreener chain the tracker queries for a row
  E  entry     — which multichain alerts are recorded for measurement
  B  boring    — symbols that must no longer be treated as the token of interest

POSITIVE CONTROL: run this BEFORE replacing the two modules. The deployed
tracker has no _chain_for, so routing falls back to a copy of its inline rule
(0x -> robinhood), and the deployed monitor has no _entry_decision.
Expected on the deployed files: R1-R3 PASS, R4-R6 FAIL, every E case FAIL,
B1-B3 FAIL, B4 PASS. Any other pattern means this test is wrong, not the
modules. After replacing both modules, every case must pass.

Usage: python audit/test_multichain_measurement.py   (exit code 1 on any failure)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import performance_tracker as pt  # noqa: E402
import multichain_monitor as mm  # noqa: E402
import dexscreener_pairs as dp  # noqa: E402

SOL_MINT = "So11111111111111111111111111111111111111112"
EVM_ADDR = "0x4200000000000000000000000000000000000006"


def route(address: str, channel: str | None) -> str:
    """Chain the tracker would query; mirrors the deployed inline rule if needed."""
    if hasattr(pt, "_chain_for"):
        return pt._chain_for(address, channel)
    return "robinhood" if address.startswith("0x") else "solana"


def entry(side, dex_price, verdict, gecko_price):
    if not hasattr(mm, "_entry_decision"):
        return None
    return mm._entry_decision(side, dex_price, verdict, gecko_price)[0]


def pool(base_sym, quote_sym):
    return {"base_address": "0xbase", "base_symbol": base_sym,
            "quote_address": "0xquote", "quote_symbol": quote_sym}


RESULTS = []


def check(case_id: str, name: str, got, want) -> None:
    RESULTS.append((case_id, got == want, name, got, want))


# --- R: routing ---
check("R1", "solana mint on a solana channel", route(SOL_MINT, "jupiter"), "solana")
check("R2", "robinhood row keeps the address rule", route(EVM_ADDR, "robinhood"), "robinhood")
check("R3", "0x row with no channel keeps the address rule", route(EVM_ADDR, None), "robinhood")
check("R4", "multichain_base routes to base", route(EVM_ADDR, "multichain_base"), "base")
chains = getattr(mm, "MEASURED_CHANNEL_CHAINS", {})
all_routed = bool(chains) and all(
    route(EVM_ADDR, ch) == cid for ch, cid in chains.items()
) and set(chains) == {f"multichain_{n}" for n in mm.CHAINS}
check("R5", "every CHAINS network routes to its dexscreener id", all_routed, True)
legacy_ok = bool(chains) and all(ch in pt.LEGACY_CHANNELS for ch in chains) \
    and "robinhood" not in pt.LEGACY_CHANNELS
check("R6", "multichain channels measured by legacy path, robinhood not", legacy_ok, True)

# --- E: entry decision (values from the 2026-09-21 probe) ---
check("E1", "base side, dex/gecko 0.977 (NEST) -> record", entry("base", 0.02175, dp.OK, 0.02227), True)
check("E2", "base side, dex/gecko 0.506 (AUSD wrong majority) -> skip", entry("base", 0.5059, dp.OK, 1.0), False)
check("E3", "base side, unverified but gecko agrees (ANITA) -> record",
      entry("base", 0.001903, dp.UNVERIFIED, 0.001901), True)
check("E4", "base side, no gecko price, unverified -> skip", entry("base", 0.0019, dp.UNVERIFIED, None), False)
check("E5", "quote side, OK verdict -> record", entry("quote", 4.2e-06, dp.OK, None), True)
check("E6", "quote side, unverified (FUS) -> skip", entry("quote", 4.225e-06, dp.UNVERIFIED, None), False)
check("E7", "no base-side pair (GHO/USDG) -> skip", entry("base", None, dp.NOPAIR, 1.002), False)

# --- B: boring symbols seen live as 'interesting' ---
check("B1", "AUSD/USDC is an infra pool", mm._select_interesting(pool("AUSD", "USDC")), (None, None))
check("B2", "USDG/WETH is an infra pool", mm._select_interesting(pool("USDG", "WETH")), (None, None))
check("B3", "XAUt0/USDT0 is an infra pool", mm._select_interesting(pool("XAUt0", "USD₮0")), (None, None))
check("B4", "a meme against WMON still alerts on the meme",
      mm._select_interesting(pool("CHOG", "WMON")), ("0xbase", "CHOG"))


def main() -> int:
    print(f"tracker has _chain_for: {hasattr(pt, '_chain_for')} | "
          f"monitor has _entry_decision: {hasattr(mm, '_entry_decision')}")
    failures = []
    for case_id, ok, name, got, want in RESULTS:
        if not ok:
            failures.append(case_id)
        print(f"{case_id:<3} {'PASS' if ok else 'FAIL':<5} {name} | got={got!r} want={want!r}")
    print(f"failed cases: {failures if failures else 'none'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())