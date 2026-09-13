#!/usr/bin/env python3
"""
factory_listener.py — detect new pools on Robinhood Chain by polling the
public RPC. Covers Uniswap v4, v3 and v2.

VERSION 3: v2 ADDED
-------------------
Venue discovery is now finished. Coverage of production alerts by venue,
measured over 24h windows:

    v4 PoolManager 0x8366a3...   17,024 pools/day   ~76% of alerts
    v3 factory     0x1f7d75...      508 pools/day   ~13% of alerts
    v2 factory     0x8bceaa...      216 pools/day   ~ 6% of alerts
                                                    ~95% combined

v2 was found by tracing the 14 alerts that neither v4 nor v3 explained
back to their creating transaction. The method was validated on the same
run by a positive control: it rediscovered the v3 factory for the pools
already known to come from it. Without that control the v2 finding would
have been unfalsifiable.

REJECTED CANDIDATES — DO NOT RE-ADD
-----------------------------------
Two other contracts passed the detection rule and were deliberately left
out:

    0xe33e9e479df8802cb0866d5d05258bec4cf62948   14,040/day, 2 pools
    0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e   16,608/day, 2 pools

Each alone carries roughly the volume of all of v4, for two pools in a
14-pool sample, and both were pointed at by the same two tokens. They are
almost certainly a launchpad or router that emits its own event naming
the new pool and the token — which is exactly what the detection rule
looks for. The rule keys on log CONTENT, not on a contract's role, and
this is its known limit rather than a bad result.

THE v2 FEE PROBLEM, DECIDED EXPLICITLY
--------------------------------------
v2 pairs have no fee parameter; the fee is fixed at 0.30%. Applying the
normal filter would reject every v2 pair, since the threshold is 0.01%.
So v2 bypasses the fee filter entirely and everything it produces is
flagged.

That is a real decision with a real cost: +216 candidates/day on top of
roughly 1,424 from v4 and v3, about a 15% increase in watchlist volume,
in exchange for the ~6% of alerts only v2 explains. Set RH_V2_PASS_ALL=0
to apply the fee rule uniformly instead, which in practice means
dropping v2 completely.

STILL NOT AN ALERT TRIGGER
--------------------------
Measured lead time was a median of 10.9 minutes, but Initialize and
PairCreated fire when a pool EXISTS, not when it has anything to trade,
and 93 of 100 matched tokens had more than one pool (median 77s apart).
Roughly 1,600 candidates a day against ~124 actual alerts. This feeds a
watchlist, not a webhook. It still prints only; nothing is wired.

WHY POLLING
-----------
The public RPC has no eth_subscribe. Alchemy's free tier is 30M CU/month
and an unfiltered v4 subscription measured 16.88M — 1.8x headroom, which
a reconnect backfill spends. Server-side filtering cannot narrow it: fee
is the only real discriminator and on v4 it is not indexed. All three
venues are fetched in ONE eth_getLogs call, so adding venues costs no
extra requests at all.

EVENT SHAPES — ALL DIFFERENT, ALL VERIFIED ON CHAIN
---------------------------------------------------
v4  Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)
    topics: [t0, poolId, currency0, currency1]   (3 indexed)
    data:   fee, tickSpacing, hooks, sqrtPriceX96, tick
    key = poolId (32 bytes, NOT an address)

v3  PoolCreated(address,address,uint24,int24,address)
    topics: [t0, token0, token1, fee]            (3 indexed, fee among them)
    data:   tickSpacing, pool
    key = pool address

v2  PairCreated(address,address,address,uint256)
    topics: [t0, token0, token1]                 (2 indexed only)
    data:   pair, allPairsLength
    key = pair address; no fee field at all

Three venues, three topic counts, three places the fee lives (or does
not). A single decoder would silently produce garbage for two of them.

Usage:
    python factory_listener.py

Environment overrides:
    RH_RPC_URL           RPC endpoint
    RH_POLL_SECONDS      poll interval (default 2.0)
    RH_MAX_FEE           max fee in hundredths of a bip (default 100)
    RH_V2_PASS_ALL       '0' to subject v2 to the fee filter (default 1)
    RH_SHOW_ALL          '1' to print every pool
    RH_WARMUP_BLOCKS     history to replay at startup (default 0)
    RH_CONFIRMATIONS     blocks to stay behind the head (default 0)
"""

import asyncio
import json
import os
import signal
import statistics
import sys
import time
from collections import deque

try:
    import aiohttp
except ImportError:
    print("[FATAL] aiohttp is required (already in requirements.txt).")
    sys.exit(1)

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

RPC_URL = os.environ.get("RH_RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
EXPECTED_CHAIN_ID = 4663

V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
V4_INITIALIZE_TOPIC0 = (
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
)
V4_INITIALIZE_SIGNATURE = (
    "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
)

V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
V3_POOL_CREATED_TOPIC0 = (
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
)
V3_POOL_CREATED_SIGNATURE = "PoolCreated(address,address,uint24,int24,address)"

V2_FACTORY = "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f"
V2_PAIR_CREATED_TOPIC0 = (
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
)
V2_PAIR_CREATED_SIGNATURE = "PairCreated(address,address,address,uint256)"

# v2 has no fee parameter. 0.30% is the protocol constant, recorded so the
# fee histogram stays meaningful, never read from the log.
V2_FIXED_FEE_RAW = 3000

WATCHED_ADDRESSES = [V4_POOL_MANAGER, V3_FACTORY, V2_FACTORY]
WATCHED_TOPICS = [
    V4_INITIALIZE_TOPIC0, V3_POOL_CREATED_TOPIC0, V2_PAIR_CREATED_TOPIC0,
]
VENUES = ("v4", "v3", "v2")

POLL_SECONDS = float(os.environ.get("RH_POLL_SECONDS", "2.0"))
WARMUP_BLOCKS = int(os.environ.get("RH_WARMUP_BLOCKS", "0"))
CONFIRMATIONS = int(os.environ.get("RH_CONFIRMATIONS", "0"))

MAX_FEE = int(os.environ.get("RH_MAX_FEE", "100"))
V2_PASS_ALL = os.environ.get("RH_V2_PASS_ALL", "1") != "0"
DYNAMIC_FEE_FLAG = 0x800000

SHOW_ALL = os.environ.get("RH_SHOW_ALL") == "1"

MAX_BLOCKS_PER_QUERY = 20000
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_BASE = 2.0

SEEN_CAPACITY = 50000
STATS_EVERY_SECONDS = 60

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
KNOWN_CURRENCIES = {ZERO_ADDRESS: "ETH"}


# --------------------------------------------------------------------------
# Keccak-256, used to verify all three filter topics at startup
# --------------------------------------------------------------------------

_MASK64 = (1 << 64) - 1
_ROTC = [1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14,
         27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44]
_PILN = [10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4,
         15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1]
_RNDC = [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A,
    0x8000000080008000, 0x000000000000808B, 0x0000000080000001,
    0x8000000080008081, 0x8000000000008009, 0x000000000000008A,
    0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089,
    0x8000000000008003, 0x8000000000008002, 0x8000000000000080,
    0x000000000000800A, 0x800000008000000A, 0x8000000080008081,
    0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
]


def _rotl64(value: int, shift: int) -> int:
    return ((value << shift) | (value >> (64 - shift))) & _MASK64


def _keccak_f(state: list) -> list:
    for rnd in range(24):
        column = [
            state[i] ^ state[i + 5] ^ state[i + 10] ^ state[i + 15] ^ state[i + 20]
            for i in range(5)
        ]
        for i in range(5):
            diff = column[(i + 4) % 5] ^ _rotl64(column[(i + 1) % 5], 1)
            for j in range(0, 25, 5):
                state[j + i] ^= diff
        carry = state[1]
        for i in range(24):
            target = _PILN[i]
            held = state[target]
            state[target] = _rotl64(carry, _ROTC[i])
            carry = held
        for j in range(0, 25, 5):
            row = [state[j + i] for i in range(5)]
            for i in range(5):
                state[j + i] = row[i] ^ ((~row[(i + 1) % 5] & _MASK64) & row[(i + 2) % 5])
        state[0] ^= _RNDC[rnd]
    return state


def keccak256(data: bytes) -> bytes:
    rate = 136
    state = [0] * 25
    padded = bytearray(data)
    padded.append(0x01)
    while len(padded) % rate != 0:
        padded.append(0x00)
    padded[-1] ^= 0x80
    for offset in range(0, len(padded), rate):
        block = padded[offset:offset + rate]
        for i in range(rate // 8):
            state[i] ^= int.from_bytes(block[i * 8:(i + 1) * 8], "little")
        state = _keccak_f(state)
    out = bytearray()
    for i in range(4):
        out += state[i].to_bytes(8, "little")
    return bytes(out)


def verify_topics() -> list:
    """
    Re-derive every filter topic from its signature.

    A wrong topic means listening for an event that never fires, which is
    indistinguishable from a quiet chain — the worst way this module can
    fail. Returns mismatches; empty means all three are correct.
    """
    pairs = [
        ("v4 Initialize", V4_INITIALIZE_SIGNATURE, V4_INITIALIZE_TOPIC0),
        ("v3 PoolCreated", V3_POOL_CREATED_SIGNATURE, V3_POOL_CREATED_TOPIC0),
        ("v2 PairCreated", V2_PAIR_CREATED_SIGNATURE, V2_PAIR_CREATED_TOPIC0),
    ]
    bad = []
    for name, signature, expected in pairs:
        derived = "0x" + keccak256(signature.encode()).hex()
        if derived != expected:
            bad.append((name, derived, expected))
    return bad


# --------------------------------------------------------------------------
# Decoding — one decoder per venue, because the shapes genuinely differ
# --------------------------------------------------------------------------

def to_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    return int(value, 16)


def signed_from_word(word: str, bits: int) -> int:
    value = int(word, 16)
    if value >= (1 << (bits - 1)):
        value -= (1 << bits)
    return value


def decode_v4(log: dict):
    """3 indexed params; fee is in the data blob. Key is a 32-byte PoolId."""
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None
    blob = (log.get("data") or "").removeprefix("0x")
    if len(blob) < 64 * 5:
        return None
    words = [blob[i * 64:(i + 1) * 64] for i in range(5)]
    fee_raw = int(words[0], 16)
    return {
        "venue": "v4",
        "key": "0x" + topics[1].lower().removeprefix("0x"),
        "token0": "0x" + topics[2].lower().removeprefix("0x")[-40:],
        "token1": "0x" + topics[3].lower().removeprefix("0x")[-40:],
        "fee_raw": fee_raw,
        "fee_dynamic": bool(fee_raw & DYNAMIC_FEE_FLAG),
        "fee_known": True,
        "tick_spacing": signed_from_word(words[1], 24),
        "hooks": "0x" + words[2][-40:],
        "block": to_int(log.get("blockNumber")),
        "tx": log.get("transactionHash"),
    }


def decode_v3(log: dict):
    """3 indexed params; fee IS one of them, in topics[3]."""
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None
    blob = (log.get("data") or "").removeprefix("0x")
    if len(blob) < 64 * 2:
        return None
    words = [blob[i * 64:(i + 1) * 64] for i in range(2)]
    return {
        "venue": "v3",
        "key": "0x" + words[1][-40:],
        "token0": "0x" + topics[1].lower().removeprefix("0x")[-40:],
        "token1": "0x" + topics[2].lower().removeprefix("0x")[-40:],
        "fee_raw": int(topics[3], 16),
        "fee_dynamic": False,
        "fee_known": True,
        "tick_spacing": signed_from_word(words[0], 24),
        "hooks": None,
        "block": to_int(log.get("blockNumber")),
        "tx": log.get("transactionHash"),
    }


def decode_v2(log: dict):
    """
    Only 2 indexed params, so topics has 3 entries, not 4. The length
    check below must not be copied from the other two decoders — a
    >= 4 test would reject every v2 pair and the venue would look dead.

    There is no fee in this event. The protocol constant is recorded so
    the histogram stays readable, and fee_known=False marks it as ours
    rather than the chain's.
    """
    topics = log.get("topics") or []
    if len(topics) < 3:
        return None
    blob = (log.get("data") or "").removeprefix("0x")
    if len(blob) < 64 * 2:
        return None
    words = [blob[i * 64:(i + 1) * 64] for i in range(2)]
    return {
        "venue": "v2",
        "key": "0x" + words[0][-40:],
        "token0": "0x" + topics[1].lower().removeprefix("0x")[-40:],
        "token1": "0x" + topics[2].lower().removeprefix("0x")[-40:],
        "fee_raw": V2_FIXED_FEE_RAW,
        "fee_dynamic": False,
        "fee_known": False,
        "tick_spacing": None,
        "hooks": None,
        "block": to_int(log.get("blockNumber")),
        "tx": log.get("transactionHash"),
    }


DECODERS = {
    V4_INITIALIZE_TOPIC0: decode_v4,
    V3_POOL_CREATED_TOPIC0: decode_v3,
    V2_PAIR_CREATED_TOPIC0: decode_v2,
}


def fee_text(item: dict) -> str:
    if item["fee_dynamic"]:
        return "dynamic"
    suffix = "" if item["fee_known"] else " (fixed)"
    return f"{item['fee_raw'] / 10000:.4f}%{suffix}"


def currency_text(address: str) -> str:
    label = KNOWN_CURRENCIES.get(address)
    return label if label else address[:10] + "…"


def is_interesting(item: dict) -> bool:
    """
    Local fee filter. No server applies it — on v4 the fee is not indexed
    — but locally it is free, and it is the only field that separates
    real pools from the 20-95% fee traps that dominate v4.

    v2 is exempt by default: it has no fee to test, and rejecting it on
    the protocol constant of 0.30% would silently drop the whole venue.
    """
    if item["venue"] == "v2":
        return True if V2_PASS_ALL else item["fee_raw"] <= MAX_FEE
    if item["fee_dynamic"]:
        return False
    return item["fee_raw"] <= MAX_FEE


# --------------------------------------------------------------------------
# RPC
# --------------------------------------------------------------------------

class Rpc:
    def __init__(self, session: aiohttp.ClientSession, url: str):
        self.session = session
        self.url = url
        self.calls = 0
        self.errors = 0

    async def call(self, method: str, params: list):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(MAX_RETRIES):
            try:
                self.calls += 1
                async with self.session.post(
                    self.url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as response:
                    body = await response.json()
                if "error" in body:
                    return None, body["error"]
                return body.get("result"), None
            except Exception as exc:
                self.errors += 1
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
                    continue
                return None, {"transport": f"{type(exc).__name__}: {exc}"}
        return None, {"transport": "retries exhausted"}

    async def block_number(self):
        result, error = await self.call("eth_blockNumber", [])
        return (to_int(result) if result else None), error

    async def get_logs(self, from_block: int, to_block: int):
        """
        All three venues in ONE request. address takes a list and
        topics[0] takes a list of alternatives, so the per-tick request
        count is identical to the single-venue version.
        """
        return await self.call("eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": WATCHED_ADDRESSES,
            "topics": [WATCHED_TOPICS],
        }])

    async def block_timestamp(self, block_number: int):
        result, error = await self.call(
            "eth_getBlockByNumber", [hex(block_number), False]
        )
        if error or not result:
            return None
        return to_int(result.get("timestamp"))


# --------------------------------------------------------------------------
# Listener
# --------------------------------------------------------------------------

class Listener:
    def __init__(self, rpc: Rpc):
        self.rpc = rpc
        self.last_block = None
        self.seen = deque(maxlen=SEEN_CAPACITY)
        self.seen_set = set()
        self.total = {venue: 0 for venue in VENUES}
        self.matched = {venue: 0 for venue in VENUES}
        self.fee_histogram = {venue: {} for venue in VENUES}
        self.undecodable = 0
        self.unknown_topic = 0
        self.latencies = []
        self.started = time.time()
        self.last_stats = time.time()
        self.running = True

    def remember(self, key) -> bool:
        if key in self.seen_set:
            return False
        if len(self.seen) == self.seen.maxlen:
            self.seen_set.discard(self.seen[0])
        self.seen.append(key)
        self.seen_set.add(key)
        return True

    def note_fee(self, item: dict):
        label = fee_text(item)
        bucket = self.fee_histogram[item["venue"]]
        bucket[label] = bucket.get(label, 0) + 1

    async def prime(self):
        head, error = await self.rpc.block_number()
        if head is None:
            print(f"[FATAL] could not read head block: {error}")
            return False
        self.last_block = max(1, head - CONFIRMATIONS - WARMUP_BLOCKS)
        print(f"head {head}; starting from block {self.last_block + 1}")
        if WARMUP_BLOCKS:
            print(f"replaying {WARMUP_BLOCKS} blocks of history first")
        return True

    async def report(self, item: dict):
        stamp = time.strftime("%H:%M:%S")
        pair = f"{currency_text(item['token0'])}/{currency_text(item['token1'])}"
        if item["venue"] == "v4":
            extra = ("hook=none" if item["hooks"] == ZERO_ADDRESS
                     else f"hook={item['hooks'][:10]}…")
        elif item["venue"] == "v3":
            extra = f"tick={item['tick_spacing']}"
        else:
            extra = "no fee field"

        block_time = await self.rpc.block_timestamp(item["block"])
        if block_time:
            latency = time.time() - block_time
            self.latencies.append(latency)
            latency_text = f"{latency:5.1f}s"
        else:
            latency_text = "    ?"

        print(f"{stamp}  {item['venue'].upper()}  {pair:<22} "
              f"fee={fee_text(item):<17} {extra:<20} lag={latency_text}")
        print(f"          {item['key']}")
        print(f"          block {item['block']}  tx {item['tx']}")

    def print_stats(self):
        elapsed = time.time() - self.started
        total = sum(self.total.values())
        rate = total / elapsed * 3600 if elapsed else 0
        seen = " ".join(f"{v}={self.total[v]}" for v in VENUES)
        hits = " ".join(f"{v}={self.matched[v]}" for v in VENUES)
        line = (f"[stats] {elapsed / 60:.0f}m  seen={total} ({rate:.0f}/h) "
                f"[{seen}]  matched={sum(self.matched.values())} [{hits}]")
        if self.undecodable or self.unknown_topic:
            line += (f"  undecodable={self.undecodable} "
                     f"unknown_topic={self.unknown_topic}")
        if self.latencies:
            # Discovery lag only: block timestamp -> log seen. NOT
            # comparable to the 19s end-to-end pipeline figure; scoring,
            # pricing and posting all sit downstream of this.
            line += f"  discovery lag median {statistics.median(self.latencies):.1f}s"
        line += f"  rpc={self.rpc.calls} err={self.rpc.errors}"
        print(line)

    async def tick(self):
        head, error = await self.rpc.block_number()
        if head is None:
            print(f"[warn] head lookup failed: {error}")
            return
        target = head - CONFIRMATIONS
        if target <= self.last_block:
            return

        cursor = self.last_block + 1
        while cursor <= target:
            end = min(cursor + MAX_BLOCKS_PER_QUERY - 1, target)
            logs, error = await self.rpc.get_logs(cursor, end)
            if error:
                # Never advance past a range we failed to read. Losing a
                # pool silently is worse than re-reading, and the dedup
                # window makes a re-read harmless.
                print(f"[warn] getLogs {cursor}..{end} failed: "
                      f"{json.dumps(error)[:140]}")
                return

            for log in logs or []:
                key = (log.get("transactionHash"), log.get("logIndex"))
                if not self.remember(key):
                    continue

                topics = log.get("topics") or []
                topic = topics[0].lower() if topics else None
                decoder = DECODERS.get(topic)
                if decoder is None:
                    # Impossible given the server-side filter; counted
                    # rather than ignored, because if it ever fires the
                    # filter and the decoders have diverged.
                    self.unknown_topic += 1
                    continue

                item = decoder(log)
                if item is None:
                    self.undecodable += 1
                    print(f"[warn] undecodable {topic[:12]}… at "
                          f"{log.get('transactionHash')}")
                    continue

                self.total[item["venue"]] += 1
                self.note_fee(item)

                if is_interesting(item):
                    self.matched[item["venue"]] += 1
                    await self.report(item)
                elif SHOW_ALL:
                    print(f"          skip {item['venue']} "
                          f"fee={fee_text(item):<17} {item['key'][:18]}…")

            self.last_block = end
            cursor = end + 1

    async def run(self):
        if not await self.prime():
            return
        print(f"polling every {POLL_SECONDS}s; "
              f"flagging fee <= {MAX_FEE / 10000:.4f}%")
        print(f"v2 fee filter: {'bypassed (all pairs flagged)' if V2_PASS_ALL else 'applied'}")
        print(f"watching v4 {V4_POOL_MANAGER}")
        print(f"         v3 {V3_FACTORY}")
        print(f"         v2 {V2_FACTORY}")
        print("-" * 72)
        while self.running:
            start = time.time()
            try:
                await self.tick()
            except Exception as exc:
                print(f"[error] tick failed: {type(exc).__name__}: {exc}")
            if time.time() - self.last_stats >= STATS_EVERY_SECONDS:
                self.print_stats()
                self.last_stats = time.time()
            elapsed = time.time() - start
            await asyncio.sleep(max(0.0, POLL_SECONDS - elapsed))

    def summary(self):
        elapsed = time.time() - self.started
        print("\n" + "-" * 72)
        print(f"ran {elapsed / 60:.1f} minutes")
        for venue in VENUES:
            seen = self.total[venue]
            hit = self.matched[venue]
            share = f"{100.0 * hit / seen:.1f}%" if seen else "n/a"
            per_day = seen / elapsed * 86400 if elapsed else 0
            print(f"{venue}: seen {seen} ({per_day:,.0f}/day), "
                  f"flagged {hit} ({share})")
        if self.undecodable:
            print(f"undecodable: {self.undecodable}")
        if self.unknown_topic:
            print(f"unknown topic: {self.unknown_topic} "
                  f"(filter and decoders have diverged — investigate)")
        if self.latencies:
            print(f"discovery lag: median "
                  f"{statistics.median(self.latencies):.1f}s, "
                  f"max {max(self.latencies):.1f}s")
            print("  (block timestamp -> log seen; downstream stages not "
                  "included)")

        for venue in VENUES:
            buckets = self.fee_histogram[venue]
            if not buckets:
                continue
            print(f"\n{venue} fee tiers seen (all pools, filter ignored):")
            ranked = sorted(buckets.items(), key=lambda i: i[1], reverse=True)
            for label, count in ranked[:12]:
                print(f"  {count:>7}  {label}")

        print(f"\nrpc calls: {self.rpc.calls} ({self.rpc.errors} errors)")
        print("state was in memory only; nothing was written")


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

async def main():
    print("=" * 72)
    print("Robinhood Chain pool listener — v3, v4 + v3 + v2, in-memory")
    print("=" * 72)

    bad = verify_topics()
    if bad:
        for name, derived, expected in bad:
            print(f"[FATAL] {name}: signature hashes to {derived}, "
                  f"constant says {expected}")
        print("Refusing to start: a wrong topic looks like a quiet chain.")
        return 1
    print("[ok] all three filter topics verified against their signatures")

    async with aiohttp.ClientSession() as session:
        rpc = Rpc(session, RPC_URL)

        chain_id, error = await rpc.call("eth_chainId", [])
        if error or to_int(chain_id) != EXPECTED_CHAIN_ID:
            print(f"[FATAL] chain check failed: {error or to_int(chain_id)}")
            return 1
        print(f"[ok] connected to chain {EXPECTED_CHAIN_ID} at {RPC_URL}")

        listener = Listener(rpc)

        def stop(*_):
            listener.running = False

        try:
            signal.signal(signal.SIGINT, stop)
        except (ValueError, AttributeError):
            pass

        try:
            await listener.run()
        except KeyboardInterrupt:
            pass
        finally:
            listener.summary()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(0)