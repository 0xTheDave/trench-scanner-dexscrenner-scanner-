# performance_tracker.py
# Measures alert performance: fetches token price 1h/6h/24h after each
# alert and stores it in SQLite. Posts a per-channel performance report
# on a fixed cadence (Discord webhook optional — logs only if unset).
#
# TWO MEASUREMENT PATHS, and the difference matters more than it looks.
#
#   LEGACY (DexScreener): reads the most-liquid pair AT MEASUREMENT TIME.
#   On Robinhood Chain a token accumulates high-fee trap pools after the alert,
#   so "most liquid an hour later" routinely names a different venue than the
#   one that was quoted — median divergence +111.6pp on the sampled rows. Kept
#   for solana channels, where the pathology does not occur and no pool is
#   pinned.
#
#   PINNED (GeckoTerminal OHLCV): reads the pool that was pinned at alert time,
#   bounded to the measurement instant with before_timestamp. BOTH ends of the
#   return come from the same venue: the entry is re-derived from that pool's
#   candle at alert time (entry_price_pinned) rather than reusing
#   price_at_alert, which came from DexScreener and disagreed with the pinned
#   pool on 11.9% of selections (n=469).
#
# The two are NOT comparable and must never be averaged together. Every write
# records its method in the matching price_*_src column, and the report groups
# by it instead of pooling.
#
# SLOT ROTATION IS NOT COSMETIC. Both paths are budget-limited, and a fixed
# slot order means the first slot consumes the entire budget on every run while
# the later ones never execute at all. That is not "slower measurement" — for
# the pinned path it is permanent starvation, because robinhood rows have no
# fallback path and a NULL price column is re-selected forever. The starting
# slot therefore rotates, and the offset is PERSISTED: an in-memory counter
# would reset to price_1h on every restart and reproduce the same starvation.
#
# TWO SEPARATE BUDGETS, because the two paths are limited by different things.
#
#   ROW budget (legacy): bounded by wall clock. Each row costs REQUEST_DELAY
#   seconds, so the cap must be a per-RUN total across all slots and channels.
#   An earlier revision applied the cap per channel inside a nested loop, which
#   silently multiplied the work by the channel count and pushed a single run
#   past the 600s scheduling interval. The cap belongs outside every loop.
#
#   CALL budget (pinned): bounded by GeckoTerminal's shared quota. Measured
#   ceiling is ~5 requests/min for ALL consumers combined (jupiter enrichment,
#   multichain discovery, migration charts, robinhood charts, and this module);
#   429s begin at the 6th request in a 60s window regardless of spacing, so the
#   binding constraint is request COUNT per window.
#
#   NOTE: one unit of the call budget can cost TWO HTTP requests, because
#   geckoterminal_client._get retries once after a 429. The budget is therefore
#   sized against the worst case, not the nominal one.
#
#   CAPACITY, measured rather than assumed: a new row costs 2 calls (entry
#   derivation + one slot); subsequent slots for that row cost 1, since the
#   entry is cached in entry_price_pinned. Full lifetime is 4 calls per alert.
#   At ~124 robinhood alerts/day that is ~21 calls/hour against a supply of
#   GECKO_CALL_BUDGET_PER_RUN x 6 runs/hour. The margin is thin and leaves no
#   room to drain a backlog — if the [perf] line shows budget= on most runs,
#   demand elsewhere must be cut, not this budget raised.

import asyncio
import aiohttp
import os
import statistics
import time
from datetime import datetime, timezone

import db
import geckoterminal_client as gt

DEXSCREENER_BASE = "https://api.dexscreener.com"
WEBHOOK_PERFORMANCE = os.environ.get("DISCORD_WEBHOOK_PERFORMANCE", "")

# Measurement method markers written to price_*_src.
SRC_LEGACY = db.SRC_DEXSCREENER_CURRENT
SRC_PINNED = db.SRC_GECKO_PINNED
# Pinned pool answered, but its newest candle at or before the measurement
# instant is older than the slot's tolerance: the pool stopped trading before
# the mark. The price is real but it is not the price AT the mark. Recorded
# rather than left NULL, because a NULL row is re-selected forever by the due
# query; flagged rather than merged, because "last traded at" and "tradable at"
# are different claims.
SRC_PINNED_STALE = "gecko_pinned_stale"
# Pinned pool returned no candle at all before the mark.
SRC_PINNED_NO_DATA = "gecko_pinned_nodata"

# (column, seconds after alert, timeframe, aggregate, staleness tolerance)
#
# Timeframe widens with the horizon because GeckoTerminal does not retain
# minute candles indefinitely: asking for 1-minute data 24h back returns
# nothing on quiet pools, which would look like a dead token rather than a
# retention limit. Tolerance widens for the same reason — a 24h mark measured
# against an hourly series cannot be sharper than an hour.
MEASUREMENT_SLOTS = (
    ("price_1h", 3_600, "minute", 1, 900),
    ("price_6h", 21_600, "minute", 5, 1_800),
    ("price_24h", 86_400, "hour", 1, 7_200),
)

# Rotation offset, persisted so restarts cannot pin the order back to price_1h.
_SLOT_OFFSET_KEY = "perf_slot_offset"

# Entry candle: the pool is minutes old at alert time (observed alert ages
# 0.1-0.8h), so the series is short and 1-minute resolution is both available
# and necessary.
ENTRY_TIMEFRAME = "minute"
ENTRY_AGGREGATE = 1
ENTRY_TOLERANCE = 900

# Channels measured by the legacy path. Robinhood is deliberately absent: the
# pinned path owns it, and letting the legacy path reach those rows first would
# fill the price column with a DexScreener reading that the pinned path can
# then never replace (selection keys off the column being NULL).
#
# SHARP EDGE: a channel added to the scanner later and not added here will
# never be measured. The startup line below prints this list so the omission is
# visible rather than silent.
LEGACY_CHANNELS = ("jupiter", "migrations", "spikes", "alpha", "gems")
PINNED_CHANNEL = "robinhood"
PINNED_NETWORK_HINT = "robinhood"

# Rows per RUN for the legacy path, across every slot and channel combined.
# At REQUEST_DELAY=1.0 this bounds a run at ~75s of sleeping, comfortably
# inside the 600s scheduling interval.
BATCH_LIMIT_LEGACY_PER_RUN = 75
# Rows considered per slot on the pinned path. The call budget binds first in
# practice; this only stops one slot from starving the others.
BATCH_LIMIT_PINNED = 3
GECKO_CALL_BUDGET_PER_RUN = 4

REQUEST_DELAY = 1.0
REPORT_DAYS = 7
RUG_THRESHOLD = 0.10        # price_24h <= 10% of entry counts as rugged

# Report cadence is enforced HERE via a DB timestamp, not by the scanner's
# sleep loop. The old design slept 24h before the first report and reset that
# clock on every restart, so during active development the report never fired.
# 6h while tuning thresholds; bump back to 24h once the strategy settles.
REPORT_INTERVAL_SECONDS = 6 * 3600
_LAST_REPORT_KEY = "perf_last_report_ts"

_network_id: str | None = None
_startup_logged = False


def _rotated_slots() -> tuple:
    """
    MEASUREMENT_SLOTS starting from a different slot each run.

    Advances and persists the offset as a side effect, so the rotation survives
    restarts. Without persistence a process that restarts often — which is the
    normal state during development — would always start at price_1h and the
    later slots would never be reached, which is exactly the failure this
    exists to prevent.
    """
    offset = db.kv_get_json(_SLOT_OFFSET_KEY, 0) or 0
    offset = offset % len(MEASUREMENT_SLOTS)
    db.kv_set_json(_SLOT_OFFSET_KEY, (offset + 1) % len(MEASUREMENT_SLOTS))
    return MEASUREMENT_SLOTS[offset:] + MEASUREMENT_SLOTS[:offset]


# === Legacy path (DexScreener) ===

async def _fetch_current_price(session: aiohttp.ClientSession, address: str) -> float:
    """
    Current price from the most liquid DexScreener pair. The chain is inferred
    from the address format: Robinhood Chain tokens are 0x-prefixed EVM
    addresses, Solana mints are base58 (never 0x). Without this split, robinhood
    alerts would be queried against the solana endpoint and always read as dead.
    Returns 0.0 when no tradable pair exists — the token is dead, which
    correctly counts as a -100% outcome, not missing data.
    """
    chain = "robinhood" if address.startswith("0x") else "solana"
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/{chain}/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return 0.0
            data = await resp.json()
    except Exception:
        return 0.0

    if not data:
        return 0.0

    valid = [p for p in data if (p.get("liquidity") or {}).get("usd")]
    if not valid:
        return 0.0

    best = max(valid, key=lambda x: x.get("liquidity", {}).get("usd", 0))
    try:
        return float(best.get("priceUsd") or 0)
    except (ValueError, TypeError):
        return 0.0


# === Budgets ===

class _Budget:
    """
    A countdown of allowed operations for ONE run.

    Instantiated per run rather than kept module-level so a run cannot inherit
    a previous run's exhausted state, and so `spent` is readable in the closing
    log line.
    """

    def __init__(self, limit: int):
        self.limit = limit
        self.remaining = limit
        self.spent = 0

    def take(self) -> bool:
        if self.remaining <= 0:
            return False
        self.remaining -= 1
        self.spent += 1
        return True


# === Pinned path (GeckoTerminal OHLCV) ===

async def _resolve_network(session: aiohttp.ClientSession, budget: _Budget) -> str | None:
    """
    Resolve GeckoTerminal's network id for Robinhood Chain.

    Charges the call budget only when a request will actually be made.
    geckoterminal_client caches the id module-wide and robinhood_monitor
    resolves it during its own startup, so by the time this runs the answer is
    usually free — charging for it anyway would silently spend a quarter of the
    run's budget on nothing.
    """
    global _network_id
    if _network_id:
        return _network_id

    cached = gt._network_id_cache.get(PINNED_NETWORK_HINT.lower())
    if cached:
        _network_id = cached
        return _network_id

    if not budget.take():
        return None
    _network_id = await gt.resolve_network_id(session, PINNED_NETWORK_HINT)
    if not _network_id:
        print("[perf] GeckoTerminal exposes no robinhood network — "
              "pinned path unavailable this run")
    return _network_id


async def _candle_at(
    session: aiohttp.ClientSession,
    network: str,
    pool_key: str,
    bound: int,
    timeframe: str,
    aggregate: int,
    tolerance: int,
    budget: _Budget,
) -> tuple[float | None, str | None]:
    """
    Closing price from the newest candle at or before `bound`.

    Returns (price, verdict) where verdict is one of: None (clean), "stale",
    "nodata", "budget", "window_violation".

    TWO INDEPENDENT CHECKS, on purpose:

      1. ts <= bound — did before_timestamp actually bound the result? Proven
         against this API (12/12 probes, 4 of them decisive), but a per-call
         check is what catches a regression or a pool where the parameter
         behaves differently. If this fails the price is DISCARDED, not stored:
         a candle from after the mark is a future price, and writing it would
         fabricate foresight.

      2. bound - ts <= tolerance — is the candle close enough to the mark to
         represent it? A pool that stopped trading returns its last candle,
         which can be hours early. That price is real but answers a different
         question, so it is stored under a distinct source marker.

    Merging these into one condition would lose the distinction between "the
    API misbehaved" (never storable) and "the pool went quiet" (storable, with
    a caveat).
    """
    if not budget.take():
        return None, "budget"

    candles = await gt.fetch_ohlcv(
        session, network, pool_key,
        timeframe=timeframe, aggregate=aggregate, limit=3,
        before_timestamp=bound,
    )
    if not candles:
        return None, "nodata"

    try:
        ts = int(candles[0][0])
        close = float(candles[0][4])
    except (IndexError, TypeError, ValueError):
        return None, "nodata"

    if ts > bound:
        return None, "window_violation"
    if close <= 0:
        return None, "nodata"
    if (bound - ts) > tolerance:
        return close, "stale"
    return close, None


async def _ensure_entry_price(
    session: aiohttp.ClientSession,
    network: str,
    row: dict,
    budget: _Budget,
) -> float | None:
    """
    Entry price from the PINNED pool at alert time, derived once per row and
    reused by every later slot.

    Returns None when it cannot be derived, which disqualifies the row from the
    pinned path entirely — a return with a DexScreener entry and a pinned exit
    is exactly the cross-venue artefact this path exists to remove.
    """
    existing = row.get("entry_price_pinned")
    if existing and existing > 0:
        return existing

    price, verdict = await _candle_at(
        session, network, row["pool_address"], int(row["alerted_at"]),
        ENTRY_TIMEFRAME, ENTRY_AGGREGATE, ENTRY_TOLERANCE, budget,
    )
    # A stale entry candle is rejected rather than accepted with a marker:
    # on this chain entry prices drift ~9%/min, so a candle 15 minutes off the
    # alert is not the entry, and every return computed from it would be wrong
    # by an unknown amount in an unknown direction.
    if price is None or verdict in ("stale", "window_violation"):
        return None
    db.set_entry_price_pinned(row["id"], price)
    return price


async def _measure_pinned_slot(
    session: aiohttp.ClientSession,
    network: str,
    column: str,
    delay_seconds: int,
    timeframe: str,
    aggregate: int,
    tolerance: int,
    budget: _Budget,
) -> tuple[int, dict]:
    """
    Measure one slot for robinhood rows via the pinned pool.
    Returns (rows_measured, verdict_counts).
    """
    rows = db.due_measurements_pinned(
        column, delay_seconds, BATCH_LIMIT_PINNED, channel=PINNED_CHANNEL
    )
    measured = 0
    verdicts: dict[str, int] = {}

    for row in rows:
        if budget.remaining <= 0:
            verdicts["budget"] = verdicts.get("budget", 0) + 1
            break

        # 4.0% of instrumented rows have no pinned pool (GeckoTerminal had not
        # indexed the token yet). They fall back to the legacy path instead of
        # being skipped: skipping would leave the column NULL and re-select the
        # same rows on every run, forever.
        if not row.get("pool_address"):
            await asyncio.sleep(REQUEST_DELAY)
            price = await _fetch_current_price(session, row["address"])
            db.set_measurement(row["id"], column, price, SRC_LEGACY)
            measured += 1
            verdicts["no_pin_fallback"] = verdicts.get("no_pin_fallback", 0) + 1
            continue

        entry = await _ensure_entry_price(session, network, row, budget)
        if entry is None:
            if budget.remaining <= 0:
                # Ran out mid-row rather than genuinely failing to derive an
                # entry. Leave the row untouched so the next run can retry it;
                # writing a legacy price here would permanently deny it the
                # pinned path.
                verdicts["budget"] = verdicts.get("budget", 0) + 1
                break
            # Cannot build a same-venue return. Fall back rather than leave the
            # row to be re-selected indefinitely; the marker keeps it out of
            # pinned aggregates.
            await asyncio.sleep(REQUEST_DELAY)
            price = await _fetch_current_price(session, row["address"])
            db.set_measurement(row["id"], column, price, SRC_LEGACY)
            measured += 1
            verdicts["no_entry_fallback"] = verdicts.get("no_entry_fallback", 0) + 1
            continue

        bound = int(row["alerted_at"]) + delay_seconds
        price, verdict = await _candle_at(
            session, network, row["pool_address"], bound,
            timeframe, aggregate, tolerance, budget,
        )

        if verdict == "budget":
            verdicts["budget"] = verdicts.get("budget", 0) + 1
            break
        if verdict == "window_violation":
            # Never stored. Leaving the row NULL is correct here: this is an API
            # fault, not a fact about the token, and the row should be retried.
            print(f"[perf] WINDOW VIOLATION on row {row['id']} "
                  f"({row.get('symbol')}) — candle newer than the bound; "
                  f"before_timestamp may have regressed")
            verdicts["window_violation"] = verdicts.get("window_violation", 0) + 1
            continue
        if verdict == "nodata":
            # The pool has no candle before the mark. Distinct from a dead
            # token: it may simply be unindexed. Recorded as 0.0 with its own
            # marker so aggregates can exclude it rather than read -100%.
            db.set_measurement(row["id"], column, 0.0, SRC_PINNED_NO_DATA)
            measured += 1
            verdicts["nodata"] = verdicts.get("nodata", 0) + 1
            continue

        source = SRC_PINNED_STALE if verdict == "stale" else SRC_PINNED
        db.set_measurement(row["id"], column, price, source)
        measured += 1
        key = "stale" if verdict == "stale" else "clean"
        verdicts[key] = verdicts.get(key, 0) + 1

    return measured, verdicts


# === Entry point called by the scanner loop ===

async def track_performance(session: aiohttp.ClientSession):
    """
    Fill in due price measurements for recorded alerts.

    Signature unchanged — scanner.py drives this via run_periodic(session) and
    needs no modification.
    """
    global _startup_logged
    if not _startup_logged:
        _startup_logged = True
        print(f"[perf] pinned path: #{PINNED_CHANNEL} "
              f"({GECKO_CALL_BUDGET_PER_RUN} gecko calls/run) | "
              f"legacy path: {', '.join(LEGACY_CHANNELS)} "
              f"({BATCH_LIMIT_LEGACY_PER_RUN} rows/run) | slot order rotates")

    started = time.monotonic()
    calls = _Budget(GECKO_CALL_BUDGET_PER_RUN)
    legacy_rows = _Budget(BATCH_LIMIT_LEGACY_PER_RUN)
    measured = 0
    all_verdicts: dict[str, int] = {}

    # Both paths walk the slots in the same rotated order, advanced once per
    # run. Under a budget the first slot visited is the one that gets served,
    # so rotating is what stops 6h and 24h from being permanently unreachable.
    slots = _rotated_slots()

    # --- pinned path first: it has the tighter constraint, and rows it cannot
    # --- reach this run must not be picked up by the legacy path.
    network = await _resolve_network(session, calls)
    if network:
        for column, delay, timeframe, aggregate, tolerance in slots:
            if calls.remaining <= 0:
                break
            count, verdicts = await _measure_pinned_slot(
                session, network, column, delay, timeframe, aggregate,
                tolerance, calls,
            )
            measured += count
            for key, value in verdicts.items():
                all_verdicts[key] = all_verdicts.get(key, 0) + value

    # --- legacy path for every other channel ---
    # The row budget is checked in BOTH loops and before every request. Bounding
    # only the per-query LIMIT would cap rows per channel while leaving the run
    # unbounded, which previously pushed a run past its own interval.
    for column, delay, _tf, _agg, _tol in slots:
        if legacy_rows.remaining <= 0:
            break
        for channel in LEGACY_CHANNELS:
            if legacy_rows.remaining <= 0:
                break
            rows = db.due_measurements_pinned(
                column, delay, min(legacy_rows.remaining, 25), channel=channel
            )
            for row in rows:
                if not legacy_rows.take():
                    break
                await asyncio.sleep(REQUEST_DELAY)
                price = await _fetch_current_price(session, row["address"])
                db.set_measurement(row["id"], column, price, SRC_LEGACY)
                measured += 1

    elapsed = time.monotonic() - started
    if measured:
        detail = " ".join(f"{k}={v}" for k, v in sorted(all_verdicts.items()))
        first_slot = slots[0][0]
        print(f"[perf] Recorded {measured} measurements in {elapsed:.0f}s "
              f"| from {first_slot} "
              f"| gecko {calls.spent}/{calls.limit} "
              f"| legacy rows {legacy_rows.spent}/{legacy_rows.limit}"
              + (f" | pinned: {detail}" if detail else ""))


# === Reporting ===

def _entry_for(row: dict, src: str | None) -> float | None:
    """
    The entry price that matches how the exit was measured.

    A pinned exit must be divided by the pinned entry; pairing it with
    price_at_alert reintroduces the cross-venue gap the pinned path exists to
    close. Returns None when the matching entry is missing, which drops the row
    from that slot's statistics rather than silently substituting the other one.
    """
    if src in (SRC_PINNED, SRC_PINNED_STALE):
        entry = row.get("entry_price_pinned")
        return entry if entry and entry > 0 else None
    entry = row.get("price_at_alert")
    return entry if entry and entry > 0 else None


def _pct_return(entry: float | None, current: float | None) -> float | None:
    if current is None or not entry or entry <= 0:
        return None
    return (current / entry - 1) * 100


def _slot_stats(rows: list[dict], column: str, src_column: str) -> dict[str, list[float]]:
    """
    Returns for one slot, GROUPED BY measurement method.

    Grouping is not presentation — it is correctness. A DexScreener reading and
    a pinned-pool reading answer different questions, and their median pooled
    together is a number that describes no population. Rows marked no_data are
    excluded entirely: a 0.0 written because the pool was unindexed is not a
    -100% outcome.
    """
    groups: dict[str, list[float]] = {}
    for row in rows:
        src = row.get(src_column)
        if src == SRC_PINNED_NO_DATA:
            continue
        ret = _pct_return(_entry_for(row, src), row.get(column))
        if ret is None:
            continue
        label = src or "legacy_unmarked"
        groups.setdefault(label, []).append(ret)
    return groups


# Below this count a group's win rate and median are printed with a marker
# rather than presented as a result. Groups this small are also drawn from a
# different time window than the large legacy groups beside them, so the two
# are not a comparison even when they sit on adjacent lines.
_MIN_GROUP_FOR_RESULT = 30


def _channel_summary(rows: list[dict]) -> str:
    lines = [f"Alerts: **{len(rows)}**"]

    for column, label, src_column in (
        ("price_1h", "1h", "price_1h_src"),
        ("price_24h", "24h", "price_24h_src"),
    ):
        groups = _slot_stats(rows, column, src_column)
        for method in sorted(groups):
            values = groups[method]
            wins = sum(1 for r in values if r > 0)
            note = "" if len(values) >= _MIN_GROUP_FOR_RESULT else " ⚠ too few"
            lines.append(
                f"{label} [{method}]: win **{wins / len(values):.0%}** | "
                f"median **{statistics.median(values):+.1f}%** "
                f"({len(values)}){note}"
            )

    rugs = sum(
        1 for x in rows
        if x.get("price_24h") is not None
        and x.get("price_24h_src") != SRC_PINNED_NO_DATA
        and (_entry_for(x, x.get("price_24h_src")) or 0) > 0
        and x["price_24h"] <= _entry_for(x, x.get("price_24h_src")) * RUG_THRESHOLD
    )
    if rugs:
        lines.append(f"Rugged (24h): {rugs}")

    if len(lines) == 1:
        lines.append("No measurements completed yet")

    return "\n".join(lines)


async def send_performance_report(session: aiohttp.ClientSession):
    """
    Aggregate last N days of alerts per channel and post a report.
    Sends unconditionally — the cadence gate lives in
    maybe_send_performance_report. Kept public so it can be triggered manually
    (e.g. from a one-off script) to force a report on demand.
    """
    rows = db.performance_rows(REPORT_DAYS)

    if not rows:
        print("[perf] No alert data yet — skipping report")
        return

    by_channel: dict[str, list[dict]] = {}
    for row in rows:
        by_channel.setdefault(row["channel"], []).append(row)

    print(f"[perf] Report: {len(rows)} alerts across {len(by_channel)} channels (last {REPORT_DAYS}d)")

    fields = []
    for channel in sorted(by_channel.keys()):
        channel_rows = by_channel[channel]
        summary = _channel_summary(channel_rows)
        fields.append({
            "name": f"📊 #{channel}",
            "value": summary,
            "inline": False,
        })
        print(f"[perf]   {channel}: {summary.replace(chr(10), ' | ').replace('**', '')}")

    if not WEBHOOK_PERFORMANCE:
        print("[perf] DISCORD_WEBHOOK_PERFORMANCE not set — report logged only")
        return

    embed = {
        "embeds": [{
            "title": f"📈 PERFORMANCE REPORT — last {REPORT_DAYS} days",
            "description": (
                "Returns are grouped by measurement method. Groups are NOT "
                "comparable to each other: they cover different rows and "
                "different time windows, so a gap between two lines is not "
                "evidence that one method reads higher than the other."
            ),
            "color": 0x3498DB,
            "fields": fields[:25],
            "footer": {"text": "Trench Scanner • Performance Tracker"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }

    async with session.post(WEBHOOK_PERFORMANCE, json=embed) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[perf] Discord send error: {resp.status} {text}")


async def maybe_send_performance_report(session: aiohttp.ClientSession):
    """
    Restart-proof report gate. Sends a report only when REPORT_INTERVAL_SECONDS
    have elapsed since the last one (timestamp persisted in DB, so restarts
    never reset the clock). Cheap to poll frequently — the scanner calls this on
    a short interval and this decides whether a report is actually due. On the
    very first call (no stored timestamp) it sends immediately, since there is
    already accumulated data to report.
    """
    last_ts = db.kv_get_json(_LAST_REPORT_KEY, 0) or 0
    now = time.time()

    if last_ts and (now - last_ts) < REPORT_INTERVAL_SECONDS:
        return  # not due yet

    await send_performance_report(session)
    db.kv_set_json(_LAST_REPORT_KEY, now)