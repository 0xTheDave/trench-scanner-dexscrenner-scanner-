#!/usr/bin/env python3
# audit/diag_channel_yield.py
#
# READ-ONLY. Opens the scanner DB with mode=ro and makes no network calls of
# any kind. Safe to run while the scanner is live.
#
# QUESTION IT ANSWERS: the [gecko] by-source line says where the shared request
# budget GOES. It says nothing about what each consumer BUYS. This script puts
# the other half on the table — alerts produced and, where measured, how those
# alerts turned out — so a cut is aimed at the consumer with the worst cost per
# alert rather than the biggest number.
#
# WHAT IT CANNOT ANSWER, stated up front rather than approximated:
#   - Which module a shared Gecko bucket belongs to. `solana:ohlcv` mixes every
#     consumer that draws a chart on solana. The COST_PER_HOUR table below marks
#     such mappings ASSUMED, and an assumed mapping must not be quoted as
#     measured.
#   - Whether a migration alert actually rendered a chart, unless the schema
#     records it. The script checks for such a column and says plainly when
#     there is none; the answer then has to come from the logs, not from here.
#
# THREE MEASUREMENT TRAPS THIS GUARDS AGAINST:
#   1. Alerts-per-hour over wall-clock time silently counts hours when the
#      scanner was down as hours that produced nothing. Rates are computed over
#      ACTIVE hours (hours in which any channel produced any alert), which is a
#      proxy for uptime and is labelled as one.
#   2. A channel added recently looks low-yield because it is young. First-seen
#      and last-seen timestamps are printed per channel so age is visible.
#   3. A median over a handful of rows is noise. Groups below MIN_GROUP are
#      printed with a marker and must not be read as results.
#
# Usage:
#   python audit/diag_channel_yield.py                 # last 7 days
#   python audit/diag_channel_yield.py --days 14
#   python audit/diag_channel_yield.py --db F:\trench-scanner\trench_scanner.db

import argparse
import os
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone

# Measured 2026-09-13 from the [gecko] by-source line: 493 slots over 132 min,
# ~224 slots/hour total. Slots, not logical calls — a 429 retry is counted
# because it spends the budget twice.
#
# `channels` maps a bucket to the scanner channels it serves. 'certain' means
# the URL shape identifies the consumer on its own; ASSUMED means it does not
# and the mapping is inference from the call sites, to be treated as such.
COST_PER_HOUR = {
    # bucket:                  (slots/h, 429s, 404s, channels, certain?)
    "new_pools+trending_pools": (98.6, 14, 0, ["multichain"], True),
    "solana:ohlcv":             (50.5, 15, 7, ["migrations"], False),
    "solana:token_pools":       (30.9,  8, 0, ["jupiter"], False),
    "robinhood:ohlcv:bounded":  (24.1,  6, 0, ["<performance_tracker>"], True),
    "robinhood:token_pools":    ( 9.5,  2, 0, ["robinhood"], True),
    "robinhood:ohlcv":          ( 9.1,  3, 0, ["robinhood"], True),
}
MEASURED_TOTAL_PER_HOUR = 224.0

# Source markers are DISCOVERED from the DB, never hardcoded. A first version of
# this script assumed the pinned marker was "gecko_pinned" while performance_tracker
# writes "gecko_pinned_ohlcv"; the match failed silently, every pinned row fell back
# to the DexScreener entry, and the output looked plausible while dividing a pinned
# exit by a cross-venue entry. A wrong constant produces a wrong ANSWER, not an
# error, so the only safe source of these strings is the data itself.
PINNED_PREFIX = "gecko_pinned"
NO_DATA_SUFFIX = "nodata"
STALE_MARK = "stale"


def is_pinned_src(src: str | None) -> bool:
    """Pinned exit: entry must come from entry_price_pinned, never price_at_alert."""
    return bool(src) and src.startswith(PINNED_PREFIX) and not src.endswith(NO_DATA_SUFFIX)


def is_nodata_src(src: str | None) -> bool:
    """0.0 written because the pool was unindexed. Not a -100% outcome."""
    return bool(src) and src.endswith(NO_DATA_SUFFIX)

# Below this, a group's win rate and median are noise and are flagged as such.
MIN_GROUP = 30

# Columns that would record whether an alert carried a chart, if any exists.
CHART_COLUMN_CANDIDATES = ("has_chart", "chart", "chart_sent", "with_chart")


def resolve_db_path(explicit: str | None) -> str:
    """--db wins, then SCANNER_DB_PATH, then the default filename."""
    if explicit:
        return explicit
    return os.environ.get("SCANNER_DB_PATH", "trench_scanner.db")


def open_readonly(path: str) -> sqlite3.Connection:
    """
    Open the DB strictly read-only. mode=ro is enforced by SQLite itself, so a
    stray write in this script fails loudly instead of touching live data.
    """
    if not os.path.exists(path):
        sys.exit(f"DB not found: {path}\n"
                 f"Pass --db or set SCANNER_DB_PATH.")
    uri = f"file:{os.path.abspath(path).replace(os.sep, '/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def columns_of(conn: sqlite3.Connection, table: str) -> list[str]:
    """
    Live schema, not an assumed one. Every query below is built against what
    this returns, because a hardcoded column list goes stale the moment the
    schema moves and then fails as a wrong answer rather than an error.
    """
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]


def fmt_ts(ts: float | None) -> str:
    if not ts:
        return "n/a"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


def active_hours(conn: sqlite3.Connection, since: float) -> int:
    """
    Distinct clock hours in which ANY alert was recorded, as an uptime proxy.

    A proxy, not a measurement: a quiet hour with the scanner up looks the same
    as an hour with the scanner down. It errs toward UNDERSTATING elapsed time,
    which makes every per-hour rate below an upper bound on the true rate.
    """
    row = conn.execute(
        "SELECT COUNT(DISTINCT CAST(alerted_at / 3600 AS INTEGER)) AS h "
        "FROM alerts WHERE alerted_at >= ?",
        (since,),
    ).fetchone()
    return int(row["h"] or 0)


def channel_volume(conn: sqlite3.Connection, since: float) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT channel, COUNT(*) AS n, "
        "       MIN(alerted_at) AS first_ts, MAX(alerted_at) AS last_ts "
        "FROM alerts WHERE alerted_at >= ? "
        "GROUP BY channel ORDER BY n DESC",
        (since,),
    ).fetchall()


def entry_for(row: sqlite3.Row, src: str | None, cols: list[str]) -> float | None:
    """
    The entry that matches how the exit was measured. A pinned exit divided by
    a DexScreener entry re-creates the cross-venue gap the pinned path exists
    to remove, so a row whose matching entry is missing is DROPPED rather than
    paired with the other one.
    """
    if is_pinned_src(src) and "entry_price_pinned" in cols:
        v = row["entry_price_pinned"]
        return v if v and v > 0 else None
    v = row["price_at_alert"] if "price_at_alert" in cols else None
    return v if v and v > 0 else None


def outcomes(conn: sqlite3.Connection, since: float, cols: list[str],
             column: str, src_column: str) -> dict[str, dict[str, list[tuple[float, str]]]]:
    """
    Returns {channel: {src: [(pct return, symbol)]}} for one measurement slot.
    Grouped by src because the two methods answer different questions and their
    pooled median describes no population. The symbol rides along so the tail
    section can name the rows driving a mean, which is the only way to tell a
    real 10x from a division by a near-zero entry.
    """
    select = ["channel", column, "price_at_alert"]
    if "symbol" in cols:
        select.append("symbol")
    if src_column in cols:
        select.append(src_column)
    if "entry_price_pinned" in cols:
        select.append("entry_price_pinned")

    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM alerts "
        f"WHERE alerted_at >= ? AND {column} IS NOT NULL",
        (since,),
    ).fetchall()

    out: dict[str, dict[str, list[float]]] = {}
    for row in rows:
        src = row[src_column] if src_column in select else None
        if is_nodata_src(src):
            continue  # a 0.0 written because the pool was unindexed is not -100%
        entry = entry_for(row, src, cols)
        price = row[column]
        if not entry or price is None:
            continue
        ret = (price / entry - 1) * 100
        symbol = row["symbol"] if "symbol" in select else "?"
        out.setdefault(row["channel"], {}).setdefault(src or "legacy_unmarked", []).append(
            (ret, symbol or "?"))
    return out


def percentile(sorted_vals: list[float], q: float) -> float:
    """
    Linear-interpolated percentile. Written out rather than taken from
    statistics.quantiles because that helper needs n>=2 and silently changes
    what it reports with the `method` argument; here the definition is visible.
    """
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def print_tails(title: str, grouped: dict[str, dict[str, list[tuple[float, str]]]]):
    """
    Does any group have a tail worth paying for?

    MEAN is the expectancy of an equal-size bet on every alert in the group, so
    it — not the median — is the number that decides whether a fat right tail
    pays for a mass of -90% rows.

    mean-top1 is the same mean with the single best row removed. If the sign
    flips between the two, the group's positive expectancy rests on ONE row and
    is a lottery ticket, not a distribution. A mean that survives this is still
    only as good as the entry prices behind it, which is why the top rows are
    named below the table.
    """
    print(f"\n  {title}")
    if not grouped:
        print("    (no measurements in window)")
        return
    print(f"    {'channel':<12} {'src':<20} {'n':>5} {'mean':>9} {'p75':>8} "
          f"{'p90':>8} {'p95':>9} {'max':>10} {'>=2x':>6} {'>=10x':>6} {'mean-top1':>10}")
    named: list[str] = []
    for channel in sorted(grouped):
        for src in sorted(grouped[channel]):
            rows = grouped[channel][src]
            vals = sorted(r for r, _s in rows)
            n = len(vals)
            mean = sum(vals) / n
            ex_top = (sum(vals[:-1]) / (n - 1)) if n > 1 else float("nan")
            hit2 = sum(1 for v in vals if v >= 100) / n
            hit10 = sum(1 for v in vals if v >= 900) / n
            flag = "" if n >= MIN_GROUP else "  <- n too small"
            print(f"    {channel:<12} {src:<20} {n:>5} {mean:>+8.1f}% "
                  f"{percentile(vals, 0.75):>+7.1f}% {percentile(vals, 0.90):>+7.1f}% "
                  f"{percentile(vals, 0.95):>+8.1f}% {vals[-1]:>+9.1f}% "
                  f"{hit2:>5.1%} {hit10:>5.1%} {ex_top:>+9.1f}%{flag}")
            if n >= MIN_GROUP and vals[-1] >= 100:
                top = sorted(rows, key=lambda rs: rs[0], reverse=True)[:3]
                named.append(f"    {channel}/{src}: " +
                             ", ".join(f"{sym} {r:+.0f}%" for r, sym in top))
    if named:
        print("\n    top rows behind each tail (check these are real, not a "
              "near-zero entry):")
        for line in named:
            print(line)


def print_outcomes(title: str, grouped: dict[str, dict[str, list[float]]]):
    print(f"\n  {title}")
    if not grouped:
        print("    (no measurements in window)")
        return
    for channel in sorted(grouped):
        for src in sorted(grouped[channel]):
            vals = [r for r, _sym in grouped[channel][src]]
            wins = sum(1 for v in vals if v > 0)
            flag = "" if len(vals) >= MIN_GROUP else "  <- n too small, not a result"
            print(f"    {channel:<12} [{src:<20}] "
                  f"n={len(vals):<5} win={wins / len(vals):>4.0%} "
                  f"median={statistics.median(vals):+8.1f}%{flag}")


def entry_disagreement(conn: sqlite3.Connection, since: float, cols: list[str]):
    """
    How far apart are the two entry prices on rows that have BOTH?

    ratio = price_at_alert / entry_price_pinned. A ratio far below 1 means the
    DexScreener snapshot was taken at a price the pinned pool never traded at,
    which is the mechanism that turns a legacy return into +86,836,099,869%:
    divide by a near-zero entry and arithmetic does the rest.

    SCOPE, and it is narrow: entry_price_pinned exists only on rows the pinned
    path has touched, i.e. robinhood. This section PROVES the mechanism where
    both numbers exist. It does not measure it on migrations or jupiter, whose
    absurd tails are consistent with the same cause but are not shown to have
    it by anything here.
    """
    if "entry_price_pinned" not in cols or "price_at_alert" not in cols:
        print("  Needs both price_at_alert and entry_price_pinned. Not present.")
        return
    rows = conn.execute(
        "SELECT symbol, channel, price_at_alert AS dex, entry_price_pinned AS pinned "
        "FROM alerts WHERE alerted_at >= ? AND entry_price_pinned > 0 AND price_at_alert > 0",
        (since,),
    ).fetchall()
    if not rows:
        print("  No rows carry both entries in this window.")
        return

    ratios = sorted((r["dex"] / r["pinned"], r["symbol"] or "?") for r in rows)
    vals = [x for x, _s in ratios]
    n = len(vals)
    print(f"  rows with both entries: {n} "
          f"(channels: {', '.join(sorted({r['channel'] for r in rows}))})")
    print(f"  ratio price_at_alert / entry_price_pinned")
    for label, q in (("p01", 0.01), ("p10", 0.10), ("p50", 0.50),
                     ("p90", 0.90), ("p99", 0.99)):
        print(f"    {label}: {percentile(vals, q):>12.4f}")
    print(f"    min: {vals[0]:>12.6f}    max: {vals[-1]:>12.2f}")

    for lo, hi, what in ((0.0, 0.5, "dex entry <50% of pinned (inflates legacy return)"),
                         (2.0, float('inf'), "dex entry >2x pinned (deflates legacy return)"),
                         (0.0, 0.1, "dex entry <10% of pinned (order-of-magnitude wrong)")):
        k = sum(1 for v in vals if lo <= v < hi)
        print(f"    {k:>5} rows ({k / n:>5.1%})  {what}")

    worst = ratios[:3]
    print("    most extreme low ratios: " +
          ", ".join(f"{sym} {v:.6f}" for v, sym in worst))


HORIZONS = (("1h ", "price_1h", "price_1h_src"),
            ("6h ", "price_6h", "price_6h_src"),
            ("24h", "price_24h", "price_24h_src"))


def paired_horizons(conn: sqlite3.Connection, since: float, cols: list[str]):
    """
    THE SAME ROWS at 1h, 6h and 24h, all three computed from entry_price_pinned.

    Why this section exists: the per-slot groups elsewhere in this script are
    different populations — the 24h slot only ever reaches alerts older than a
    day — so "the edge is at 1h and gone by 24h" could just as easily be two
    different market windows. Restricting to rows measured at ALL THREE marks
    removes that explanation. Three points also separate a straight-line decay
    (no exit moment exists, the channel is simply negative) from an edge that
    survives to 6h (a window wide enough to build a rule on).

    TWO SELECTION EFFECTS, neither of which this section removes:
      1. Every row here is at least a day old, so it describes last week.
      2. `clean only` requires the pool to still be trading at all three marks,
         which conditions on information from after the entry. It is not a
         tradable subset and the gap between it and `all pinned` is partly that
         filter rather than the market.

    REPORTED BOTH WAYS ON PURPOSE. `stale` inflates the late marks: the last
    candle before a pool died predates the collapse, so a dead token is scored
    at its pre-collapse price. Measured 2026-09-13 at the 24h slot: clean mean
    -50.0%, stale mean +5.8%, while the two MEDIANS were identical (-86.9%).
    The median cannot see this because both groups pile up near -87% and differ
    only in the tail, which is why every horizon below prints mean, median and
    mean-top1 side by side.

    `best` is the share of rows on which that horizon was the row's own best
    outcome. It is a rank statistic, so unlike the mean it cannot be moved by
    one broken entry, and it is the closest thing here to "when should the exit
    be".
    """
    needed = [c for _l, c, sc in HORIZONS for c in (c, sc)] + ["entry_price_pinned"]
    missing = [c for c in needed if c not in cols]
    if missing:
        print(f"  Missing columns: {', '.join(missing)}")
        return

    where = " AND ".join(f"{c} IS NOT NULL" for _l, c, _sc in HORIZONS)
    rows = conn.execute(
        "SELECT symbol, entry_price_pinned AS entry, "
        + ", ".join(f"{c}, {sc}" for _l, c, sc in HORIZONS) +
        f" FROM alerts WHERE alerted_at >= ? AND entry_price_pinned > 0 AND {where}",
        (since,),
    ).fetchall()

    all_pinned, clean = [], []
    for r in rows:
        srcs = [r[sc] for _l, _c, sc in HORIZONS]
        if not all(is_pinned_src(x) for x in srcs):
            continue
        rets = [(r[c] / r["entry"] - 1) * 100 for _l, c, _sc in HORIZONS]
        item = (rets, r["symbol"] or "?")
        all_pinned.append(item)
        if not any(STALE_MARK in (x or "") for x in srcs):
            clean.append(item)

    for label, data in (("all pinned rows", all_pinned), ("clean only (no stale at any mark)", clean)):
        n = len(data)
        print(f"\n  {label}: n={n}" + ("" if n >= MIN_GROUP else "   <- n too small, not a result"))
        if not n:
            continue
        for idx, (horizon, _c, _sc) in enumerate(HORIZONS):
            vals = sorted(d[0][idx] for d in data)
            mean = sum(vals) / n
            ex_top = (sum(vals[:-1]) / (n - 1)) if n > 1 else float("nan")
            win = sum(1 for v in vals if v > 0) / n
            # Ties credit the EARLIEST horizon only. A row whose 6h and 24h
            # marks are the same candle (a dead pool measured twice) would
            # otherwise be counted as "best" at both, and the column summed
            # past 100%. If selling earlier gets the same price, earlier wins:
            # less time exposed for the same outcome.
            best = sum(1 for rets, _s in data
                       if rets[idx] == max(rets) and max(rets) not in rets[:idx]) / n
            print(f"    {horizon}  mean={mean:>+9.1f}%  median={percentile(vals, 0.5):>+8.1f}%  "
                  f"p90={percentile(vals, 0.90):>+9.1f}%  win={win:>4.0%}  "
                  f"best={best:>4.0%}  mean-top1={ex_top:>+9.1f}%")

        # Name the rows driving each horizon's mean. Without this the 6h mean
        # can read +459% on a group whose median is -76%, and nothing in the
        # output says which row did it or whether its candle is believable.
        for idx, (horizon, _c, _sc) in enumerate(HORIZONS):
            ranked = sorted(data, key=lambda d: d[0][idx], reverse=True)[:3]
            if ranked and ranked[0][0][idx] >= 100:
                print(f"    top {horizon.strip()} rows: " +
                      ", ".join(f"{sym} {rets[idx]:+.0f}%" for rets, sym in ranked))

        # Pairwise: does selling earlier beat holding on longer?
        for a, b in ((0, 1), (1, 2), (0, 2)):
            la, lb = HORIZONS[a][0].strip(), HORIZONS[b][0].strip()
            beat = sum(1 for rets, _s in data if rets[a] > rets[b]) / n
            deltas = sorted(rets[a] - rets[b] for rets, _s in data)
            print(f"    selling at {la:<3} beat holding to {lb:<3} on {beat:>4.0%} of rows; "
                  f"median difference {percentile(deltas, 0.5):>+7.1f}pp")

        # Straight line, or a plateau? Compare the 6h median against the
        # midpoint of the 1h and 24h medians. Medians, because this question is
        # about where the mass sits, not where the tail is.
        meds = [percentile(sorted(d[0][i] for d in data), 0.5) for i in range(3)]
        midpoint = (meds[0] + meds[2]) / 2
        gap = meds[1] - midpoint
        shape = ("closer to 1h — the window is wider than an hour" if gap > 5
                 else "closer to 24h — most of the decay happens early" if gap < -5
                 else "on the straight line between them — no plateau, no exit moment")
        print(f"    6h median {meds[1]:+.1f}% vs midpoint of 1h/24h {midpoint:+.1f}% "
              f"({gap:+.1f}pp): {shape}")


def main():
    ap = argparse.ArgumentParser(description="Per-channel alert yield vs measured Gecko cost (read-only).")
    ap.add_argument("--db", default=None, help="path to the scanner DB")
    ap.add_argument("--days", type=float, default=7.0, help="window in days (default 7)")
    args = ap.parse_args()

    path = resolve_db_path(args.db)
    conn = open_readonly(path)
    since = time.time() - args.days * 86400

    cols = columns_of(conn, "alerts")
    if not cols:
        sys.exit("No 'alerts' table in this DB — wrong file?")
    for required in ("channel", "alerted_at"):
        if required not in cols:
            sys.exit(f"'alerts' has no '{required}' column. Found: {', '.join(cols)}")

    print("=" * 78)
    print(f"CHANNEL YIELD — {path}")
    print(f"window: last {args.days:g} days (since {fmt_ts(since)} UTC)")
    print("=" * 78)
    print(f"\nalerts columns ({len(cols)}): {', '.join(cols)}")

    # Inventory of the markers actually present, printed before anything is
    # computed from them. If a marker here is not recognised as pinned by
    # is_pinned_src, its rows are being divided by the wrong entry.
    src_cols = [c for c in ("price_1h_src", "price_6h_src", "price_24h_src") if c in cols]
    if src_cols:
        print("\nmeasurement markers found in this DB:")
        seen: dict[str, int] = {}
        for sc in src_cols:
            for r in conn.execute(
                f"SELECT {sc} AS s, COUNT(*) AS n FROM alerts "
                f"WHERE alerted_at >= ? AND {sc} IS NOT NULL GROUP BY {sc}", (since,)
            ):
                seen[r["s"]] = seen.get(r["s"], 0) + r["n"]
        for marker, n in sorted(seen.items(), key=lambda kv: -kv[1]):
            if is_nodata_src(marker):
                kind = "no-data (excluded)"
            elif is_pinned_src(marker):
                kind = "PINNED -> entry_price_pinned" + ("  [stale]" if STALE_MARK in marker else "")
            else:
                kind = "legacy -> price_at_alert"
            print(f"  {marker:<24} n={n:<6} {kind}")

    hours = active_hours(conn, since)
    print(f"\nactive hours in window: {hours} "
          f"(hours with >=1 alert on any channel — an uptime PROXY, so every "
          f"rate below is an upper bound)")
    if hours == 0:
        sys.exit("No alerts in the window. Widen --days.")

    # --- volume ------------------------------------------------------------
    print("\n" + "-" * 78)
    print("ALERTS PER CHANNEL")
    print("-" * 78)
    print(f"  {'channel':<14} {'alerts':>7} {'per hour':>9}   {'first':<17} {'last':<17}")
    volume = channel_volume(conn, since)
    rate_by_channel: dict[str, float] = {}
    for row in volume:
        rate = row["n"] / hours
        rate_by_channel[row["channel"]] = rate
        print(f"  {row['channel']:<14} {row['n']:>7} {rate:>9.2f}   "
              f"{fmt_ts(row['first_ts']):<17} {fmt_ts(row['last_ts']):<17}")

    # --- cost vs yield -----------------------------------------------------
    print("\n" + "-" * 78)
    print("MEASURED GECKO COST vs ALERTS BOUGHT")
    print(f"(cost from the [gecko] by-source line, 493 slots / 132 min, "
          f"~{MEASURED_TOTAL_PER_HOUR:.0f} slots/h total)")
    print("-" * 78)
    print(f"  {'bucket':<26} {'slots/h':>8} {'share':>7} {'429':>5} {'404':>5} "
          f"{'alerts/h':>9} {'slots/alert':>12}  mapping")
    for bucket, (sph, r429, r404, channels, certain) in COST_PER_HOUR.items():
        share = sph / MEASURED_TOTAL_PER_HOUR
        alerts_h = sum(rate_by_channel.get(c, 0.0) for c in channels)
        if alerts_h > 0:
            per_alert = f"{sph / alerts_h:>12.1f}"
        elif channels and channels[0].startswith("<"):
            per_alert = f"{'n/a':>12}"   # not an alert producer
        else:
            per_alert = f"{'no alerts':>12}"
        tag = "certain" if certain else "ASSUMED"
        print(f"  {bucket:<26} {sph:>8.1f} {share:>6.1%} {r429:>5} {r404:>5} "
              f"{alerts_h:>9.2f} {per_alert}  {tag}: {', '.join(channels)}")
    print("\n  A channel name that does not appear in the ALERTS table above "
          "shows as 'no alerts'.\n  Check the channel string the module writes "
          "before reading that as zero yield.")

    # --- outcomes ----------------------------------------------------------
    print("\n" + "-" * 78)
    print("MEASURED OUTCOMES PER CHANNEL, GROUPED BY METHOD")
    print("-" * 78)
    if "price_at_alert" not in cols:
        print("  'price_at_alert' missing — returns cannot be computed.")
    else:
        computed: list[tuple[str, dict]] = []
        for column, src_column, label in (
            ("price_1h", "price_1h_src", "1h"),
            ("price_24h", "price_24h_src", "24h"),
        ):
            if column not in cols:
                print(f"\n  {label}: no '{column}' column")
                continue
            grouped = outcomes(conn, since, cols, column, src_column)
            computed.append((label, grouped))
            print_outcomes(label, grouped)
        print("\n  Groups are NOT comparable across methods: different rows, "
              "different windows.")

        print("\n" + "-" * 78)
        print("TAIL: DOES ANY CHANNEL HAVE AN UPSIDE WORTH PAYING FOR?")
        print("-" * 78)
        for label, grouped in computed:
            print_tails(label, grouped)
        print("\n  mean is the expectancy of an equal-size bet on every alert in "
              "the group.\n  mean-top1 is the same mean without the single best "
              "row: a sign flip between\n  the two means the group's edge is one "
              "row, not a distribution.")

        print("\n" + "-" * 78)
        print("ARE THE TWO ENTRY PRICES EVEN THE SAME PRICE?")
        print("-" * 78)
        entry_disagreement(conn, since, cols)

        print("\n" + "-" * 78)
        print("SAME ROWS, ALL THREE HORIZONS (pinned entry on every side)")
        print("-" * 78)
        paired_horizons(conn, since, cols)

    # --- the chart question ------------------------------------------------
    print("\n" + "-" * 78)
    print("DID MIGRATION ALERTS ACTUALLY GET A CHART?")
    print("-" * 78)
    found = [c for c in CHART_COLUMN_CANDIDATES if c in cols]
    if found:
        col = found[0]
        rows = conn.execute(
            f"SELECT channel, {col} AS flag, COUNT(*) AS n FROM alerts "
            f"WHERE alerted_at >= ? GROUP BY channel, flag ORDER BY channel",
            (since,),
        ).fetchall()
        for row in rows:
            print(f"  {row['channel']:<14} {col}={str(row['flag']):<6} n={row['n']}")
    else:
        print("  The schema records nothing about charts "
              f"(looked for: {', '.join(CHART_COLUMN_CANDIDATES)}).")
        print("  This DB cannot answer it. The log can:")
        print("    Select-String -Path scanner.log -Pattern '\\[migration\\] . \\$' | Measure-Object")
        print("  and compare against the solana:ohlcv slot count for the same window.")
        print("  Do NOT infer the number from alert counts — a chart call can be "
              "made and\n  still produce no chart (404, too few candles).")

    conn.close()
    print("\nDone. Nothing was written and no API was called.")


if __name__ == "__main__":
    main()