# analyze_alerts.py
# One-shot READ-ONLY analysis of the alerts table.
# Answers three questions from real historical data instead of guesswork:
#   1. Per channel: how does performance vary by liquidity-at-alert band?
#      (finds the liquidity floor below which a channel is pure loss)
#   2. Confluence: do tokens alerted by >=2 different channels within a
#      short window outperform single-channel tokens?
#   3. Score vs outcome for alpha/gems: does the scoring model correlate
#      with returns at all?
#
# Opens the database in read-only mode (URI mode=ro) — safe to run while
# the live scanner is writing. Prints plain-text tables to stdout.
#
# Usage:  python analyze_alerts.py [days]
#         days = lookback window, default 30 (0 = entire table)

import os
import sqlite3
import statistics
import sys
import time

try:
    from dotenv import load_dotenv
    load_dotenv()  # only needed if SCANNER_DB_PATH lives in .env
except ImportError:
    pass

DB_PATH = os.environ.get("SCANNER_DB_PATH", "trench_scanner.db")

# Liquidity bands in USD. Upper bound is exclusive.
LIQUIDITY_BANDS = [
    (0, 10_000, "<10k"),
    (10_000, 25_000, "10-25k"),
    (25_000, 50_000, "25-50k"),
    (50_000, 100_000, "50-100k"),
    (100_000, 250_000, "100-250k"),
    (250_000, float("inf"), ">250k"),
]

CONFLUENCE_WINDOW_SECONDS = 3600  # two channels within 60 min = confluence
RUG_THRESHOLD = 0.10  # price_24h <= 10% of entry counts as a rug

SCORE_BUCKETS = [
    (0, 50, "<50"),
    (50, 60, "50-59"),
    (60, 70, "60-69"),
    (70, 200, "70+"),
]


def _connect_readonly(path: str) -> sqlite3.Connection:
    """Open the SQLite database strictly read-only."""
    uri = f"file:{path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _fetch_rows(conn: sqlite3.Connection, days: int) -> list[dict]:
    query = (
        "SELECT id, address, symbol, channel, score, liquidity_at_alert, "
        "price_at_alert, alerted_at, price_1h, price_6h, price_24h "
        "FROM alerts"
    )
    params: tuple = ()
    if days > 0:
        query += " WHERE alerted_at >= ?"
        params = (time.time() - days * 86_400,)
    cursor = conn.execute(query, params)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _return_pct(entry: float, later: float | None) -> float | None:
    """Percent return from entry to a later measurement. None = unmeasured."""
    if later is None or not entry or entry <= 0:
        return None
    return (later - entry) / entry * 100.0


def _band_label(liquidity: float | None) -> str:
    if liquidity is None:
        return "unknown"
    for low, high, label in LIQUIDITY_BANDS:
        if low <= liquidity < high:
            return label
    return "unknown"


def _summarize(returns_24h: list[float], returns_1h: list[float]) -> dict:
    """Aggregate stats over measured returns."""
    out = {
        "n_24h": len(returns_24h),
        "win_24h": None,
        "median_24h": None,
        "rug_rate": None,
        "n_1h": len(returns_1h),
        "median_1h": None,
    }
    if returns_24h:
        wins = sum(1 for r in returns_24h if r > 0)
        rugs = sum(1 for r in returns_24h if r <= (RUG_THRESHOLD * 100 - 100))
        out["win_24h"] = wins / len(returns_24h) * 100.0
        out["median_24h"] = statistics.median(returns_24h)
        out["rug_rate"] = rugs / len(returns_24h) * 100.0
    if returns_1h:
        out["median_1h"] = statistics.median(returns_1h)
    return out


def _fmt(value: float | None, suffix: str = "%") -> str:
    if value is None:
        return "n/a"
    return f"{value:+.1f}{suffix}" if suffix == "%" else f"{value:.1f}{suffix}"


def _print_table(title: str, headers: list[str], rows: list[list[str]]):
    print(f"\n=== {title} ===")
    if not rows:
        print("(no data)")
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    fmt_row = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt_row.format(*headers))
    print(fmt_row.format(*["-" * w for w in widths]))
    for row in rows:
        print(fmt_row.format(*row))


# === Analysis 1: channel x liquidity band ===

def analyze_liquidity_bands(rows: list[dict]):
    grouped: dict[tuple[str, str], dict[str, list[float]]] = {}
    for row in rows:
        band = _band_label(row["liquidity_at_alert"])
        key = (row["channel"], band)
        bucket = grouped.setdefault(key, {"r24": [], "r1": []})
        r24 = _return_pct(row["price_at_alert"], row["price_24h"])
        r1 = _return_pct(row["price_at_alert"], row["price_1h"])
        if r24 is not None:
            bucket["r24"].append(r24)
        if r1 is not None:
            bucket["r1"].append(r1)

    band_order = [label for _, _, label in LIQUIDITY_BANDS] + ["unknown"]
    channels = sorted({ch for ch, _ in grouped})

    table_rows = []
    for channel in channels:
        for band in band_order:
            bucket = grouped.get((channel, band))
            if not bucket or not bucket["r24"]:
                continue
            stats = _summarize(bucket["r24"], bucket["r1"])
            table_rows.append([
                channel,
                band,
                str(stats["n_24h"]),
                _fmt(stats["win_24h"]),
                _fmt(stats["median_24h"]),
                _fmt(stats["rug_rate"]),
                _fmt(stats["median_1h"]),
            ])
    _print_table(
        "Performance by channel x liquidity band (measured 24h alerts only)",
        ["channel", "liq band", "n", "win 24h", "med 24h", "rug rate", "med 1h"],
        table_rows,
    )


# === Analysis 2: confluence (>=2 channels within the window) ===

def analyze_confluence(rows: list[dict]):
    by_address: dict[str, list[dict]] = {}
    for row in rows:
        by_address.setdefault(row["address"], []).append(row)

    confluence_first_alerts: list[dict] = []
    single_first_alerts: list[dict] = []
    combo_counts: dict[str, int] = {}

    for alerts in by_address.values():
        alerts.sort(key=lambda r: r["alerted_at"])
        is_confluence = False
        channels_in_window: set[str] = set()
        for i, a in enumerate(alerts):
            for b in alerts[i + 1:]:
                if b["alerted_at"] - a["alerted_at"] > CONFLUENCE_WINDOW_SECONDS:
                    break
                if b["channel"] != a["channel"]:
                    is_confluence = True
                    channels_in_window.add(a["channel"])
                    channels_in_window.add(b["channel"])
        first = alerts[0]
        if is_confluence:
            confluence_first_alerts.append(first)
            combo = "+".join(sorted(channels_in_window))
            combo_counts[combo] = combo_counts.get(combo, 0) + 1
        else:
            single_first_alerts.append(first)

    def collect(alert_rows: list[dict]) -> tuple[list[float], list[float]]:
        r24s, r1s = [], []
        for row in alert_rows:
            r24 = _return_pct(row["price_at_alert"], row["price_24h"])
            r1 = _return_pct(row["price_at_alert"], row["price_1h"])
            if r24 is not None:
                r24s.append(r24)
            if r1 is not None:
                r1s.append(r1)
        return r24s, r1s

    conf_stats = _summarize(*collect(confluence_first_alerts))
    single_stats = _summarize(*collect(single_first_alerts))

    _print_table(
        f"Confluence (>=2 channels within {CONFLUENCE_WINDOW_SECONDS // 60} min) "
        "vs single-channel — stats on FIRST alert per token",
        ["group", "tokens", "n 24h", "win 24h", "med 24h", "rug rate", "med 1h"],
        [
            [
                "confluence",
                str(len(confluence_first_alerts)),
                str(conf_stats["n_24h"]),
                _fmt(conf_stats["win_24h"]),
                _fmt(conf_stats["median_24h"]),
                _fmt(conf_stats["rug_rate"]),
                _fmt(conf_stats["median_1h"]),
            ],
            [
                "single",
                str(len(single_first_alerts)),
                str(single_stats["n_24h"]),
                _fmt(single_stats["win_24h"]),
                _fmt(single_stats["median_24h"]),
                _fmt(single_stats["rug_rate"]),
                _fmt(single_stats["med_1h"] if "med_1h" in single_stats else single_stats["median_1h"]),
            ],
        ],
    )

    combo_rows = [
        [combo, str(count)]
        for combo, count in sorted(combo_counts.items(), key=lambda x: -x[1])
    ]
    _print_table("Confluence channel combinations", ["combo", "tokens"], combo_rows)


# === Analysis 3: score vs outcome (alpha + gems only) ===

def analyze_score_correlation(rows: list[dict]):
    scored = [
        r for r in rows
        if r["channel"] in ("alpha", "gems") and r["score"] is not None
    ]
    grouped: dict[str, dict[str, list[float]]] = {}
    for row in scored:
        label = None
        for low, high, lbl in SCORE_BUCKETS:
            if low <= row["score"] < high:
                label = lbl
                break
        if label is None:
            continue
        bucket = grouped.setdefault(label, {"r24": [], "r1": []})
        r24 = _return_pct(row["price_at_alert"], row["price_24h"])
        r1 = _return_pct(row["price_at_alert"], row["price_1h"])
        if r24 is not None:
            bucket["r24"].append(r24)
        if r1 is not None:
            bucket["r1"].append(r1)

    table_rows = []
    for _, _, label in SCORE_BUCKETS:
        bucket = grouped.get(label)
        if not bucket or not bucket["r24"]:
            continue
        stats = _summarize(bucket["r24"], bucket["r1"])
        table_rows.append([
            label,
            str(stats["n_24h"]),
            _fmt(stats["win_24h"]),
            _fmt(stats["median_24h"]),
            _fmt(stats["rug_rate"]),
        ])
    _print_table(
        "Score vs outcome (alpha + gems channels, n is small — treat as directional)",
        ["score", "n 24h", "win 24h", "med 24h", "rug rate"],
        table_rows,
    )


def main():
    days = 30
    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            print(f"Invalid days argument: {sys.argv[1]!r}, using default 30")

    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    conn = _connect_readonly(DB_PATH)
    try:
        rows = _fetch_rows(conn, days)
    finally:
        conn.close()

    window = f"last {days} days" if days > 0 else "entire table"
    measured = sum(1 for r in rows if r["price_24h"] is not None)
    print(f"Loaded {len(rows)} alerts ({window}), "
          f"{measured} with a 24h measurement.")

    analyze_liquidity_bands(rows)
    analyze_confluence(rows)
    analyze_score_correlation(rows)

    print("\nDone. Reminder: 'unknown' liquidity band = alerts recorded "
          "without liquidity_at_alert (mostly older rows or channels that "
          "never pass liquidity).")


if __name__ == "__main__":
    main()