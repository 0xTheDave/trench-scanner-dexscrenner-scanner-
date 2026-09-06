# db.py
# SQLite persistence layer: alert performance tracking + state that
# must survive restarts. Standard library only, single shared connection.
# Writes are tiny and infrequent — synchronous sqlite3 is fine here.

import json
import os
import sqlite3
import time

DB_PATH = os.environ.get("SCANNER_DB_PATH", "trench_scanner.db")

_PRICE_COLUMNS = ("price_1h", "price_6h", "price_24h")

# Columns added after the alerts table shipped. CREATE TABLE IF NOT EXISTS is a
# no-op on an existing database, so declaring them in the schema block alone
# would silently do nothing on any DB that already has rows — the writes would
# then fail or, worse, the reads would return NULL forever. These are applied
# explicitly via ALTER TABLE at connect time.
#
#   pool_address    the pool the price was read from, pinned at alert time.
#                   Robinhood tokens carry a WETH 0.01% pool alongside a swarm
#                   of 20-95% fee traps mass-deployed after the alert; measuring
#                   "the most liquid pool an hour later" measures a different
#                   venue than the one that was quoted. Median divergence on the
#                   sample was +111.6pp, with gaps past 1000pp in both directions.
#   pool_fee_pct    fee tier of that pool, parsed from its name. Kept for
#                   auditing which alerts passed the <=0.05% gate.
#   priced_at       when price_at_alert was actually observed upstream.
#   alert_sent_at   when the Discord webhook accepted the message.
#
# alert_sent_at minus priced_at is the pipeline latency: the delay between the
# price this row records and the moment a human could first act on it. It has
# never been measured, and on this chain entry prices drift ~9%/min, so it
# decides whether a backtested return is reachable at all.
_ALERT_COLUMN_ADDITIONS = (
    ("pool_address", "TEXT"),
    ("pool_fee_pct", "REAL"),
    ("priced_at", "REAL"),
    ("alert_sent_at", "REAL"),
)

_conn: sqlite3.Connection | None = None


def _migrate_alerts(conn: sqlite3.Connection):
    """
    Add any missing columns to alerts. Idempotent: reads the live schema and
    only issues ALTER TABLE for what is genuinely absent, so it is safe to run
    on every start, on a fresh DB, and on one with existing rows.
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(alerts)")}
    added = []
    for name, column_type in _ALERT_COLUMN_ADDITIONS:
        if name not in existing:
            conn.execute(f"ALTER TABLE alerts ADD COLUMN {name} {column_type}")
            added.append(name)
    if added:
        conn.commit()
        print(f"[db] Migrated alerts: added {', '.join(added)}")


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                symbol TEXT,
                channel TEXT NOT NULL,
                score INTEGER,
                liquidity_at_alert REAL,
                price_at_alert REAL NOT NULL,
                alerted_at REAL NOT NULL,
                price_1h REAL,
                price_6h REAL,
                price_24h REAL,
                pool_address TEXT,
                pool_fee_pct REAL,
                priced_at REAL,
                alert_sent_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(alerted_at);

            CREATE TABLE IF NOT EXISTS seen (
                scope TEXT NOT NULL,
                address TEXT NOT NULL,
                ts REAL NOT NULL,
                PRIMARY KEY (scope, address)
            );

            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
        _conn.commit()
        # Fresh databases get the columns from the block above; existing ones
        # get them here. Both paths end at the same schema.
        _migrate_alerts(_conn)
    return _conn


def schema_info() -> list[tuple]:
    """
    Live column list for alerts, as (cid, name, type, notnull, default, pk).
    Exists so a migration can be verified from a one-line script instead of
    opening the DB by hand.
    """
    conn = _get_conn()
    return conn.execute("PRAGMA table_info(alerts)").fetchall()


# === Alert performance tracking ===

def record_alert(
    address: str,
    symbol: str,
    channel: str,
    price_at_alert: float,
    score: int | None = None,
    liquidity: float | None = None,
    pool_address: str | None = None,
    pool_fee_pct: float | None = None,
    priced_at: float | None = None,
    alert_sent_at: float | None = None,
) -> int | None:
    """
    Store a fired alert with its entry price for later measurement.

    The four trailing arguments are optional and default to NULL, so every
    existing caller keeps working untouched — a channel that does not pin a
    pool simply records nothing there, and the tracker falls back to its old
    path for those rows.

    Returns the new row id, or None if the alert was rejected for a
    non-positive entry price.
    """
    if not price_at_alert or price_at_alert <= 0:
        return None
    conn = _get_conn()
    cursor = conn.execute(
        "INSERT INTO alerts (address, symbol, channel, score, "
        "liquidity_at_alert, price_at_alert, alerted_at, "
        "pool_address, pool_fee_pct, priced_at, alert_sent_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            address, symbol, channel, score, liquidity, price_at_alert,
            time.time(), pool_address, pool_fee_pct, priced_at, alert_sent_at,
        ),
    )
    conn.commit()
    return cursor.lastrowid


def due_measurements(column: str, delay_seconds: int, limit: int = 25) -> list[tuple]:
    """
    Return (id, address) rows whose `column` is unmeasured and due.

    Kept at its original shape on purpose. due_measurements_pinned below is the
    replacement; this one stays until every caller has moved over, so a partial
    deploy cannot break the running scanner.
    """
    if column not in _PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {column}")
    conn = _get_conn()
    cutoff = time.time() - delay_seconds
    rows = conn.execute(
        f"SELECT id, address FROM alerts "
        f"WHERE {column} IS NULL AND alerted_at <= ? "
        f"ORDER BY alerted_at ASC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    return rows


def due_measurements_pinned(
    column: str, delay_seconds: int, limit: int = 25
) -> list[dict]:
    """
    Same selection as due_measurements, returned as dicts and carrying the
    pinned pool.

    pool_address is NULL for every row written before the pin existed and for
    every channel that does not pin one. The caller decides what that means —
    this function reports the fact and nothing more.
    """
    if column not in _PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {column}")
    conn = _get_conn()
    cutoff = time.time() - delay_seconds
    cursor = conn.execute(
        f"SELECT id, address, channel, pool_address, pool_fee_pct, alerted_at "
        f"FROM alerts "
        f"WHERE {column} IS NULL AND alerted_at <= ? "
        f"ORDER BY alerted_at ASC LIMIT ?",
        (cutoff, limit),
    )
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def set_measurement(row_id: int, column: str, price: float):
    if column not in _PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {column}")
    conn = _get_conn()
    conn.execute(f"UPDATE alerts SET {column} = ? WHERE id = ?", (price, row_id))
    conn.commit()


def performance_rows(days: int = 7) -> list[dict]:
    """
    Return alert rows from the last N days for report aggregation.

    Rows are dicts, so the added keys are invisible to existing consumers that
    index by name.
    """
    conn = _get_conn()
    cutoff = time.time() - days * 86_400
    cursor = conn.execute(
        "SELECT address, symbol, channel, score, price_at_alert, "
        "alerted_at, price_1h, price_6h, price_24h, "
        "liquidity_at_alert, pool_address, pool_fee_pct, "
        "priced_at, alert_sent_at "
        "FROM alerts WHERE alerted_at >= ?",
        (cutoff,),
    )
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def latency_rows(days: int = 7, channel: str | None = None) -> list[dict]:
    """
    Pipeline latency per alert: seconds between observing the price and the
    Discord message landing. Only rows written after the instrumentation exists
    have both timestamps, so this returns an empty list until then rather than
    guessing a value for older rows.
    """
    conn = _get_conn()
    cutoff = time.time() - days * 86_400
    query = (
        "SELECT id, symbol, channel, alerted_at, priced_at, alert_sent_at, "
        "(alert_sent_at - priced_at) AS latency_seconds "
        "FROM alerts "
        "WHERE alerted_at >= ? AND priced_at IS NOT NULL "
        "AND alert_sent_at IS NOT NULL"
    )
    params: list = [cutoff]
    if channel:
        query += " AND channel = ?"
        params.append(channel)
    query += " ORDER BY alerted_at ASC"
    cursor = conn.execute(query, params)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# === Restart-proof seen/dedup state ===

def load_seen(scope: str, ttl_seconds: int) -> dict[str, float]:
    conn = _get_conn()
    cutoff = time.time() - ttl_seconds
    rows = conn.execute(
        "SELECT address, ts FROM seen WHERE scope = ? AND ts > ?",
        (scope, cutoff),
    ).fetchall()
    return {addr: ts for addr, ts in rows}


def save_seen(scope: str, address: str):
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO seen (scope, address, ts) VALUES (?, ?, ?)",
        (scope, address, time.time()),
    )
    conn.commit()


def cleanup_seen(scope: str, ttl_seconds: int):
    conn = _get_conn()
    cutoff = time.time() - ttl_seconds
    conn.execute("DELETE FROM seen WHERE scope = ? AND ts <= ?", (scope, cutoff))
    conn.commit()


# === Generic JSON key-value state (Jupiter baseline etc.) ===

def kv_get_json(key: str, default=None):
    conn = _get_conn()
    row = conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return default


def kv_set_json(key: str, value):
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
        (key, json.dumps(value)),
    )
    conn.commit()


# === Meme/utility classification cache (per mint, no TTL — a token's
# category does not change, so once classified it is never re-queried) ===

def get_token_classification(mint: str) -> dict | None:
    return kv_get_json(f"classify:{mint}")


def set_token_classification(mint: str, classification: dict):
    kv_set_json(f"classify:{mint}", classification)