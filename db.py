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

_conn: sqlite3.Connection | None = None


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
                price_24h REAL
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
    return _conn


# === Alert performance tracking ===

def record_alert(
    address: str,
    symbol: str,
    channel: str,
    price_at_alert: float,
    score: int | None = None,
    liquidity: float | None = None,
):
    """Store a fired alert with its entry price for later measurement."""
    if not price_at_alert or price_at_alert <= 0:
        return
    conn = _get_conn()
    conn.execute(
        "INSERT INTO alerts (address, symbol, channel, score, "
        "liquidity_at_alert, price_at_alert, alerted_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (address, symbol, channel, score, liquidity, price_at_alert, time.time()),
    )
    conn.commit()


def due_measurements(column: str, delay_seconds: int, limit: int = 25) -> list[tuple]:
    """Return (id, address) rows whose `column` is unmeasured and due."""
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


def set_measurement(row_id: int, column: str, price: float):
    if column not in _PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {column}")
    conn = _get_conn()
    conn.execute(f"UPDATE alerts SET {column} = ? WHERE id = ?", (price, row_id))
    conn.commit()


def performance_rows(days: int = 7) -> list[dict]:
    """Return alert rows from the last N days for report aggregation."""
    conn = _get_conn()
    cutoff = time.time() - days * 86_400
    cursor = conn.execute(
        "SELECT address, symbol, channel, score, price_at_alert, "
        "alerted_at, price_1h, price_6h, price_24h "
        "FROM alerts WHERE alerted_at >= ?",
        (cutoff,),
    )
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