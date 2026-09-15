# db.py
# SQLite persistence layer: alert performance tracking + state that
# must survive restarts. Standard library only, single shared connection.
# Writes are tiny and infrequent — synchronous sqlite3 is fine here.

import json
import os
import sqlite3
import time

DB_PATH = os.environ.get("SCANNER_DB_PATH", "trench_scanner.db")

# price_15m added 2026-09-13. Measured over 7 days on the same rows at all three
# existing marks, the EARLIEST mark was the best of the three on 73-79% of rows
# (rank statistic, so no outlier can move it), while the 24h mark was best on
# 7-13% and carried a median of -87%. 1h is not an optimum, it is the edge of
# what we sample: robinhood alerts fire at pool ages of 0.1-1.4h, so whatever
# happens may well happen before the first mark. This column samples below it.
_PRICE_COLUMNS = ("price_15m", "price_1h", "price_6h", "price_24h")

# Which column records HOW each price column was measured. Measurement method
# is tracked per slot, not per row: a row alerted before the pinned-pool path
# ships gets its 1h reading from the old method and its 24h reading from the
# new one. A single per-row marker would silently mislabel one of them.
_SOURCE_COLUMNS = {
    "price_15m": "price_15m_src",
    "price_1h": "price_1h_src",
    "price_6h": "price_6h_src",
    "price_24h": "price_24h_src",
}

# Method identifiers written into the _src columns. Any aggregation that mixes
# these is comparing two different measurements, so reports must group by them
# rather than averaging across.
SRC_DEXSCREENER_CURRENT = "dexscreener_current"   # most-liquid pair, read at measurement time
SRC_GECKO_PINNED = "gecko_pinned_ohlcv"           # pinned pool, OHLCV bounded at the slot

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
#                   Measured NULL rate: 4.0% of instrumented rows (2026-09-12),
#                   flat across days — GeckoTerminal simply has not indexed the
#                   freshest tokens yet. That class cannot be measured on the
#                   pinned path and must fall back, not be dropped.
#   pool_fee_pct    fee tier of that pool, parsed from its name. Kept for
#                   auditing which alerts passed the <=0.05% gate. NOTE: 100%
#                   NULL on all 884 pinned rows as of 2026-09-13 — either never
#                   passed by the caller or the name parse always fails. Not on
#                   the critical path (measurement keys off pool_address), but
#                   it is a dead column that currently looks alive.
#   priced_at       when price_at_alert was actually observed upstream.
#   alert_sent_at   when the Discord webhook accepted the message.
#
#   price_*_src     how that slot was measured. See _SOURCE_COLUMNS above.
#   entry_price_pinned
#                   entry price re-derived from the PINNED pool's OHLCV at
#                   alert time. Stored ALONGSIDE price_at_alert, never over it:
#                   price_at_alert is the only record of what the alert actually
#                   showed, and overwriting it would destroy the ability to ask
#                   later whether the quoted price was reachable. A return
#                   computed from entry_price_pinned and a pinned-pool exit has
#                   both ends on the same venue; one computed from
#                   price_at_alert does not — the two sources disagreed on
#                   11.9% of selections (n=469, 2026-09-13).
#
#                   MEASURED 2026-09-13, n=353 rows carrying both entries: the
#                   ratio price_at_alert / entry_price_pinned has median 1.0031
#                   and p10-p90 of 0.81-1.38, so for most rows the two entries
#                   are the same price and swapping them moves a MEDIAN return
#                   by ~0.3pp. The value of this column is entirely in the tail:
#                   2.3% of rows are wrong by an order of magnitude, and those
#                   are what turn a legacy return into +86,836,099,869%. Judge
#                   this column on means and percentiles, never on medians.
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
    ("price_15m", "REAL"),
    ("price_1h_src", "TEXT"),
    ("price_6h_src", "TEXT"),
    ("price_24h_src", "TEXT"),
    ("price_15m_src", "TEXT"),
    ("entry_price_pinned", "REAL"),
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
                price_15m REAL,
                price_1h REAL,
                price_6h REAL,
                price_24h REAL,
                pool_address TEXT,
                pool_fee_pct REAL,
                priced_at REAL,
                alert_sent_at REAL,
                price_15m_src TEXT,
                price_1h_src TEXT,
                price_6h_src TEXT,
                price_24h_src TEXT,
                entry_price_pinned REAL
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

            -- Launch Radar. These tables predate this block: they were created
            -- out-of-band and already hold ~971k rows on the live DB, where
            -- CREATE TABLE IF NOT EXISTS is a no-op. They are declared here so
            -- a FRESH database (a Railway deploy, a rebuilt dev box) comes up
            -- with the same schema instead of failing on first launch event.
            -- Column list mirrors the live PRAGMA output exactly — do not
            -- reorder or retype without migrating the existing rows.
            CREATE TABLE IF NOT EXISTS launches (
                mint TEXT PRIMARY KEY,
                creator_wallet TEXT,
                name TEXT,
                symbol TEXT,
                uri TEXT,
                dev_buy_sol REAL,
                dev_buy_tokens REAL,
                mcap_sol_at_launch REAL,
                v_sol_at_launch REAL,
                v_tok_at_launch REAL,
                pool TEXT,
                is_mayhem_mode INTEGER,
                has_socials INTEGER,
                socials_json TEXT,
                name_is_junk INTEGER,
                created_at REAL,
                signature TEXT,
                migrated INTEGER DEFAULT 0,
                migrated_at REAL,
                velocity_60s REAL,
                unique_buyers_60s INTEGER,
                buyers_measured INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_launches_created ON launches(created_at);
            CREATE INDEX IF NOT EXISTS idx_launches_creator ON launches(creator_wallet);

            CREATE TABLE IF NOT EXISTS creator_stats (
                creator_wallet TEXT PRIMARY KEY,
                total_launches INTEGER DEFAULT 0,
                migrated_count INTEGER DEFAULT 0,
                first_seen REAL,
                last_seen REAL
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
    column: str,
    delay_seconds: int,
    limit: int = 25,
    channel: str | None = None,
    max_age_seconds: float | None = None,
) -> list[dict]:
    """
    Same selection as due_measurements, returned as dicts and carrying the
    pinned pool plus the already-derived pinned entry price.

    pool_address is NULL for every row written before the pin existed and for
    every channel that does not pin one. The caller decides what that means —
    this function reports the fact and nothing more.

    `channel` narrows the selection so the Gecko-backed path can claim only the
    rows it can actually measure, instead of pulling solana rows into a
    robinhood-only budget and returning them unmeasured.

    `max_age_seconds` bounds the selection from the OTHER side: only rows
    alerted within this many seconds are returned. Required by any short-horizon
    slot, for two reasons that compound:

      1. GeckoTerminal does not retain minute candles indefinitely. Asking for a
         15-minute mark on a five-day-old alert returns nothing on a quiet pool,
         which the tracker records as nodata — a fact about candle retention
         written into the DB as if it were a fact about the token.
      2. Selection is ORDER BY alerted_at ASC. The moment a new price column
         exists, EVERY historical row has it NULL and is past its delay, so the
         oldest rows are claimed first and the budget is spent entirely on the
         backlog. A new short-horizon column would fill up with nodata from last
         week and never reach a fresh alert, and the slot would look broken
         rather than starved.

    Bounding here rather than skipping in the caller's loop is deliberate: a row
    skipped inside the loop is re-selected on every later run forever, which is
    the starvation failure the slot rotation exists to prevent. A row excluded
    by this bound simply stops being a candidate.
    """
    if column not in _PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {column}")
    conn = _get_conn()
    now = time.time()
    cutoff = now - delay_seconds
    query = (
        f"SELECT id, address, channel, pool_address, pool_fee_pct, alerted_at, "
        f"price_at_alert, entry_price_pinned "
        f"FROM alerts "
        f"WHERE {column} IS NULL AND alerted_at <= ?"
    )
    params: list = [cutoff]
    if max_age_seconds is not None:
        query += " AND alerted_at >= ?"
        params.append(now - max_age_seconds)
    if channel:
        query += " AND channel = ?"
        params.append(channel)
    query += " ORDER BY alerted_at ASC LIMIT ?"
    params.append(limit)

    cursor = conn.execute(query, params)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def set_measurement(row_id: int, column: str, price: float, source: str | None = None):
    """
    Record a price for one slot, and how it was obtained.

    `source` defaults to None so the pre-existing caller keeps working during a
    partial deploy, but every new call should pass one: a price with no recorded
    method cannot be separated from a differently-measured price later, and the
    two are not comparable. Rows written before this column existed stay NULL,
    which is itself informative — NULL means "old DexScreener path".
    """
    if column not in _PRICE_COLUMNS:
        raise ValueError(f"Invalid price column: {column}")
    conn = _get_conn()
    if source is None:
        conn.execute(f"UPDATE alerts SET {column} = ? WHERE id = ?", (price, row_id))
    else:
        src_column = _SOURCE_COLUMNS[column]
        conn.execute(
            f"UPDATE alerts SET {column} = ?, {src_column} = ? WHERE id = ?",
            (price, source, row_id),
        )
    conn.commit()


def set_entry_price_pinned(row_id: int, price: float):
    """
    Store the entry price re-derived from the pinned pool.

    Written once per row and reused by every later slot, so the derivation costs
    one API call per alert rather than one per measurement. Never touches
    price_at_alert.
    """
    conn = _get_conn()
    conn.execute(
        "UPDATE alerts SET entry_price_pinned = ? WHERE id = ?", (price, row_id)
    )
    conn.commit()


def performance_rows(days: int = 7) -> list[dict]:
    """
    Return alert rows from the last N days for report aggregation.

    Rows are dicts, so the added keys are invisible to existing consumers that
    index by name. The _src columns are included so a report can refuse to
    average across measurement methods.
    """
    conn = _get_conn()
    cutoff = time.time() - days * 86_400
    cursor = conn.execute(
        "SELECT address, symbol, channel, score, price_at_alert, "
        "alerted_at, price_15m, price_1h, price_6h, price_24h, "
        "liquidity_at_alert, pool_address, pool_fee_pct, "
        "priced_at, alert_sent_at, "
        "price_15m_src, price_1h_src, price_6h_src, price_24h_src, "
        "entry_price_pinned "
        "FROM alerts WHERE alerted_at >= ?",
        (cutoff,),
    )
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def measurement_method_census(days: int = 7) -> list[tuple]:
    """
    Count measurements per (channel, slot, method) over the window.

    Exists so the mixed-method problem stays visible: after the pinned path
    ships, a channel's price_1h column holds readings from two incompatible
    methods, and nothing else in the schema makes that obvious. NULL method
    means the old DexScreener path.
    """
    conn = _get_conn()
    cutoff = time.time() - days * 86_400
    out = []
    for column, src_column in _SOURCE_COLUMNS.items():
        rows = conn.execute(
            f"SELECT channel, ?, {src_column}, COUNT(*) "
            f"FROM alerts "
            f"WHERE alerted_at >= ? AND {column} IS NOT NULL "
            f"GROUP BY channel, {src_column} "
            f"ORDER BY channel",
            (column, cutoff),
        ).fetchall()
        out.extend(rows)
    return out


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


# === Launch Radar (silent collection phase) ===
#
# pumpportal_client writes here on every pump.fun/bonk create event and closes
# the outcome loop on the migration stream. Nothing is alerted from these
# tables — they exist so the question "does any launch-time signal predict
# graduation?" can be answered from data rather than intuition.
#
# NOTE ON `pool`: the discriminator between pump.fun and bonk launches is this
# field ('pump' or 'bonk'), NOT a "platform" key — that belongs to a different
# provider and does not exist in the PumpPortal stream. Bonk rows lack
# v_sol_at_launch / v_tok_at_launch / is_mayhem_mode entirely, so ANY analysis
# touching those columns must filter WHERE pool='pump' or it will silently
# compare populations that were never measured the same way.


def record_launch(
    mint: str,
    creator_wallet: str | None = None,
    name: str | None = None,
    symbol: str | None = None,
    uri: str | None = None,
    dev_buy_sol: float | None = None,
    dev_buy_tokens: float | None = None,
    mcap_sol_at_launch: float | None = None,
    v_sol_at_launch: float | None = None,
    v_tok_at_launch: float | None = None,
    pool: str | None = None,
    is_mayhem_mode: bool | int | None = None,
    has_socials: bool | int | None = None,
    socials_json: str | None = None,
    name_is_junk: bool | int | None = None,
    signature: str | None = None,
) -> bool:
    """
    Record a new launch. Returns True if this mint was newly inserted, False if
    it was already present (PumpPortal repeats create events on reconnect).

    Existence is checked with an explicit SELECT rather than relying on
    INSERT OR IGNORE + rowcount. On the live database `mint` is the primary
    key so both work, but the tables were created out-of-band and a rebuilt DB
    that somehow lacked the constraint would silently accumulate duplicates
    under the rowcount approach. The extra SELECT costs nothing at this write
    rate (~17/min) and cannot be wrong.

    creator_stats is updated only for genuinely new mints, so a replayed event
    cannot inflate a wallet's launch count.
    """
    if not mint:
        return False

    conn = _get_conn()
    existing = conn.execute(
        "SELECT 1 FROM launches WHERE mint = ?", (mint,)
    ).fetchone()
    if existing:
        return False

    now = time.time()
    conn.execute(
        "INSERT INTO launches ("
        "mint, creator_wallet, name, symbol, uri, "
        "dev_buy_sol, dev_buy_tokens, mcap_sol_at_launch, "
        "v_sol_at_launch, v_tok_at_launch, pool, is_mayhem_mode, "
        "has_socials, socials_json, name_is_junk, created_at, signature, "
        "migrated"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
        (
            mint, creator_wallet, name, symbol, uri,
            dev_buy_sol, dev_buy_tokens, mcap_sol_at_launch,
            v_sol_at_launch, v_tok_at_launch, pool,
            int(bool(is_mayhem_mode)) if is_mayhem_mode is not None else None,
            int(bool(has_socials)) if has_socials is not None else None,
            socials_json,
            int(bool(name_is_junk)) if name_is_junk is not None else None,
            now, signature,
        ),
    )

    if creator_wallet:
        conn.execute(
            "INSERT INTO creator_stats "
            "(creator_wallet, total_launches, migrated_count, first_seen, last_seen) "
            "VALUES (?, 1, 0, ?, ?) "
            "ON CONFLICT(creator_wallet) DO UPDATE SET "
            "total_launches = total_launches + 1, last_seen = excluded.last_seen",
            (creator_wallet, now, now),
        )

    conn.commit()
    return True


def mark_launch_migrated(mint: str) -> bool:
    """
    Close the outcome loop: flag a recorded launch as graduated.

    Returns True only when this call actually changed something — i.e. the mint
    was in `launches` and was not already flagged. False means either the token
    launched before collection started (the common case early on) or the event
    was a duplicate. Both are normal and neither is an error.

    Idempotent by construction: the UPDATE is guarded on migrated != 1, so a
    repeated migration event cannot double-count the creator's success tally.
    """
    if not mint:
        return False

    conn = _get_conn()
    row = conn.execute(
        "SELECT creator_wallet, migrated FROM launches WHERE mint = ?", (mint,)
    ).fetchone()
    if row is None:
        return False           # launched before collection started
    creator_wallet, already = row[0], row[1]
    if already == 1:
        return False           # duplicate migration event

    now = time.time()
    conn.execute(
        "UPDATE launches SET migrated = 1, migrated_at = ? "
        "WHERE mint = ? AND (migrated IS NULL OR migrated != 1)",
        (now, mint),
    )
    if creator_wallet:
        conn.execute(
            "UPDATE creator_stats SET migrated_count = migrated_count + 1, "
            "last_seen = ? WHERE creator_wallet = ?",
            (now, creator_wallet),
        )
    conn.commit()
    return True


def launch_count() -> int:
    """Total launches collected so far. Used by the progress heartbeat."""
    conn = _get_conn()
    row = conn.execute("SELECT COUNT(*) FROM launches").fetchone()
    return row[0] if row else 0


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