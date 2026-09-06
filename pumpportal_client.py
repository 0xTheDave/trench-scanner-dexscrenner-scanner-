# pumpportal_client.py
# Single shared PumpPortal WebSocket connection, multiplexing TWO subscriptions
# on ONE connection (PumpPortal warns against multiple simultaneous connections
# from one client — timeout risk). Replaces the old migration_monitor.py.
#
#   subscribeMigration  -> _handle_migration  (migration alerts, unchanged
#                          except one line that closes the launch-outcome loop)
#   subscribeNewToken   -> _handle_create     (Launch Radar — SILENT phase:
#                          writes to `launches`/`creator_stats`, NO Discord)
#
# Launch Radar is in its silent data-collection phase: every new pump.fun
# launch is recorded to the DB along with the free anti-rug signals derivable
# from the create event (dev buy, mayhem flag, socials from the metadata uri,
# junk-name flag). Outcome is filled in for free when/if the token later
# appears on the migration stream. NOTHING is posted to Discord yet — we
# collect ~N launches, then run analysis to decide whether any filter has
# predictive value before ever building a channel.
#
# Cost note: subscribeNewToken and subscribeMigration are BOTH free. The only
# extra I/O added here is a plain HTTP GET of each launch's metadata `uri`
# (free; usually IPFS/Arweave), rate-limited by a global lock so peaks can't
# flood the gateways. subscribeTokenTrade/AccountTrade (metered) are NOT used.

import asyncio
import aiohttp
import json
import os
import re
import time
from datetime import datetime, timezone

import db
from bundle_client import fetch_bundle_report, assess_bundle_risk, is_pumpfun_mint
from geckoterminal_client import fetch_ohlcv
from chart_renderer import render_chart_async

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
DEXSCREENER_BASE = "https://api.dexscreener.com"
WEBHOOK_MIGRATIONS = os.environ.get("DISCORD_WEBHOOK_MIGRATIONS", "")

CHAIN_ID = "solana"
GECKO_NETWORK = "solana"

MIN_LIQUIDITY_USD = 10_000

PAIR_FETCH_ATTEMPTS = 4
PAIR_FETCH_DELAY_SECONDS = 15

MAX_CONCURRENT_HANDLERS = 5

COLOR_DEFAULT = 0x00D4FF
COLOR_BUNDLE_RISK = 0xFF3300

DEDUP_TTL = 86_400
alerted_migrations: dict[str, float] = {}

_handler_semaphore = asyncio.Semaphore(MAX_CONCURRENT_HANDLERS)

# ----------------------------- Launch Radar ----------------------------- #

# Global rate limit for metadata (uri) fetches — same pattern as the Gecko /
# OpenSea clients. At ~17 launches/min average a 0.5s floor gives ~120 req/min
# headroom for peaks without hammering IPFS/Arweave gateways.
_METADATA_MIN_INTERVAL = 0.5
_metadata_lock = asyncio.Lock()
_metadata_last_call = 0.0
_METADATA_TIMEOUT = 8

# Progress heartbeat: log total collected every N launches.
_LAUNCH_LOG_EVERY = 25
_launch_seen_count = 0

# Junk-name detection. In the SILENT phase this is only a FLAG stored on the
# row (name_is_junk) — never a filter. We record every launch regardless, so
# analysis can later test whether junk names actually correlate with worse
# outcomes. Patterns cover: test/placeholder names, empty/1-2 char symbols,
# and obvious low-effort spam. Impersonation of known names is intentionally
# NOT hardcoded here (too noisy / subjective) — left for the analysis phase.
_JUNK_PATTERNS = [
    r"^\s*$",              # empty / whitespace only
    r"^test\d*$",          # test, test1, test2...
    r"^\?+$",              # ? / ?? / ???
    r"^[\W_]+$",           # only symbols/underscores, no alphanumerics
    r"^(asdf|qwer|zxcv)",  # keyboard-mash prefixes
]
_JUNK_RE = re.compile("|".join(_JUNK_PATTERNS), re.IGNORECASE)

# Where socials commonly live inside the metadata JSON. We check flat keys
# first, then a few known nested containers.
_SOCIAL_KEYS = ("twitter", "telegram", "website", "discord")
_SOCIAL_NESTS = ("extensions", "properties", "links")


def _is_junk_name(name: str | None, symbol: str | None) -> bool:
    """True if either the name or the ticker looks like junk/placeholder.
    A short symbol alone (1-2 chars) is weak signal, so we only flag it when
    it's also non-alphanumeric; real 1-2 char tickers do exist."""
    for value in (name, symbol):
        if value is None:
            continue
        if _JUNK_RE.search(value.strip()):
            return True
    return False


def _extract_socials(meta: dict) -> dict:
    """Pull social/link fields out of a token's metadata JSON, handling both
    flat and nested shapes seen in the live diagnostic."""
    found: dict[str, str] = {}
    if not isinstance(meta, dict):
        return found

    for key in _SOCIAL_KEYS:
        val = meta.get(key)
        if val:
            found[key] = val

    for nest in _SOCIAL_NESTS:
        container = meta.get(nest)
        if isinstance(container, dict):
            for key, val in container.items():
                if val and key.lower() in _SOCIAL_KEYS:
                    found.setdefault(key.lower(), val)

    return found


async def _fetch_metadata(session: aiohttp.ClientSession, uri: str) -> dict | None:
    """
    Free HTTP GET of the off-chain metadata JSON (IPFS/Arweave gateway),
    serialized behind a global rate-limit lock. Returns parsed dict or None
    on any failure/timeout — a failed fetch just means has_socials=0, never
    a dropped launch.
    """
    global _metadata_last_call
    if not uri:
        return None

    async with _metadata_lock:
        delta = time.monotonic() - _metadata_last_call
        if delta < _METADATA_MIN_INTERVAL:
            await asyncio.sleep(_METADATA_MIN_INTERVAL - delta)
        _metadata_last_call = time.monotonic()

    try:
        async with session.get(
            uri, timeout=aiohttp.ClientTimeout(total=_METADATA_TIMEOUT)
        ) as resp:
            if resp.status != 200:
                return None
            # Gateways often mislabel JSON content-type -> parse permissively.
            try:
                return await resp.json(content_type=None)
            except Exception:
                text = await resp.text()
                return json.loads(text)
    except Exception:
        return None


async def _handle_create(session: aiohttp.ClientSession, event: dict):
    """
    Launch Radar silent phase: record a new pump.fun launch to the DB with the
    free signals we can derive from the create event, plus socials fetched
    from the metadata uri. No Discord, no alerts.
    """
    global _launch_seen_count

    mint = event.get("mint")
    if not mint:
        return

    name = event.get("name")
    symbol = event.get("symbol")

    # Socials live in the metadata JSON (confirmed by diagnostic — never inline
    # on the event). Free GET, rate-limited; failure -> has_socials=0.
    meta = await _fetch_metadata(session, event.get("uri"))
    socials = _extract_socials(meta) if meta else {}
    has_socials = bool(socials)
    socials_json = json.dumps(socials) if socials else None

    name_is_junk = _is_junk_name(name, symbol)

    is_new = db.record_launch(
        mint=mint,
        creator_wallet=event.get("traderPublicKey"),
        name=name,
        symbol=symbol,
        uri=event.get("uri"),
        dev_buy_sol=event.get("solAmount"),
        dev_buy_tokens=event.get("initialBuy"),
        mcap_sol_at_launch=event.get("marketCapSol"),
        v_sol_at_launch=event.get("vSolInBondingCurve"),
        v_tok_at_launch=event.get("vTokensInBondingCurve"),
        pool=event.get("pool"),
        is_mayhem_mode=event.get("is_mayhem_mode"),
        has_socials=has_socials,
        socials_json=socials_json,
        name_is_junk=name_is_junk,
        signature=event.get("signature"),
    )

    if not is_new:
        return  # duplicate create event — already counted

    _launch_seen_count += 1
    if _launch_seen_count % _LAUNCH_LOG_EVERY == 0:
        total = db.launch_count()
        print(
            f"[launch] +{_launch_seen_count} this session | {total} total collected | "
            f"latest=${str(symbol or '?').lstrip('$')} "
            f"pool={event.get('pool')!r} mayhem={event.get('is_mayhem_mode')} "
            f"dev_buy={event.get('solAmount')} socials={'y' if has_socials else 'n'} "
            f"junk={'y' if name_is_junk else 'n'}"
        )


# ---------------------------- Migration path ---------------------------- #
# Unchanged from migration_monitor.py, except one added line in
# _handle_migration that closes the Launch Radar outcome loop for free.

def _is_deduped(mint: str) -> bool:
    last = alerted_migrations.get(mint)
    if last is None:
        return False
    return time.time() - last < DEDUP_TTL


def _mark_alerted(mint: str):
    alerted_migrations[mint] = time.time()


def _cleanup_dedup():
    now = time.time()
    expired = [k for k, v in alerted_migrations.items() if now - v > DEDUP_TTL]
    for k in expired:
        del alerted_migrations[k]


def _fmt_usd(value: float) -> str:
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value/1_000:.1f}K"
    return f"${value:.0f}"


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception:
        return None


async def _fetch_best_pair(
    session: aiohttp.ClientSession,
    mint: str,
) -> dict | None:
    """
    Fetch the most liquid DexScreener pair for a freshly migrated mint.
    Retries because indexing lags the on-chain event by seconds to minutes.
    """
    for attempt in range(PAIR_FETCH_ATTEMPTS):
        await asyncio.sleep(PAIR_FETCH_DELAY_SECONDS)

        data = await _fetch_json(
            session,
            f"{DEXSCREENER_BASE}/token-pairs/v1/{CHAIN_ID}/{mint}"
        )

        if data:
            valid = [p for p in data if (p.get("liquidity") or {}).get("usd")]
            if valid:
                return max(valid, key=lambda x: x.get("liquidity", {}).get("usd", 0))

    return None


def _build_migration_embed(
    mint: str,
    pair: dict | None,
    event: dict,
    bundle_label: str,
    bundle_high_risk: bool,
    has_chart: bool = False,
) -> dict:
    """
    Build the migration alert embed. Used both for the initial send and for
    the in-place edit once the bundle report arrives — that's why bundle
    state comes in as pre-computed label + flag rather than a raw report.
    When has_chart is True, the embed references the attached chart image.
    """
    if pair:
        symbol = pair.get("baseToken", {}).get("symbol", "???").lstrip("$")
        name = pair.get("baseToken", {}).get("name", symbol)
        price = pair.get("priceUsd") or "N/A"
        liq = (pair.get("liquidity") or {}).get("usd") or 0
        vol_m5 = (pair.get("volume") or {}).get("m5") or 0
        ch_m5 = (pair.get("priceChange") or {}).get("m5") or 0
        txns_m5 = (pair.get("txns") or {}).get("m5") or {}
        buys_m5 = txns_m5.get("buys", 0)
        sells_m5 = txns_m5.get("sells", 0)
        mcap = pair.get("marketCap") or pair.get("fdv") or 0
        dex_url = pair.get("url", "")
        dex_id = pair.get("dexId", "?")
    else:
        symbol = (event.get("symbol") or mint[:8]).lstrip("$")
        name = event.get("name") or "Unknown"
        price = "N/A"
        liq = vol_m5 = ch_m5 = buys_m5 = sells_m5 = mcap = 0
        dex_url = f"https://dexscreener.com/solana/{mint}"
        dex_id = "?"

    fields = [
        {"name": "📋 CA", "value": f"`{mint}`", "inline": False},
        {"name": "💵 Price", "value": f"${price}", "inline": True},
        {"name": "📦 MCap", "value": _fmt_usd(mcap) if mcap else "N/A", "inline": True},
        {"name": "💧 Liquidity", "value": _fmt_usd(liq) if liq else "N/A", "inline": True},
        {"name": "⚡ Vol 5m", "value": _fmt_usd(vol_m5) if vol_m5 else "N/A", "inline": True},
        {"name": "📈 5m", "value": f"{ch_m5:+.1f}%" if pair else "N/A", "inline": True},
        {"name": "🔄 Txns 5m", "value": f"{buys_m5}↑ {sells_m5}↓" if pair else "N/A", "inline": True},
        {"name": "🧨 Bundle", "value": bundle_label, "inline": False},
        {
            "name": "🔗 Links",
            "value": (
                f"[📊 DexScreener]({dex_url}) · "
                f"[💊 pump.fun](https://pump.fun/coin/{mint})"
            ),
            "inline": False,
        },
    ]

    description = (
        f"**{name}**\n"
        f"Graduated from pump.fun — fresh DEX liquidity ({dex_id})\n"
        f"⚠️ Minutes old, extreme risk, no history"
    )
    if bundle_high_risk:
        description += "\n🧨 **Bundle warning** — bundlers still hold a large share of supply"

    embed = {
        "title": f"🎓 MIGRATION — ${symbol}",
        "description": description,
        "color": COLOR_BUNDLE_RISK if bundle_high_risk else COLOR_DEFAULT,
        "fields": fields,
        "footer": {"text": "Trench Scanner • PumpPortal Migrations"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if has_chart:
        embed["image"] = {"url": "attachment://chart.png"}
    return embed


async def _send_migration_alert(
    session: aiohttp.ClientSession,
    mint: str,
    pair: dict | None,
    event: dict,
) -> tuple[int | None, str | None]:
    """
    Send the migration alert immediately (no bundle/chart wait). Bundle field
    starts as 'checking...' for pump.fun mints (enriched later) or 'n/a'.
    Uses ?wait=true so Discord returns the message body — we need its id to
    edit the embed once the bundle report + chart land.
    Returns (http_status, message_id). message_id is None if unavailable.
    """
    if is_pumpfun_mint(mint):
        bundle_label = "⏳ checking..."
    else:
        bundle_label = "n/a"

    embed = _build_migration_embed(mint, pair, event, bundle_label, False)

    send_url = f"{WEBHOOK_MIGRATIONS}?wait=true"
    try:
        async with session.post(send_url, json={"embeds": [embed]}) as resp:
            if resp.status not in (200, 204):
                text = await resp.text()
                print(f"[migration] Discord send error: {resp.status} {text}")
                return resp.status, None

            message_id = None
            try:
                body = await resp.json()
                message_id = body.get("id")
            except Exception:
                pass  # 204 or unexpected body — alert still went out, just can't edit
            return resp.status, message_id
    except Exception as e:
        print(f"[migration] Send failed for {mint[:8]}...: {e}")
        return None, None


async def _edit_migration_alert(
    session: aiohttp.ClientSession,
    message_id: str,
    embed: dict,
    chart_png: bytes | None = None,
) -> int | None:
    """
    Edit a previously sent migration embed in place (PATCH on the webhook).
    When chart_png is provided, uses multipart to attach the chart image and
    the embed references it via attachment://chart.png; otherwise plain JSON.
    """
    edit_url = f"{WEBHOOK_MIGRATIONS}/messages/{message_id}"
    try:
        if chart_png:
            form = aiohttp.FormData()
            form.add_field("payload_json", json.dumps({"embeds": [embed]}))
            form.add_field(
                "file", chart_png,
                filename="chart.png", content_type="image/png",
            )
            async with session.patch(edit_url, data=form) as resp:
                if resp.status not in (200, 204):
                    text = await resp.text()
                    print(f"[migration] Discord edit error (multipart): {resp.status} {text}")
                return resp.status

        async with session.patch(edit_url, json={"embeds": [embed]}) as resp:
            if resp.status not in (200, 204):
                text = await resp.text()
                print(f"[migration] Discord edit error: {resp.status} {text}")
            return resp.status
    except Exception as e:
        print(f"[migration] Edit failed for message {message_id}: {e}")
        return None


async def _enrich_with_bundle(
    session: aiohttp.ClientSession,
    mint: str,
    pair: dict | None,
    event: dict,
    message_id: str,
):
    """
    Fully isolated background task: fetch the (slow, unofficial) bundle report
    AND the OHLCV chart, then edit the already-sent embed to show both. Any
    failure here is swallowed — it must never affect the alert already sent.
    Chart is best-effort: fresh migrations usually have too few candles, so
    no chart is the common case (embed just updates the bundle field).
    """
    try:
        report = await fetch_bundle_report(session, mint)
        label, high_risk = assess_bundle_risk(report)

        symbol = (pair or {}).get("baseToken", {}).get("symbol") or mint[:8]
        symbol = str(symbol).lstrip("$")

        chart_png = None
        pair_address = (pair or {}).get("pairAddress")
        if pair_address:
            ohlcv = await fetch_ohlcv(session, GECKO_NETWORK, pair_address)
            if ohlcv:
                chart_png = await render_chart_async(ohlcv, symbol, "5m")

        embed = _build_migration_embed(
            mint, pair, event, label, high_risk, has_chart=bool(chart_png)
        )
        await _edit_migration_alert(session, message_id, embed, chart_png)

        chart_tag = " +chart" if chart_png else ""
        if high_risk:
            print(f"[migration] 🧨 bundle HIGH-RISK on ${symbol} — embed updated to red{chart_tag}")
        else:
            print(f"[migration] bundle enriched for ${symbol}: {label}{chart_tag}")
    except Exception as e:
        print(f"[migration] Bundle enrich failed for {mint[:8]}...: {e}")


async def _handle_migration(session: aiohttp.ClientSession, event: dict):
    """Process a single migration event: fetch pair, filter, alert, enrich."""
    mint = event.get("mint")
    if not mint or _is_deduped(mint):
        return

    _mark_alerted(mint)  # mark early — prevents duplicate handlers for same mint

    # Launch Radar outcome loop (free): if we recorded this token's launch on
    # the newToken stream, mark it graduated. Idempotent + no-op for tokens
    # that launched before collection started. Never affects the alert below.
    try:
        if db.mark_launch_migrated(mint):
            print(f"[launch] outcome: {mint[:8]}... graduated (marked migrated)")
    except Exception as e:
        print(f"[launch] mark_migrated failed for {mint[:8]}...: {e}")

    async with _handler_semaphore:
        pair = await _fetch_best_pair(session, mint)

        if pair:
            liq = (pair.get("liquidity") or {}).get("usd") or 0
            if liq < MIN_LIQUIDITY_USD:
                symbol = pair.get("baseToken", {}).get("symbol", "???")
                print(f"[migration] {symbol} skipped — liq ${liq:,.0f} < ${MIN_LIQUIDITY_USD:,}")
                return
        else:
            print(f"[migration] {mint[:8]}... no DexScreener pair after "
                  f"{PAIR_FETCH_ATTEMPTS} attempts — sending bare alert")

        status, message_id = await _send_migration_alert(session, mint, pair, event)

        if status in (200, 204):
            symbol = (pair or {}).get("baseToken", {}).get("symbol") or mint[:8]
            print(f"[migration] ✅ ${str(symbol).lstrip('$')} migrated | mint={mint[:8]}...")

            if pair:
                try:
                    alert_price = float(pair.get("priceUsd") or 0)
                except (ValueError, TypeError):
                    alert_price = 0.0
                liq = (pair.get("liquidity") or {}).get("usd") or 0
                db.record_alert(
                    mint,
                    str(symbol).lstrip("$"),
                    "migrations",
                    alert_price,
                    liquidity=liq,
                )

            if message_id and is_pumpfun_mint(mint):
                asyncio.create_task(
                    _enrich_with_bundle(session, mint, pair, event, message_id)
                )

    _cleanup_dedup()


# ----------------------- Shared connection loop ------------------------- #

async def run_pumpportal_client(session: aiohttp.ClientSession):
    """
    Main WebSocket loop. ONE connection carrying BOTH subscriptions:
      - subscribeMigration : bonding-curve graduations (alerts)
      - subscribeNewToken  : new launches (Launch Radar, silent collection)
    Reconnects automatically on disconnect, re-sending both subscriptions.

    Replaces run_migration_monitor(). If DISCORD_WEBHOOK_MIGRATIONS is missing
    we still run the create path so Launch Radar collection isn't blocked by a
    migrations misconfig.
    """
    if not WEBHOOK_MIGRATIONS:
        print("[pumpportal] DISCORD_WEBHOOK_MIGRATIONS not set — migration alerts disabled "
              "(create/launch collection still active)")

    while True:
        try:
            print("[pumpportal] Connecting to PumpPortal WebSocket...")
            async with session.ws_connect(
                PUMPPORTAL_WS,
                heartbeat=30,
            ) as ws:
                print("[pumpportal] Connected")

                # Both subscriptions on the SAME connection — never open a
                # second WS to PumpPortal (they time out multi-connection clients).
                await ws.send_str(json.dumps({"method": "subscribeMigration"}))
                await ws.send_str(json.dumps({"method": "subscribeNewToken"}))
                print("[pumpportal] Subscribed to migration + newToken events")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        if not isinstance(data, dict) or "mint" not in data:
                            continue

                        if data.get("txType") == "create":
                            asyncio.create_task(_handle_create(session, data))
                        else:
                            asyncio.create_task(_handle_migration(session, data))

                    elif msg.type in (
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED,
                    ):
                        print("[pumpportal] WebSocket closed/error")
                        break

        except asyncio.CancelledError:
            print("[pumpportal] Client cancelled")
            return
        except Exception as e:
            print(f"[pumpportal] Connection error: {e}")

        print("[pumpportal] Reconnecting in 5s...")
        await asyncio.sleep(5)