# migration_monitor.py
# Real-time pump.fun -> DEX migration monitor via PumpPortal WebSocket.
# Migration = token graduated from the bonding curve and just received
# real DEX liquidity. This is the earliest actionable on-chain signal
# the scanner has — alerts fire within ~1 minute of graduation.
# Self-contained module (own webhook + embed), same pattern as robinhood_monitor.
#
# Bundle enrichment runs OFF the critical path: the alert is posted the
# moment we have a DexScreener pair, then a separate isolated task fetches
# the (slow, unofficial) bundle report AND the OHLCV chart, then EDITS the
# embed in place (one multipart PATCH). This keeps alerts fast while still
# surfacing bundle risk + chart when the data arrives.
#
# Note: freshly-migrated tokens are minutes old, so OHLCV usually has too few
# candles to draw — migration will most often show no chart, by design.

import asyncio
import aiohttp
import json
import os
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
# GeckoTerminal network id for Solana is the stable string "solana" — no
# resolve needed (unlike robinhood).
GECKO_NETWORK = "solana"

# Light filters only — a token minutes after migration has no meaningful
# txn/volume history yet, so the standard gem filters would reject everything.
MIN_LIQUIDITY_USD = 10_000

# DexScreener needs time to index the new pool after migration
PAIR_FETCH_ATTEMPTS = 4
PAIR_FETCH_DELAY_SECONDS = 15

# Limit concurrent migration handlers (each waits for DexScreener indexing)
MAX_CONCURRENT_HANDLERS = 5

# Embed colors
COLOR_DEFAULT = 0x00D4FF
COLOR_BUNDLE_RISK = 0xFF3300

DEDUP_TTL = 86_400
alerted_migrations: dict[str, float] = {}

_handler_semaphore = asyncio.Semaphore(MAX_CONCURRENT_HANDLERS)


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

    # ?wait=true makes Discord respond 200 + JSON (incl. message id) instead of 204
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

        # Best-effort chart: use the DexScreener pool address directly as the
        # GeckoTerminal pool id (same on-chain address). No data / too few
        # candles -> None -> embed edits without an image.
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

    async with _handler_semaphore:
        # Only the pair is on the critical path — the alert cannot go out
        # without it (nothing to price/track). Bundle+chart are fetched after.
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

            # Record for performance tracking (only when we have a price)
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

            # Enrich with bundle + chart only if we can edit (have message_id)
            # and the mint is a pump.fun token (others have no bundle data).
            if message_id and is_pumpfun_mint(mint):
                asyncio.create_task(
                    _enrich_with_bundle(session, mint, pair, event, message_id)
                )

    _cleanup_dedup()


async def run_migration_monitor(session: aiohttp.ClientSession):
    """
    Main WebSocket loop. Subscribes to pump.fun migration events.
    Reconnects automatically on disconnect.
    """
    if not WEBHOOK_MIGRATIONS:
        print("[migration] DISCORD_WEBHOOK_MIGRATIONS not set — monitor disabled")
        return

    while True:
        try:
            print("[migration] Connecting to PumpPortal WebSocket...")
            async with session.ws_connect(
                PUMPPORTAL_WS,
                heartbeat=30,
            ) as ws:
                print("[migration] Connected")

                await ws.send_str(json.dumps({"method": "subscribeMigration"}))
                print("[migration] Subscribed to migration events")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        # Skip subscription confirmations / service messages
                        if not isinstance(data, dict) or "mint" not in data:
                            continue

                        # Fire-and-forget so the WS loop never blocks on
                        # DexScreener indexing waits
                        asyncio.create_task(_handle_migration(session, data))

                    elif msg.type in (
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED,
                    ):
                        print(f"[migration] WebSocket closed/error")
                        break

        except asyncio.CancelledError:
            print("[migration] Monitor cancelled")
            return
        except Exception as e:
            print(f"[migration] Connection error: {e}")

        print("[migration] Reconnecting in 5s...")
        await asyncio.sleep(5)