# liquidation_monitor.py

import asyncio
import json
import time
import aiohttp
from datetime import datetime, timezone

from discord_client import send_liquidation_alert

HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"

# Per-coin USD thresholds — large caps need much higher bars
# to avoid alert spam from routine institutional flow
COIN_THRESHOLDS = {
    "BTC": 2_000_000,
    "ETH": 1_000_000,
    "SOL": 500_000,
}
DEFAULT_THRESHOLD = 200_000

# Top coins to monitor by OI — refreshed on startup
MAX_COINS_TO_MONITOR = 20

# Ping interval to keep WS alive (server closes after 60s idle)
PING_INTERVAL = 20

# Dedup: don't re-alert same coin within 5 minutes
DEDUP_TTL = 300
alerted: dict[str, float] = {}


def _get_threshold(coin: str) -> float:
    return COIN_THRESHOLDS.get(coin, DEFAULT_THRESHOLD)


def _is_deduped(coin: str) -> bool:
    last = alerted.get(coin)
    if last is None:
        return False
    return time.time() - last < DEDUP_TTL


def _mark_alerted(coin: str):
    alerted[coin] = time.time()


def _cleanup_dedup():
    now = time.time()
    expired = [k for k, v in alerted.items() if now - v > DEDUP_TTL]
    for k in expired:
        del alerted[k]


async def fetch_top_coins(session: aiohttp.ClientSession) -> list[str]:
    """
    Fetch top coins by open interest from Hyperliquid.
    Returns list of coin names sorted by OI descending.
    """
    try:
        async with session.post(
            HYPERLIQUID_API,
            json={"type": "metaAndAssetCtxs"},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                print(f"[liq] HTTP {resp.status} fetching top coins")
                return _fallback_coins()

            data = await resp.json()
            if not isinstance(data, list) or len(data) < 2:
                return _fallback_coins()

            universe = data[0].get("universe", [])
            asset_ctxs = data[1]

            coins_with_oi = []
            for meta, ctx in zip(universe, asset_ctxs):
                coin = meta.get("name", "")
                mark_px = float(ctx.get("markPx", 0) or 0)
                oi_raw = float(ctx.get("openInterest", 0) or 0)
                oi_usd = oi_raw * mark_px
                if coin and oi_usd > 0:
                    coins_with_oi.append((coin, oi_usd))

            coins_with_oi.sort(key=lambda x: x[1], reverse=True)
            top = [c[0] for c in coins_with_oi[:MAX_COINS_TO_MONITOR]]
            print(f"[liq] Monitoring {len(top)} coins: {', '.join(top)}")
            return top

    except Exception as e:
        print(f"[liq] Error fetching top coins: {e}")
        return _fallback_coins()


def _fallback_coins() -> list[str]:
    """Hardcoded fallback if API call fails."""
    return ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "SUI", "AVAX", "LINK", "ARB"]


def _parse_liquidation(trade: dict, coin: str) -> dict | None:
    """
    Parse a trade message and return event data if it qualifies.
    Note: the public trades feed does not reliably tag liquidations,
    so this monitor reports LARGE TRADES above per-coin thresholds.
    """
    side = trade.get("side", "")
    px = float(trade.get("px", 0) or 0)
    sz = float(trade.get("sz", 0) or 0)
    trade_usd = px * sz

    threshold = _get_threshold(coin)

    if trade_usd < threshold:
        return None

    return {
        "coin": coin,
        "side": side,
        "price": px,
        "size": sz,
        "usd_value": trade_usd,
        "time": trade.get("time", int(time.time() * 1000)),
        "is_liquidation": False,  # public feed can't confirm liquidations
    }


async def run_liquidation_monitor(session: aiohttp.ClientSession):
    """
    Main WebSocket loop.
    Subscribes to trades for top coins, detects large trades
    above per-coin thresholds. Reconnects automatically on disconnect.
    """
    top_coins = await fetch_top_coins(session)

    while True:
        try:
            print(f"[liq] Connecting to Hyperliquid WebSocket...")
            async with session.ws_connect(
                HYPERLIQUID_WS,
                heartbeat=None,
                timeout=aiohttp.ClientWSTimeout(ws_close=10),
            ) as ws:
                print("[liq] Connected")

                for coin in top_coins:
                    sub_msg = {
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin}
                    }
                    await ws.send_str(json.dumps(sub_msg))
                    await asyncio.sleep(0.05)

                print(f"[liq] Subscribed to {len(top_coins)} trade feeds")

                last_ping = time.time()

                async for msg in ws:
                    if time.time() - last_ping >= PING_INTERVAL:
                        await ws.send_str(json.dumps({"method": "ping"}))
                        last_ping = time.time()

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        channel = data.get("channel", "")

                        if channel in ("subscriptionResponse", "pong"):
                            continue

                        if channel != "trades":
                            continue

                        trades = data.get("data", [])
                        if not isinstance(trades, list) or not trades:
                            continue

                        coin = trades[0].get("coin", "???")

                        for trade in trades:
                            event = _parse_liquidation(trade, coin)
                            if event is None:
                                continue

                            if _is_deduped(event["coin"]):
                                continue

                            print(
                                f"[liq] 🐋 {event['coin']} | LARGE | "
                                f"${event['usd_value']:,.0f} | "
                                f"side={event['side']}"
                            )

                            await send_liquidation_alert(session, event)
                            _mark_alerted(event["coin"])
                            _cleanup_dedup()

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"[liq] WebSocket error: {ws.exception()}")
                        break

                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("[liq] WebSocket closed")
                        break

        except asyncio.CancelledError:
            print("[liq] Monitor cancelled")
            return
        except Exception as e:
            print(f"[liq] Connection error: {e}")

        print("[liq] Reconnecting in 5s...")
        await asyncio.sleep(5)