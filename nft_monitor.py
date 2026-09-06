# nft_monitor.py
# Three NFT signals from the OpenSea API v2, one Discord channel each:
# 1. scan_nft_new_collections -> DISCORD_WEBHOOK_NFT_NEW
#    New collections per chain (created_date leaderboard), quality-gated.
# 2. scan_nft_volume_spikes   -> DISCORD_WEBHOOK_NFT_SPIKES
#    New entrants into the one_day_volume top-20 per chain (big money).
# 3. scan_nft_trending        -> DISCORD_WEBHOOK_NFT_TRENDING
#    New entrants into the one_day_change top-15 per chain (momentum).
#
# Design notes:
# - Chain list is dynamic (fetched from /chains, cached 24h in kv).
# - One chain per cycle per task (rotation), separate rotation indexes.
# - First cycle per chain initializes a baseline and sends NO alerts;
#   baselines persist in kv, so restarts do not cause alert floods.
# - Hard caps per cycle as flood guards; entrants above the cap are left
#   out of the baseline and picked up next rotation.
# - Dedup is keyed on contract address, never on name/slug.
# - No db.record_alert here in v1: DexScreener cannot price NFT
#   collections; floor-based tracking is a separate future checkpoint.
#
# v1.1 (2026-08-10): NEW_MIN_ITEMS junk floor; failed stats lookups are
#   baked into the baseline so they stop burning the per-cycle budget.
# v1.2 (2026-08-10): #nft-trending rebuilt on order_by=one_day_change
#   (collections, not fungible tokens — the old /tokens/trending endpoint
#   is OpenSea's DEX aggregator product and returned memecoins). Richer
#   embeds: full socials line (X/Discord/Telegram/Instagram/Website/Wiki),
#   royalty %, created date, banner image, linked title, footer+timestamp.
#   New collections deliberately skip the stats call: stats for brand-new
#   collections are all zeros (verified live), so floor/owners would waste
#   one request per alert on empty fields.

import os
import time
from datetime import datetime, timezone

import aiohttp

import db
import opensea_client

# --- Intervals are set in scanner.py; knobs below are signal thresholds ---

NEW_COLLECTIONS_LIMIT = 50
MAX_NEW_ALERTS_PER_CYCLE = 3
MAX_NEW_DETAIL_LOOKUPS_PER_CYCLE = 6  # request budget guard
NEW_SEEN_TTL_SECONDS = 30 * 86_400
NEW_MIN_ITEMS = 10                    # junk floor (verified exempt)

SPIKES_LEADERBOARD_SIZE = 20
SPIKES_MIN_ONE_DAY_SALES = 8          # anti-churn: entrant must have real sales
MAX_STATS_LOOKUPS_PER_CYCLE = 3       # request budget guard
MAX_SPIKE_ALERTS_PER_CYCLE = 3

TRENDING_LEADERBOARD_SIZE = 15
TRENDING_MIN_ONE_DAY_SALES = 5        # momentum gate: % change needs real sales
MAX_TRENDING_STATS_LOOKUPS_PER_CYCLE = 3
MAX_TRENDING_ALERTS_PER_CYCLE = 3

CHAINS_CACHE_TTL_SECONDS = 86_400
EMPTY_CHAIN_SKIP_SECONDS = 86_400

# LP-position / infrastructure NFTs masquerading as collections
# (observed live: "DLMM Token", "Voting Escrow DUST" at the top of
# created_date results). Conservative substring blocklist.
_JUNK_PATTERNS = (
    "dlmm", "position", "uniswap", "pancakeswap", "liquidity",
    "lp token", "vesting", "escrow",
)

_COLOR_NEW = 0x5865F2
_COLOR_SPIKE = 0xF1C40F
_COLOR_TRENDING = 0xE67E22
_FOOTER = {"text": "Trench Scanner • NFT Radar"}


# === Shared helpers ===

async def _get_chains(session: aiohttp.ClientSession) -> list[str]:
    cached = db.kv_get_json("nft_chains_cache")
    now = time.time()
    if cached and cached.get("chains") and now - cached.get("ts", 0) < CHAINS_CACHE_TTL_SECONDS:
        return cached["chains"]
    chains = await opensea_client.fetch_chains(session)
    if chains:
        db.kv_set_json("nft_chains_cache", {"ts": now, "chains": chains})
        print(f"[nft] Chain list refreshed: {len(chains)} chains")
        return chains
    # Fall back to a stale cache rather than doing nothing
    return cached.get("chains", []) if cached else []


def _chain_skipped(chain: str) -> bool:
    ts = db.kv_get_json(f"nft_chain_empty:{chain}")
    return bool(ts) and (time.time() - ts) < EMPTY_CHAIN_SKIP_SECONDS


def _pick_chain(chains: list[str], rotation_key: str) -> str | None:
    """Advance the rotation index, skipping chains marked empty."""
    if not chains:
        return None
    idx = db.kv_get_json(rotation_key, 0) or 0
    for _ in range(len(chains)):
        chain = chains[idx % len(chains)]
        idx += 1
        if not _chain_skipped(chain):
            db.kv_set_json(rotation_key, idx % len(chains))
            return chain
    db.kv_set_json(rotation_key, idx % len(chains))
    return None


def _quality_gates(col: dict) -> tuple[bool, str]:
    if col["is_disabled"]:
        return False, "disabled"
    if col["is_nsfw"]:
        return False, "nsfw"
    if not col["image_url"]:
        return False, "no_image"
    if len(col["name"]) < 3:
        return False, "name_too_short"
    lowered = f"{col['name']} {col['description']}".lower()
    for pattern in _JUNK_PATTERNS:
        if pattern in lowered:
            return False, f"junk:{pattern}"
    return True, ""


def _socials_line(col: dict) -> str:
    links = [f"[OpenSea]({col['opensea_url']})"]
    if col["twitter_username"]:
        links.append(f"[X](https://x.com/{col['twitter_username']})")
    if col["discord_url"]:
        links.append(f"[Discord]({col['discord_url']})")
    if col["telegram_url"]:
        links.append(f"[Telegram]({col['telegram_url']})")
    if col["instagram_username"]:
        links.append(f"[Instagram](https://instagram.com/{col['instagram_username']})")
    if col["project_url"]:
        links.append(f"[Website]({col['project_url']})")
    if col["wiki_url"]:
        links.append(f"[Wiki]({col['wiki_url']})")
    return " · ".join(links)


def _base_embed(col: dict, title_prefix: str, color: int) -> dict:
    verified = " ✅" if col["safelist_status"] == "verified" else ""
    embed = {
        "title": f"{title_prefix} {col['name']}{verified}",
        "url": col["opensea_url"] or None,
        "color": color,
        "fields": [],
        "footer": _FOOTER,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if col["description"]:
        embed["description"] = col["description"][:300]
    if col["image_url"]:
        embed["thumbnail"] = {"url": col["image_url"]}
    if col["banner_image_url"]:
        embed["image"] = {"url": col["banner_image_url"]}
    return embed


async def _send_embed(
    session: aiohttp.ClientSession, env_var: str, embed: dict
) -> int:
    webhook_url = os.environ.get(env_var, "")
    if not webhook_url:
        print(f"[nft] {env_var} not set — alert dropped")
        return 0
    try:
        async with session.post(
            webhook_url,
            json={"embeds": [embed]},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status not in (200, 204):
                print(f"[nft] Webhook {env_var} HTTP {resp.status}")
            return resp.status
    except Exception as exc:
        print(f"[nft] Webhook error: {type(exc).__name__}: {exc}")
        return 0


# === Signal 1: new collections ===

async def scan_nft_new_collections(session: aiohttp.ClientSession):
    chains = await _get_chains(session)
    chain = _pick_chain(chains, "nft_rotation_new")
    if chain is None:
        return

    cols = await opensea_client.fetch_collections(
        session, chain, "created_date", NEW_COLLECTIONS_LIMIT
    )
    if cols is None:
        return  # request failed — leave baseline untouched
    if not cols:
        db.kv_set_json(f"nft_chain_empty:{chain}", time.time())
        print(f"[nft-new] {chain}: empty, skipping chain for 24h")
        return

    baseline_key = f"nft_new_baseline:{chain}"
    baseline = db.kv_get_json(baseline_key)
    current_addresses = [c["address"] for c in cols if c["address"]]

    if baseline is None:
        db.kv_set_json(baseline_key, current_addresses)
        print(f"[nft-new] {chain}: baseline initialized ({len(current_addresses)})")
        return

    baseline_set = set(baseline)
    seen_map = db.load_seen("nft_new", NEW_SEEN_TTL_SECONDS)
    processed: set[str] = set()
    detail_lookups = 0
    alerts_sent = 0

    for col in cols:
        addr = col["address"]
        if not addr or addr in baseline_set or addr in seen_map:
            processed.add(addr)
            continue
        if alerts_sent >= MAX_NEW_ALERTS_PER_CYCLE:
            break  # leave the rest out of the baseline -> next rotation
        if detail_lookups >= MAX_NEW_DETAIL_LOOKUPS_PER_CYCLE:
            break

        passed, reason = _quality_gates(col)
        if not passed:
            print(f"[nft-new] {chain}: skip '{col['name'][:30]}' ({reason})")
            db.save_seen("nft_new", addr)
            processed.add(addr)
            continue

        details = await opensea_client.fetch_collection_details(session, col["slug"])
        detail_lookups += 1
        item_count = details["item_count"] if details else 0
        currency = details["listing_currency"] if details else ""
        royalty_pct = details["royalty_pct"] if details else 0.0
        created_date = details["created_date"] if details else ""

        # Collection size floor — single-piece spam mints dominate the
        # created_date leaderboard on ethereum/polygon (observed live).
        if item_count < NEW_MIN_ITEMS and col["safelist_status"] != "verified":
            print(f"[nft-new] {chain}: skip '{col['name'][:30]}' (items_low {item_count})")
            db.save_seen("nft_new", addr)
            processed.add(addr)
            continue

        embed = _base_embed(col, "🆕", _COLOR_NEW)
        embed["fields"] = [
            {"name": "Chain", "value": chain, "inline": True},
            {"name": "Items", "value": str(item_count) if item_count else "?", "inline": True},
            {"name": "Category", "value": col["category"] or "—", "inline": True},
            {"name": "Royalty", "value": f"{royalty_pct:.1f}%", "inline": True},
            {"name": "Currency", "value": currency or "—", "inline": True},
            {"name": "Created", "value": created_date or "—", "inline": True},
            {"name": "Contract", "value": f"`{addr}`", "inline": False},
            {"name": "Links", "value": _socials_line(col), "inline": False},
        ]

        status = await _send_embed(session, "DISCORD_WEBHOOK_NFT_NEW", embed)
        db.save_seen("nft_new", addr)
        processed.add(addr)
        if status in (200, 204):
            alerts_sent += 1
            print(f"[nft-new] ✅ {col['name']} [{chain}] items={item_count}")

    new_baseline = [a for a in current_addresses if a in baseline_set or a in processed]
    db.kv_set_json(baseline_key, new_baseline)
    db.cleanup_seen("nft_new", NEW_SEEN_TTL_SECONDS)
    if alerts_sent:
        print(f"[nft-new] {chain}: done, alerts={alerts_sent}")


# === Shared leaderboard-entrant scanner (signals 2 and 3) ===

async def _scan_leaderboard_entrants(
    session: aiohttp.ClientSession,
    tag: str,
    rotation_key: str,
    baseline_prefix: str,
    order_by: str,
    leaderboard_size: int,
    min_one_day_sales: int,
    max_stats_lookups: int,
    max_alerts: int,
    webhook_env: str,
    title_prefix: str,
    color: int,
):
    chains = await _get_chains(session)
    chain = _pick_chain(chains, rotation_key)
    if chain is None:
        return

    cols = await opensea_client.fetch_collections(
        session, chain, order_by, leaderboard_size
    )
    if cols is None:
        return
    if not cols:
        db.kv_set_json(f"nft_chain_empty:{chain}", time.time())
        return

    baseline_key = f"{baseline_prefix}:{chain}"
    baseline = db.kv_get_json(baseline_key)
    current_addresses = [c["address"] for c in cols if c["address"]]

    if baseline is None:
        db.kv_set_json(baseline_key, current_addresses)
        print(f"[{tag}] {chain}: baseline initialized ({len(current_addresses)})")
        return

    baseline_set = set(baseline)
    entrants = [c for c in cols if c["address"] and c["address"] not in baseline_set]

    stats_lookups = 0
    alerts_sent = 0
    accepted: set[str] = set()
    rejected: set[str] = set()

    for col in entrants:
        if stats_lookups >= max_stats_lookups:
            break  # unchecked entrants stay out of baseline -> next rotation
        if alerts_sent >= max_alerts:
            break

        stats = await opensea_client.fetch_collection_stats(session, col["slug"])
        stats_lookups += 1
        if not stats:
            # Bake failed lookups (e.g. permanent 404) into the baseline
            # so they stop burning the per-cycle stats budget.
            rejected.add(col["address"])
            continue

        if stats["one_day_sales"] < min_one_day_sales or stats["one_day_volume"] <= 0:
            rejected.add(col["address"])  # goes into baseline: don't re-check churn
            continue

        symbol = stats["floor_symbol"] or "native"
        embed = _base_embed(col, title_prefix, color)
        embed["fields"] = [
            {"name": "Chain", "value": chain, "inline": True},
            {"name": "Vol 24h", "value": f"{stats['one_day_volume']:,.3f} {symbol}", "inline": True},
            {"name": "Sales 24h", "value": str(stats["one_day_sales"]), "inline": True},
            {"name": "Floor", "value": f"{stats['floor_price']:,.4f} {symbol}", "inline": True},
            {"name": "Owners", "value": str(stats["num_owners"]), "inline": True},
            {"name": "Vol 7d", "value": f"{stats['seven_day_volume']:,.3f} {symbol}", "inline": True},
            {"name": "Contract", "value": f"`{col['address']}`", "inline": False},
            {"name": "Links", "value": _socials_line(col), "inline": False},
        ]

        status = await _send_embed(session, webhook_env, embed)
        if status in (200, 204):
            accepted.add(col["address"])
            alerts_sent += 1
            print(
                f"[{tag}] ✅ {col['name']} [{chain}] "
                f"vol={stats['one_day_volume']:,.3f} {symbol} sales={stats['one_day_sales']}"
            )

    new_baseline = [
        a for a in current_addresses
        if a in baseline_set or a in accepted or a in rejected
    ]
    db.kv_set_json(baseline_key, new_baseline)


# === Signal 2: volume leaderboard entrants (big money) ===

async def scan_nft_volume_spikes(session: aiohttp.ClientSession):
    await _scan_leaderboard_entrants(
        session,
        tag="nft-spikes",
        rotation_key="nft_rotation_spikes",
        baseline_prefix="nft_vol_baseline",
        order_by="one_day_volume",
        leaderboard_size=SPIKES_LEADERBOARD_SIZE,
        min_one_day_sales=SPIKES_MIN_ONE_DAY_SALES,
        max_stats_lookups=MAX_STATS_LOOKUPS_PER_CYCLE,
        max_alerts=MAX_SPIKE_ALERTS_PER_CYCLE,
        webhook_env="DISCORD_WEBHOOK_NFT_SPIKES",
        title_prefix="📈",
        color=_COLOR_SPIKE,
    )


# === Signal 3: momentum leaderboard entrants (biggest 24h change) ===

async def scan_nft_trending(session: aiohttp.ClientSession):
    await _scan_leaderboard_entrants(
        session,
        tag="nft-trending",
        rotation_key="nft_rotation_trending",
        baseline_prefix="nft_trend_baseline",
        order_by="one_day_change",
        leaderboard_size=TRENDING_LEADERBOARD_SIZE,
        min_one_day_sales=TRENDING_MIN_ONE_DAY_SALES,
        max_stats_lookups=MAX_TRENDING_STATS_LOOKUPS_PER_CYCLE,
        max_alerts=MAX_TRENDING_ALERTS_PER_CYCLE,
        webhook_env="DISCORD_WEBHOOK_NFT_TRENDING",
        title_prefix="🔥",
        color=_COLOR_TRENDING,
    )