# filters.py

FILTERS = {
    # Liquidity
    "min_liquidity_usd": 10_000,
    "max_liquidity_usd": 300_000,

    # Volume
    "min_volume_h24": 30_000,

    # Transactions
    "min_txns_h24": 150,

    # Age
    "max_age_hours": 48,

    # Momentum
    "min_price_change_h1": 5.0,

    # Anti-rug: max % buys out of total txns — >90% buys = suspicious
    "max_buy_ratio": 0.90,

    # Anti-rug: min % buys — <20% buys = dump in progress
    "min_buy_ratio": 0.20,

    # Anti-rug: min liquidity as % of mcap — <1% = rug risk
    "min_liq_to_mcap_ratio": 0.01,

    # Honeypot: if buys >= threshold and sells == 0 = can't sell
    "honeypot_min_buys_threshold": 50,

    # Chain
    "chain_id": "solana",

    # Dedup TTL in seconds
    "dedup_ttl_seconds": 86_400,

    # === Volume spike detection ===

    # Approach B — inline spike signal on new tokens:
    # vol.m5 / vol.h1 > this ratio = most of hourly volume just happened
    "spike_m5_to_h1_ratio": 0.40,

    # Approach A — watchlist spike detection:
    # current vol.m5 must be X times higher than previous snapshot
    "spike_multiplier": 4.0,

    # Minimum vol.m5 in USD to consider a spike worth alerting
    "spike_min_vol_m5": 5_000,

    # Minimum liquidity for watchlist tokens to avoid alerting on rugs
    "spike_min_liquidity": 10_000,

    # How many top tokens to track in watchlist (by liquidity)
    "watchlist_size": 50,
}