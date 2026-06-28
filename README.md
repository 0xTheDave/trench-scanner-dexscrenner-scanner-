# Trench Scanner

Discord bot that scans DexScreener, Hyperliquid, and Jupiter for early-stage tokens,
volume spikes, funding anomalies, liquidations, boosts, community takeovers, and
high-volume Jupiter tokens. Runs as a background worker — no web server required.

---

## What it does

- Scans DexScreener every 2 minutes for new token listings on Solana
- Pulls from two endpoints simultaneously: `/latest` (new listings) and `/recent-updates`
- Filters out rugs, honeypots, and low-quality tokens automatically
- Detects volume spikes via two independent methods (inline + watchlist)
- Monitors all 230 Hyperliquid perp markets for funding anomalies every 30 minutes
- Streams real-time liquidations ($100k+) via Hyperliquid WebSocket
- Alerts on boosted tokens (projects paying for DexScreener promotion)
- Alerts on community takeover events
- Monitors Jupiter for high-volume tokens ($50k+ daily routing volume)
- Posts trending narrative updates every 2 hours

---

## Project structure
trench-scanner/

├── scanner.py                    # main loop, orchestration

├── filters.py                    # all filter thresholds

├── state.py                      # in-memory dedup and volume history

├── discord_client.py             # all Discord embed formatting and webhook delivery

├── funding_monitor.py            # Hyperliquid funding rate scanner

├── liquidation_monitor.py        # Hyperliquid WebSocket liquidation stream

├── boost_monitor.py              # DexScreener boost scanner

├── community_takeover_monitor.py # DexScreener community takeover scanner

├── jupiter_monitor.py            # Jupiter high-volume token scanner

├── requirements.txt

├── Procfile

├── .env                          # local secrets (never commit)

└── README.md

---

## Architecture
scanner.py (main loop, every 120s)

├── scan_tokens()              — DexScreener /latest + /recent-updates

├── scan_watchlist()           — volume spike watchlist (every 60s)

├── scan_funding()             — Hyperliquid funding rates (every 30min)

├── scan_boosts()              — DexScreener boosts (every 5min)

├── scan_takeovers()           — DexScreener community takeovers (every 5min)

├── scan_jupiter()             — Jupiter trending tokens (every 5min)

└── scan_narratives()          — DexScreener metas (every 2h)
run_liquidation_monitor()      — Hyperliquid WebSocket (real-time, parallel)

---

## Discord channels

| Channel | Webhook env var | Source | Frequency |
|---|---|---|---|
| `#early-gems` | `DISCORD_WEBHOOK_GEMS` | DexScreener | per alert |
| `#volume-spikes` | `DISCORD_WEBHOOK_SPIKES` | DexScreener | per alert |
| `#funding-rates` | `DISCORD_WEBHOOK_FUNDING` | Hyperliquid | per alert |
| `#liquidations` | `DISCORD_WEBHOOK_LIQUIDATIONS` | Hyperliquid WS | real-time |
| `#boosted-tokens` | `DISCORD_WEBHOOK_BOOSTED` | DexScreener | per alert |
| `#community-takeovers` | `DISCORD_WEBHOOK_TAKEOVERS` | DexScreener | per alert |
| `#jupiter-volume` | `DISCORD_WEBHOOK_JUPITER` | Jupiter API | per alert |
| `#trending-metas` | `DISCORD_WEBHOOK_NARRATIVES` | DexScreener | every 2h |

---

## Data sources

| Source | Cost | What it provides |
|---|---|---|
| DexScreener API | Free, no key | Token profiles, pairs, boosts, takeovers, metas |
| Hyperliquid REST | Free, no key | Funding rates, OI, mark prices (230 markets) |
| Hyperliquid WebSocket | Free, no key | Real-time trade stream, liquidation detection |
| Jupiter API | Free, key required | Token prices, routing volume, trending tokens |

**Total cost: $0** (+ Railway $5/month which you already pay)

---

## Filters (`filters.py`)

### Token quality filters

| Filter | Default | Purpose |
|---|---|---|
| `min_liquidity_usd` | $10,000 | Removes micro-pools |
| `max_liquidity_usd` | $300,000 | Keeps focus on early stage |
| `min_volume_h24` | $30,000 | Confirms real activity |
| `min_txns_h24` | 150 | Filters single-wallet bots |
| `max_age_hours` | 48h | Only early listings |
| `min_price_change_h1` | +5% | Requires upward momentum |
| `max_buy_ratio` | 90% | Flags coordinated pumps |
| `min_buy_ratio` | 20% | Flags active dumps |
| `min_liq_to_mcap_ratio` | 1% | Catches easy-rug setups |
| `honeypot_min_buys_threshold` | 50 | 50+ buys / 0 sells = honeypot |

### Spike detection filters

| Filter | Default | Purpose |
|---|---|---|
| `spike_m5_to_h1_ratio` | 0.40 | Inline spike sensitivity |
| `spike_multiplier` | 4.0 | Watchlist spike multiplier |
| `spike_min_vol_m5` | $5,000 | Minimum vol.m5 to consider |
| `spike_min_liquidity` | $10,000 | Skip low-liq spike tokens |
| `watchlist_size` | 50 | Max tokens tracked |

### Funding alert thresholds

| Filter | Default | Purpose |
|---|---|---|
| `FUNDING_HIGH_THRESHOLD` | +0.05%/h | Overcrowded long signal |
| `FUNDING_LOW_THRESHOLD` | -0.02%/h | Overcrowded short signal |
| `OI_CHANGE_THRESHOLD` | 20% | OI spike signal |
| `MIN_OI_USD` | $1,000,000 | Ignore tiny markets |

### Boost filters

| Filter | Default | Purpose |
|---|---|---|
| `MIN_BOOST_AMOUNT` | 50 | Ignore small boosts |

### Jupiter filters

| Filter | Default | Purpose |
|---|---|---|
| `MIN_JUPITER_VOLUME` | $50,000 | Minimum 24h routing volume |

---

## Discord embeds

### `#early-gems`
CA, Price, MCap, Age, Liquidity, Vol 24h, Vol 5m, 5m/1h/6h/24h changes,
Buy/Sell ratio bar, Vol/Liq ratio, active boosts, socials, source tag.

### `#volume-spikes`
CA, spike detail (previous vs current vol.m5 or m5/h1 ratio), Price, MCap,
Liquidity, Vol 5m/1h/24h, 5m/1h changes, txns 5m, Buy/Sell 24h.

### `#funding-rates`
Signal type (HIGH/NEGATIVE/OI SPIKE), funding rate bar 🟥🟦, mark price,
OI, OI change since last scan, 24h volume, premium, Hyperliquid trade link.

### `#liquidations`
Type (Liquidation / Large trade), side (LONG/SHORT), price, size, USD value,
Hyperliquid chart link.

### `#boosted-tokens`
Tier (MEGA/HEAVY/BOOSTED/NEW), CA, description, new boosts, total boosts,
source, Price, MCap, Liquidity, Vol 24h, 1h/24h changes, Buy/Sell ratio, links.

### `#community-takeovers`
CA, claim date, description, Price, MCap, Liquidity, Vol 24h,
1h/24h changes, Buy/Sell ratio, links.

### `#jupiter-volume`
Tier (MEGA/HIGH/ACTIVE), CA, Jupiter price, Jupiter 24h routing volume,
price confidence level, tags, listed date, Jupiter swap link.

### `#trending-metas`
Top 5 narratives sorted by 1h MCap change: name, MCap, volume, 1h/24h change, token count.

---

## Setup

### Requirements

- Python 3.11+
- Eight Discord webhooks (one per channel)
- Jupiter API key (free at portal.jup.ag)

### Install

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### Environment variables

Create `.env` in the project root:
DISCORD_WEBHOOK_GEMS=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_NARRATIVES=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_SPIKES=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_FUNDING=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_LIQUIDATIONS=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_BOOSTED=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_TAKEOVERS=https://discord.com/api/webhooks/...

DISCORD_WEBHOOK_JUPITER=https://discord.com/api/webhooks/...

JUPITER_API_KEY=your_key_from_portal_jup_ag

### Run locally

```bash
python scanner.py
```

Expected output on start:
==================================================

Trench Scanner — starting

Chain: solana

Scan interval: 120s

Watchlist interval: 60s

Funding interval: 1800s

Boost interval: 300s

Takeover interval: 300s

Jupiter interval: 300s

Liquidations: WebSocket (real-time)

---

## Deploy to Railway

### First deploy

```bash
railway login
railway init
railway up
```

Set all environment variables in Railway Dashboard → Settings → Variables.

Confirm `Procfile` contains:
worker: python scanner.py

### Subsequent deploys

```bash
git add .
git commit -m "your message"
git push
```

### Cost estimate

Single lightweight worker, no database:
- RAM: ~80MB
- Estimated Railway cost: within the $5/month Hobby plan credit

---

## Tuning

| Goal | Change |
|---|---|
| Fewer gem alerts | Raise `min_liquidity_usd`, `min_volume_h24` |
| More gem alerts | Lower `min_liquidity_usd` to $8k, `min_price_change_h1` to 2% |
| Fewer spike alerts | Raise `spike_multiplier` to 6x |
| Fewer boost alerts | Raise `MIN_BOOST_AMOUNT` to 100 |
| Fewer Jupiter alerts | Raise `MIN_JUPITER_VOLUME` to $200k |
| Less rugs | Lower `max_buy_ratio` to 0.85 |
| ETH instead of Solana | Change `chain_id` to `"ethereum"` in `filters.py` |

---

## Known limitations

- **No makers/unique wallets** — DexScreener public API does not expose this field.
- **In-memory state resets on restart** — seen tokens, watchlist, volume history lost on restart.
- **No DexScreener SLA** — free API, access can be suspended without notice.
- **Jupiter keyless rate limit** — 0.5 req/s without API key; free key raises this significantly.
- **Liquidation detection** — approximated via large trade size, not a dedicated liquidation feed.

---

## Dependencies
aiohttp==3.9.5

python-dotenv

No database. No paid external services beyond Jupiter free API key.