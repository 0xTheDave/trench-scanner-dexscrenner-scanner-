# Trench Scanner

Discord bot that scans DexScreener, Hyperliquid, and Jupiter for early-stage tokens,
volume spikes, funding anomalies, large trades, boosts, community takeovers,
Jupiter volume entrants, and Robinhood Chain gems.
Runs as a background worker — no web server required.

---

## What it does

- Scans DexScreener every 2 minutes for new token listings on Solana
- Scans Robinhood Chain (new Arbitrum-based L2) every 2 minutes with dedicated filters
- Pulls from two endpoints simultaneously: `/latest` (new listings) and `/recent-updates`
- Filters out rugs, honeypots, wash trading, and low-quality tokens automatically
- Detects volume spikes via two independent methods (inline + watchlist)
- Monitors all 230 Hyperliquid perp markets for funding anomalies every 30 minutes
- Streams large trades real-time via Hyperliquid WebSocket (per-coin thresholds)
- Alerts on boosted tokens — re-alerts only when total boost count grows
- Alerts on community takeover events
- Alerts when a token newly enters Jupiter's top 20 by routing volume
- Posts trending narrative updates every 2 hours

---

## Project structure
trench-scanner/
├── scanner.py                    # main loop, orchestration
├── filters.py                    # all Solana filter thresholds
├── state.py                      # in-memory dedup and volume history
├── discord_client.py             # all Discord embed formatting and webhook delivery
├── funding_monitor.py            # Hyperliquid funding rate scanner
├── liquidation_monitor.py        # Hyperliquid WebSocket large trade stream
├── boost_monitor.py              # DexScreener boost scanner
├── community_takeover_monitor.py # DexScreener community takeover scanner
├── jupiter_monitor.py            # Jupiter top-20 volume entrant scanner
├── robinhood_monitor.py          # Robinhood Chain gem scanner (self-contained)
├── requirements.txt
├── Procfile
├── .env                          # local secrets (never commit)
└── README.md

---

## Architecture
scanner.py (main loop, every 120s)
├── scan_tokens()              — DexScreener Solana /latest + /recent-updates
├── scan_robinhood()           — Robinhood Chain gems (every 2min)
├── scan_watchlist()           — volume spike watchlist (every 60s)
├── scan_funding()             — Hyperliquid funding rates (every 30min)
├── scan_boosts()              — DexScreener boosts (every 5min)
├── scan_takeovers()           — DexScreener community takeovers (every 5min)
├── scan_jupiter()             — Jupiter top-20 entrants (every 5min)
└── scan_narratives()          — DexScreener metas (every 2h)
run_liquidation_monitor()      — Hyperliquid WebSocket (real-time, parallel)

---

## Discord channels

| Channel | Webhook env var | Source | Frequency |
|---|---|---|---|
| `#early-gems` | `DISCORD_WEBHOOK_GEMS` | DexScreener | per alert |
| `#robinhood-gems` | `DISCORD_WEBHOOK_ROBINHOOD` | DexScreener | per alert |
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
| DexScreener API | Free, no key | Token profiles, pairs, boosts, takeovers, metas (Solana + Robinhood Chain) |
| Hyperliquid REST | Free, no key | Funding rates, OI, mark prices (230 markets) |
| Hyperliquid WebSocket | Free, no key | Real-time trade stream, large trade detection |
| Jupiter API | Free, key required | Token prices (v3), routing volume, top tokens |

**Total cost: $0** (+ Railway $5/month which you already pay)

---

## Filters

### Solana token quality filters (`filters.py`)

| Filter | Default | Purpose |
|---|---|---|
| `min_liquidity_usd` | $10,000 | Removes micro-pools |
| `max_liquidity_usd` | $300,000 | Keeps focus on early stage |
| `min_volume_h24` | $30,000 | Confirms real activity |
| `min_txns_h24` | 150 | Filters single-wallet bots |
| `max_age_hours` | 48h | Only early listings |
| `min_price_change_h1` | +5% | Momentum for pairs older than 1h |
| `min_price_change_m5_fresh` | +2% | Momentum (5m) for pairs younger than 1h |
| `min_mcap` | $50,000 | Below this = too small/dead |
| `max_mcap` | $10,000,000 | Above this = not early anymore |
| `max_vol_to_liq_ratio` | 50x | Volume far above liquidity = wash trading |
| `max_buy_ratio` | 90% | Flags coordinated pumps |
| `min_buy_ratio` | 20% | Flags active dumps |
| `min_liq_to_mcap_ratio` | 1% | Catches easy-rug setups |
| `honeypot_min_buys_threshold` | 50 | 50+ buys / 0 sells = honeypot |

**Fresh-pair momentum logic:** pairs younger than 1 hour are judged on 5-minute
price change instead of 1-hour change, so brand-new listings are not rejected
for having no 1h history yet.

### Robinhood Chain filters (`robinhood_monitor.py`, separate & looser)

| Filter | Default | Why looser |
|---|---|---|
| `min_liquidity_usd` | $5,000 | Chain is brand new |
| `max_age_hours` | 72h | Fewer listings overall |
| `min_price_change_h1` | 0% | No momentum requirement yet |

### Spike detection filters

| Filter | Default | Purpose |
|---|---|---|
| `spike_m5_to_h1_ratio` | 0.40 | Inline spike sensitivity |
| `spike_multiplier` | 4.0 | Watchlist spike multiplier |
| `spike_min_vol_m5` | $5,000 | Minimum vol.m5 to consider |
| `spike_min_liquidity` | $10,000 | Skip low-liq spike tokens |
| `watchlist_size` | 50 | Max tokens tracked |

### Funding alert thresholds (`funding_monitor.py`)

| Filter | Default | Purpose |
|---|---|---|
| `FUNDING_HIGH_THRESHOLD` | +0.05%/h | Overcrowded long signal |
| `FUNDING_LOW_THRESHOLD` | -0.02%/h | Overcrowded short signal |
| `OI_CHANGE_THRESHOLD` | 20% | OI spike signal |
| `MIN_OI_USD` | $1,000,000 | Ignore tiny markets |
| `MIN_DAY_VOLUME` | $500,000 | Skip high-OI but dead markets |

### Large trade thresholds (`liquidation_monitor.py`, per-coin)

| Coin | Threshold |
|---|---|
| BTC | $2,000,000 |
| ETH | $1,000,000 |
| SOL | $500,000 |
| All others | $200,000 |

Dedup: same coin won't alert twice within 5 minutes.
Note: the public Hyperliquid trades feed does not tag liquidations,
so this channel honestly reports LARGE TRADES above thresholds.

### Boost filters (`boost_monitor.py`)

| Filter | Default | Purpose |
|---|---|---|
| `MIN_BOOST_AMOUNT` | 50 | Ignore small boosts |
| Re-alert rule | totalAmount grew | Same token alerts again only if boosts increased |

### Jupiter filters (`jupiter_monitor.py`)

| Filter | Default | Purpose |
|---|---|---|
| `MIN_JUPITER_VOLUME` | $50,000 | Minimum 24h routing volume |
| Alert rule | New top-20 entrant only | First scan sets baseline, no alerts |
| `DEDUP_TTL` | 24h | Same token won't re-alert within a day |

---

## Setup

### Requirements

- Python 3.11+
- Nine Discord webhooks (one per channel)
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
DISCORD_WEBHOOK_ROBINHOOD=https://discord.com/api/webhooks/...
JUPITER_API_KEY=your_key_from_portal_jup_ag

### Run locally

```bash
python scanner.py
```

Expected output on start:
==================================================
Trench Scanner — starting
Chain: solana + robinhood
Scan interval: 120s
Robinhood interval: 120s
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
| Fewer large trade alerts | Raise per-coin thresholds in `COIN_THRESHOLDS` |
| Fewer Jupiter alerts | Raise `MIN_JUPITER_VOLUME` to $200k |
| Less rugs | Lower `max_buy_ratio` to 0.85 |
| Robinhood too noisy | Raise `RH_FILTERS` thresholds in `robinhood_monitor.py` |

---

## Known limitations

- **No makers/unique wallets** — DexScreener public API does not expose this field.
- **In-memory state resets on restart** — seen tokens, watchlist, volume history,
  Jupiter baseline, and boost tracking are lost on restart.
- **No DexScreener SLA** — free API, access can be suspended without notice.
- **Large trades ≠ confirmed liquidations** — Hyperliquid's public trades feed
  does not tag liquidations; the channel reports large trades honestly.
- **Robinhood chainId assumption** — expected `"robinhood"`; the scanner logs all
  chainIds seen in the API on first run for verification.

---

## Dependencies
aiohttp==3.9.5
python-dotenv

No database. No paid external services beyond Jupiter free API key.