# Trench Scanner

Discord bot that scans DexScreener for early-stage tokens and volume spikes, posting alerts to dedicated Discord channels.
Runs as a background worker — no web server required.

---

## What it does

- Scans DexScreener every 2 minutes for new token listings on Solana
- Pulls from two endpoints simultaneously: `/latest` (new listings) and `/recent-updates` (tokens gaining activity)
- Filters out rugs, honeypots, and low-quality tokens automatically
- Detects volume spikes via two independent methods (inline + watchlist)
- Posts formatted alerts to three Discord channels
- Posts trending narrative updates every 2 hours

---

## Project structure
trench-scanner/

├── scanner.py          # main loop, fetching, orchestration

├── filters.py          # all filter thresholds in one place

├── state.py            # in-memory dedup and volume history

├── discord_client.py   # Discord embed formatting and webhook delivery

├── requirements.txt    # dependencies

├── Procfile            # Railway worker definition

├── .env                # local secrets (never commit)

└── README.md

---

## How it works

### Main scan loop (every 120s)

Fetch /token-profiles/latest/v1         ─┐
Fetch /token-profiles/recent-updates/v1  ┘ in parallel
Merge + deduplicate by token address
For each unseen token:

a. Fetch /token-pairs/v1/solana/{address}

b. Pick pair with highest liquidity

c. Calculate age from pairCreatedAt

d. Check inline spike (Approach B)

e. Run passes_filters()

f. If passes → add to watchlist → send gem alert → mark seen

g. If rejected → log reason → mark seen


### Watchlist scan (every 60s)

Re-fetch current pair data for all watchlist tokens
Compare current vol.m5 to previous snapshot (VolumeHistory)
If vol.m5 grew >= 4x since last scan → send spike alert
Update snapshot in VolumeHistory


### Narrative scan (every 2h)

Fetch /metas/trending/v1
Sort by 1h market cap change
Post top 5 narratives to #trending-metas


---

## Volume spike detection

Two independent methods run in parallel:

### Approach B — Inline spike (new tokens)
Triggers at first scan of a new token.
Checks if `vol.m5 / vol.h1 >= 0.40` — meaning 40%+ of the hourly volume just happened in the last 5 minutes.
No history needed. Fires immediately on discovery.

### Approach A — Watchlist spike (known tokens)
Tokens that passed gem filters are added to a watchlist (top 50 by liquidity).
Every 60s the scanner re-fetches their data and compares current `vol.m5` to the previous snapshot.
If `vol.m5 >= 4x` the previous value → spike alert.
Catches pumps on tokens already in your watchlist that weren't new at discovery time.

### Spike dedup
Same token cannot trigger a spike alert more than once per 10 minutes (`seen_spikes` TTL = 600s).

---

## Discord channels

| Channel | Webhook env var | What posts there |
|---|---|---|
| `#early-gems` | `DISCORD_WEBHOOK_GEMS` | New tokens passing all filters |
| `#volume-spikes` | `DISCORD_WEBHOOK_SPIKES` | Spike alerts (both methods) |
| `#trending-metas` | `DISCORD_WEBHOOK_NARRATIVES` | Top 5 narratives every 2h |

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

### Rejection log examples
[filter] SCAM rejected — honeypot 120 buys / 0 sells

[filter] RUG  rejected — rug_risk liq/mcap=0.31%

[filter] DEAD rejected — no_momentum -17.9%

[filter] MEH  rejected — vol_low $4,200

[filter] OLD  rejected — too_old 72.3h

[filter] DUMP rejected — dump 8% buys

[filter] PUMP rejected — sus_buys 94%

---

## Discord embeds

### `#early-gems`

| Field | Description |
|---|---|
| CA | Full contract address — click to select and copy |
| Price / MCap / Age | Basic token stats |
| Liquidity / Vol 24h / Vol 5m | Market depth and activity |
| 5m / 1h / 6h / 24h | Price changes across timeframes |
| Buy/Sell ratio | Visual bar 🟢🔴 |
| Vol/Liq ratio | Organic activity signal |
| Boosts | Active DexScreener boosts |
| Socials | Twitter, Telegram, Website links |
| Source | 🆕 New listing or 🔄 Recent update |

### `#volume-spikes`

| Field | Description |
|---|---|
| CA | Full contract address |
| Spike detail | Type + previous vs current vol.m5 or m5/h1 ratio |
| Price / MCap / Liquidity | Market snapshot at spike time |
| Vol 5m / 1h / 24h | Volume breakdown |
| 5m / 1h price change | Momentum at spike time |
| Txns 5m | Buy/sell count in last 5 minutes |
| Buy/Sell 24h | Overall ratio bar |
| Socials | Links |

---

## Setup

### Requirements

- Python 3.11+
- Three Discord webhooks

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
DISCORD_WEBHOOK_GEMS=https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN

DISCORD_WEBHOOK_NARRATIVES=https://discord.com/api/webhooks/YOUR_ID2/YOUR_TOKEN2

DISCORD_WEBHOOK_SPIKES=https://discord.com/api/webhooks/YOUR_ID3/YOUR_TOKEN3

To get a webhook URL:
Discord server → channel settings → Integrations → Webhooks → New Webhook → Copy URL

### Run locally

```bash
python scanner.py
```

Expected output:
==================================================

Trench Scanner — starting

Chain: solana

Scan interval: 120s

Watchlist interval: 60s
[scan] 16:44:53 — scanning tokens...

[scan] Profiles to check: 25 (latest + recent-updates)

[filter] RUG rejected — rug_risk liq/mcap=0.12%

[alert] ✅ $TOKEN [latest] | age=0.2h | liq=$12,560

[scan] Done. Alerts: 1, cleaned: 0 entries

[watchlist] Scanning 1 tokens...

[watchlist] Done. Spikes found: 0, tracking: 1 tokens

[narratives] Sent 5 narratives

[main] Waiting 120s...

---

## Deploy to Railway

### First deploy

```bash
railway login
railway init
railway up
```

Set environment variables in Railway Dashboard → Settings → Variables:
- `DISCORD_WEBHOOK_GEMS`
- `DISCORD_WEBHOOK_NARRATIVES`
- `DISCORD_WEBHOOK_SPIKES`

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
- RAM: ~60MB
- Estimated Railway cost: within the $5/month Hobby plan credit

---

## Tuning

| Goal | Change |
|---|---|
| Fewer gem alerts | Raise `min_liquidity_usd`, `min_volume_h24` |
| More gem alerts | Lower `min_liquidity_usd` to $8k, `min_price_change_h1` to 2% |
| Fewer spike alerts | Raise `spike_multiplier` to 6x, raise `spike_min_vol_m5` |
| More spike alerts | Lower `spike_multiplier` to 3x, lower `spike_m5_to_h1_ratio` to 0.3 |
| Less rugs | Lower `max_buy_ratio` to 0.85, raise `min_liq_to_mcap_ratio` to 0.02 |
| ETH instead of Solana | Change `chain_id` to `"ethereum"` in `filters.py` |
| Faster scans | Lower `SCAN_INTERVAL_SECONDS` (min ~60s) |

---

## Known limitations

- **No makers/unique wallets** — DexScreener public API does not expose this field. Bundle detection approximated via buy ratio and vol/liq ratio only.
- **In-memory state resets on restart** — seen tokens, watchlist, and volume history are lost on process restart. Add Redis or SQLite if persistence is needed.
- **No DexScreener SLA** — free API, access can be suspended without notice.
- **Solana only by default** — change `chain_id` in `filters.py` for other chains.

---

## Dependencies
aiohttp==3.9.5

python-dotenv

No database. No external paid services. Only DexScreener API and Discord webhooks.