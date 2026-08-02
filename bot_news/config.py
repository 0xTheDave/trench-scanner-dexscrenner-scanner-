import os

# load_dotenv() only runs locally when a .env file is present.
# On Railway env vars come from the Variables tab, so a missing
# dotenv package or .env file is not an error.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
DISCORD_GUILD_ID = int(os.environ.get("DISCORD_GUILD_ID", 0))
NEWS_CHANNEL_ID = int(os.environ.get("NEWS_CHANNEL_ID", 0))
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

print(
    f"[config] token={'set' if DISCORD_TOKEN else 'MISSING'} | "
    f"guild_id={'set' if DISCORD_GUILD_ID else 'MISSING'} | "
    f"news_channel={'set' if NEWS_CHANNEL_ID else 'MISSING'}"
)