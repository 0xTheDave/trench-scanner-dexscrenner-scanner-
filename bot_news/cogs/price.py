import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
from datetime import datetime
import config


class Price(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def fetch_price(self, coin_id: str) -> dict | None:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": coin_id,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
        }
        headers = {}
        if config.COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = config.COINGECKO_API_KEY

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get(coin_id)

    @app_commands.command(name="price", description="Sprawdź cenę kryptowaluty")
    @app_commands.describe(coin="Symbol lub nazwa monety (np. btc, eth, sol)")
    async def price_command(self, interaction: discord.Interaction, coin: str):
        await interaction.response.defer()

        symbol_map = {
            "btc": "bitcoin",
            "eth": "ethereum",
            "sol": "solana",
            "bnb": "binancecoin",
            "avax": "avalanche-2",
            "link": "chainlink",
            "op": "optimism",
            "arb": "arbitrum",
            "sui": "sui",
            "apt": "aptos",
            "hype": "hyperliquid",
        }

        coin_id = symbol_map.get(coin.lower(), coin.lower())
        data = await self.fetch_price(coin_id)

        if data is None:
            await interaction.followup.send(
                f"❌ Nie znaleziono `{coin}`. Użyj symbolu (btc, eth, sol) lub pełnego ID z CoinGecko."
            )
            return

        price = data.get("usd", 0)
        change_24h = data.get("usd_24h_change", 0)
        market_cap = data.get("usd_market_cap", 0)
        volume_24h = data.get("usd_24h_vol", 0)

        color = discord.Color.green() if change_24h >= 0 else discord.Color.red()
        arrow = "🟢 ▲" if change_24h >= 0 else "🔴 ▼"

        embed = discord.Embed(
            title=f"💰 {coin_id.upper()} / USD",
            color=color,
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Cena", value=f"**${price:,.4f}**", inline=True)
        embed.add_field(name="24h zmiana", value=f"{arrow} {change_24h:+.2f}%", inline=True)
        embed.add_field(name="Market Cap", value=f"${market_cap:,.0f}" if market_cap else "N/A", inline=True)
        embed.add_field(name="Volume 24h", value=f"${volume_24h:,.0f}" if volume_24h else "N/A", inline=True)
        embed.set_footer(text="TrenchBot • CoinGecko")

        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(Price(bot))