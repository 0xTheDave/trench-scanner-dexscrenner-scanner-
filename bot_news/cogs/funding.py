import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
from datetime import datetime


# Key assets to monitor — customize based on your positions
DEFAULT_COINS = ["BTC", "ETH", "SOL", "HYPE", "ARB", "OP", "SUI"]


class Funding(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def fetch_funding_rates(self) -> list:
        """
        Hyperliquid public API — no API key required.
        Returns a list of assets with funding rate data.
        """
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "metaAndAssetCtxs"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

        # data[0] = metadata (asset names)
        # data[1] = asset contexts (funding, OI, etc.)
        if not isinstance(data, list) or len(data) < 2:
            return []

        assets_meta = data[0].get("universe", [])
        assets_ctx = data[1]

        results = []
        for meta, ctx in zip(assets_meta, assets_ctx):
            name = meta.get("name", "")
            if name in DEFAULT_COINS:
                funding_rate = float(ctx.get("funding", 0))
                annualized = funding_rate * 24 * 365 * 100  # annualized %
                open_interest = float(ctx.get("openInterest", 0))
                mark_price = float(ctx.get("markPx", 0))

                results.append({
                    "name": name,
                    "funding_rate": funding_rate,
                    "annualized": annualized,
                    "open_interest": open_interest,
                    "mark_price": mark_price,
                })

        # Sort by absolute funding rate value — most interesting first
        results.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        return results

    @app_commands.command(
        name="funding",
        description="Check Hyperliquid funding rates"
    )
    async def funding_command(self, interaction: discord.Interaction):
        await interaction.response.defer()

        rates = await self.fetch_funding_rates()

        if not rates:
            await interaction.followup.send(
                "❌ Failed to fetch data from Hyperliquid. Please try again."
            )
            return

        embed = discord.Embed(
            title="📊 Hyperliquid — Funding Rates",
            color=discord.Color.blurple(),
            timestamp=datetime.utcnow(),
        )

        for asset in rates:
            rate = asset["funding_rate"]
            ann = asset["annualized"]
            price = asset["mark_price"]
            oi = asset["open_interest"]

            # Funding direction indicator
            if rate > 0.0001:
                emoji = "🔴"  # longs pay shorts — potentially overheated market
                direction = "Longs pay Shorts"
            elif rate < -0.0001:
                emoji = "🟢"  # shorts pay longs — potentially undervalued market
                direction = "Shorts pay Longs"
            else:
                emoji = "⚪"
                direction = "Neutral"

            embed.add_field(
                name=f"{emoji} {asset['name']}",
                value=(
                    f"Funding: `{rate*100:.4f}%` / 8h\n"
                    f"Annualized: `{ann:.1f}%` / year\n"
                    f"OI: `${oi:,.0f}`\n"
                    f"Mark Price: `${price:,.2f}`\n"
                    f"_{direction}_"
                ),
                inline=True,
            )

        embed.set_footer(text="TrenchBot • Hyperliquid REST API")
        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(Funding(bot))