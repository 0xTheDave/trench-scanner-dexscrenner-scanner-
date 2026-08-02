import discord
from discord.ext import commands
import asyncio
import config

# News bot only needs default intents.
# MESSAGE CONTENT intent is only required for text commands (!price) —
# slash commands work without it, so it stays disabled.
intents = discord.Intents.default()

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"[bot] Online as {bot.user}")
    print(f"[bot] Guilds: {[g.name for g in bot.guilds]}")
    try:
        synced = await bot.tree.sync()
        print(f"[bot] Synced {len(synced)} slash commands")
    except Exception as e:
        print(f"[bot] Command sync error: {e}")


async def main():
    async with bot:
        for cog in ["cogs.news_feed", "cogs.price", "cogs.funding"]:
            if cog not in bot.extensions:
                await bot.load_extension(cog)
                print(f"[bot] Loaded extension: {cog}")
        await bot.start(config.DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())