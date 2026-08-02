import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import re
from datetime import datetime
import config

# Zestaw już wysłanych newsów — unikamy duplikatów po URL
sent_news_ids = set()


class NewsFeed(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.auto_feed.start()

    def cog_unload(self):
        self.auto_feed.cancel()

    # ─── RSS Feeds — crypto newsy bez klucza ──────────────────────────────────

    async def fetch_crypto_news(self) -> list:
        feeds = [
            "https://decrypt.co/feed",
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
        ]

        for feed_url in feeds:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        feed_url,
                        timeout=aiohttp.ClientTimeout(total=10),
                        headers={"User-Agent": "TrenchBot/1.0"}
                    ) as resp:
                        if resp.status != 200:
                            print(f"❌ RSS błąd {feed_url}: {resp.status}")
                            continue
                        text = await resp.text()

                root = ET.fromstring(text)
                channel = root.find("channel")
                if channel is None:
                    continue

                items = []
                for item in channel.findall("item")[:5]:
                    title = item.findtext("title", "").strip()
                    link = item.findtext("link", "").strip()
                    pub_date = item.findtext("pubDate", "")[:16]
                    description = item.findtext("description", "")
                    description = re.sub(r"<[^>]+>", "", description)[:200]

                    if title and link:
                        items.append({
                            "TITLE": title,
                            "URL": link,
                            "BODY": description,
                            "PUB_DATE": pub_date,
                            "SOURCE": feed_url.split("/")[2],
                        })

                if items:
                    print(f"✅ RSS OK: {feed_url} ({len(items)} newsów)")
                    return items

            except Exception as e:
                print(f"❌ RSS wyjątek {feed_url}: {e}")
                continue

        return []

    # ─── Alternative.me — Fear & Greed Index ──────────────────────────────────

    async def fetch_fear_greed(self) -> dict | None:
        url = "https://api.alternative.me/fng/?limit=1"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                items = data.get("data", [])
                return items[0] if items else None

    # ─── Alpha Vantage — stocks + macro ───────────────────────────────────────

    async def fetch_stock_news(self, topics: str = "financial_markets") -> list:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "topics": topics,
            "apikey": config.ALPHAVANTAGE_API_KEY,
            "limit": 5,
            "sort": "LATEST",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    print(f"❌ Alpha Vantage błąd: {resp.status}")
                    return []
                data = await resp.json()
                return data.get("feed", [])[:5]

    # ─── Embedy ───────────────────────────────────────────────────────────────

    def build_crypto_embed(self, item: dict) -> discord.Embed:
        title = item.get("TITLE", "Brak tytułu")[:256]
        url = item.get("URL", "")
        source = item.get("SOURCE", "Crypto News")
        body = item.get("BODY", "")
        pub_date = item.get("PUB_DATE", "")

        embed = discord.Embed(
            title=f"📰 {title}",
            url=url,
            description=body if body else None,
            color=discord.Color.orange(),
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Źródło", value=source, inline=True)
        if pub_date:
            embed.add_field(name="Data", value=pub_date, inline=True)
        embed.set_footer(text="TrenchBot • Decrypt / CoinDesk RSS")
        return embed

    def build_fear_greed_embed(self, data: dict) -> discord.Embed:
        value = int(data.get("value", 50))
        label = data.get("value_classification", "Neutral")

        if value >= 75:
            color = discord.Color.dark_green()
            emoji = "🤑"
        elif value >= 55:
            color = discord.Color.green()
            emoji = "😊"
        elif value >= 45:
            color = discord.Color.light_grey()
            emoji = "😐"
        elif value >= 25:
            color = discord.Color.orange()
            emoji = "😰"
        else:
            color = discord.Color.red()
            emoji = "😱"

        filled = round(value / 5)
        bar = "█" * filled + "░" * (20 - filled)

        embed = discord.Embed(
            title=f"{emoji} Fear & Greed Index",
            color=color,
            timestamp=datetime.utcnow(),
        )
        embed.add_field(
            name=f"{value}/100 — {label}",
            value=f"`{bar}`",
            inline=False,
        )
        embed.set_footer(text="TrenchBot • Alternative.me")
        return embed

    def build_stock_embed(self, item: dict) -> discord.Embed:
        title = item.get("title", "Brak tytułu")[:256]
        url = item.get("url", "")
        source = item.get("source", "Nieznane")
        sentiment_score = item.get("overall_sentiment_score", 0)
        sentiment_label = item.get("overall_sentiment_label", "Neutral")

        if sentiment_score > 0.15:
            color, emoji = discord.Color.green(), "🟢"
        elif sentiment_score < -0.15:
            color, emoji = discord.Color.red(), "🔴"
        else:
            color, emoji = discord.Color.light_grey(), "⚪"

        embed = discord.Embed(
            title=f"📊 {title}",
            url=url,
            color=color,
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Źródło", value=source, inline=True)
        embed.add_field(
            name="Sentyment",
            value=f"{emoji} {sentiment_label} ({sentiment_score:+.2f})",
            inline=True,
        )
        embed.set_footer(text="TrenchBot • Alpha Vantage")
        return embed

    # ─── Auto-feed co 30 minut ────────────────────────────────────────────────

    @tasks.loop(minutes=30)
    async def auto_feed(self):
        await self.bot.wait_until_ready()
        channel = self.bot.get_channel(config.NEWS_CHANNEL_ID)
        if channel is None:
            print(f"❌ Nie znaleziono kanału ID: {config.NEWS_CHANNEL_ID}")
            return

        print(f"📡 Auto-feed o {datetime.utcnow().strftime('%H:%M UTC')}")

        # Fear & Greed
        try:
            fg = await self.fetch_fear_greed()
            if fg:
                await channel.send(embed=self.build_fear_greed_embed(fg))
        except Exception as e:
            print(f"❌ Fear & Greed błąd: {e}")

        # Crypto newsy (RSS)
        try:
            crypto_news = await self.fetch_crypto_news()
            new_items = [n for n in crypto_news if n.get("URL") not in sent_news_ids]
            if new_items:
                await channel.send("## 🔶 Crypto News")
                for item in new_items[:3]:
                    await channel.send(embed=self.build_crypto_embed(item))
                    sent_news_ids.add(item.get("URL"))
                    await asyncio.sleep(1)
            else:
                print("ℹ️ Brak nowych crypto newsów")
        except Exception as e:
            print(f"❌ Crypto newsy błąd: {e}")

        # Stock / macro newsy
        try:
            stock_news = await self.fetch_stock_news("financial_markets,economy_macro")
            for item in stock_news[:2]:
                url = item.get("url", "")
                if url not in sent_news_ids:
                    await channel.send(embed=self.build_stock_embed(item))
                    sent_news_ids.add(url)
                    await asyncio.sleep(1)
        except Exception as e:
            print(f"❌ Stock newsy błąd: {e}")

    @auto_feed.before_loop
    async def before_auto_feed(self):
        await self.bot.wait_until_ready()

    # ─── Komenda /news ────────────────────────────────────────────────────────

    @app_commands.command(name="news", description="Pobierz najnowsze newsy crypto lub rynkowe")
    @app_commands.describe(typ="Rodzaj newsów do wyświetlenia")
    @app_commands.choices(typ=[
        app_commands.Choice(name="🔶 Crypto (RSS)", value="crypto"),
        app_commands.Choice(name="😱 Fear & Greed Index", value="feargreed"),
        app_commands.Choice(name="📈 Stocks & Macro", value="stocks"),
    ])
    async def news_command(self, interaction: discord.Interaction, typ: str = "crypto"):
        await interaction.response.defer()

        if typ == "crypto":
            items = await self.fetch_crypto_news()
            if not items:
                await interaction.followup.send("❌ Brak newsów — oba RSS feedy niedostępne.")
                return
            await interaction.followup.send("## 📰 Crypto News")
            for item in items[:5]:
                await interaction.followup.send(embed=self.build_crypto_embed(item))

        elif typ == "feargreed":
            fg = await self.fetch_fear_greed()
            if not fg:
                await interaction.followup.send("❌ Błąd pobierania Fear & Greed.")
                return
            await interaction.followup.send(embed=self.build_fear_greed_embed(fg))

        elif typ == "stocks":
            items = await self.fetch_stock_news("financial_markets,economy_macro")
            if not items:
                await interaction.followup.send("❌ Brak newsów z Alpha Vantage.")
                return
            await interaction.followup.send("## 📊 Stock & Macro News")
            for item in items[:5]:
                await interaction.followup.send(embed=self.build_stock_embed(item))


async def setup(bot):
    await bot.add_cog(NewsFeed(bot))