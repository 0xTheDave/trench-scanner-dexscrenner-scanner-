# haiku_client.py

import aiohttp
import os

import db

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAIKU_MODEL = "claude-haiku-4-5"

VALID_TOKEN_TYPES = ("meme", "utility", "unknown")


async def generate_tldr(
    session: aiohttp.ClientSession,
    token_context: dict,
) -> str | None:
    """
    Generate a 2-3 sentence TL;DR for a token using Claude Haiku.
    token_context: {symbol, name, description, socials, score,
                    rugcheck_summary, market_summary}
    Returns TL;DR string or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        return None

    prompt = (
        "You are a crypto analyst writing a brief TL;DR for a trading Discord.\n"
        "Based on the data below, write 2-3 short sentences: what the project is, "
        "one notable strength, one notable risk. Be direct and factual. "
        "No hype, no financial advice, no emoji.\n\n"
        f"Token: ${token_context.get('symbol', '?')} ({token_context.get('name', '?')})\n"
        f"Description: {token_context.get('description') or 'No description provided'}\n"
        f"Socials: {token_context.get('socials') or 'None'}\n"
        f"Market: {token_context.get('market_summary', '')}\n"
        f"On-chain safety: {token_context.get('rugcheck_summary', 'unknown')}\n"
        f"Quality score: {token_context.get('score', '?')}/100"
    )

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 200,
        "messages": [{"role": "user", "content": prompt}],
    }

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        async with session.post(
            ANTHROPIC_API,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"[haiku] HTTP {resp.status}: {text[:200]}")
                return None
            data = await resp.json()
            content = data.get("content", [])
            if content and content[0].get("type") == "text":
                return content[0]["text"].strip()
            return None
    except Exception as e:
        print(f"[haiku] Error: {e}")
        return None


def _parse_classification(text: str) -> dict | None:
    """
    Parse the strict "TYPE: ...\\nREASON: ..." format we ask Haiku for.
    Returns None if the response doesn't contain a recognizable TYPE line.
    """
    token_type = None
    reason = ""

    for line in text.splitlines():
        line = line.strip()
        if line.upper().startswith("TYPE:"):
            candidate = line.split(":", 1)[1].strip().lower()
            if candidate in VALID_TOKEN_TYPES:
                token_type = candidate
        elif line.upper().startswith("REASON:"):
            reason = line.split(":", 1)[1].strip()

    if token_type is None:
        return None

    return {"type": token_type, "reason": reason}


async def classify_token_type(
    session: aiohttp.ClientSession,
    token_context: dict,
) -> dict | None:
    """
    Classify a token as meme / utility / unknown using Claude Haiku.
    token_context: {symbol, name, description, socials}
    Returns {"type": "meme"|"utility"|"unknown", "reason": str} or None on
    failure (no API key, timeout, HTTP error, or unparseable response).
    """
    if not ANTHROPIC_API_KEY:
        return None

    prompt = (
        "You are a crypto analyst classifying a Solana token for a trading Discord.\n"
        "Classify it as exactly one of: meme, utility, unknown.\n"
        "- meme: no functional product, value driven by community/narrative/humor.\n"
        "- utility: has (or clearly aims to build) a working product, protocol, or service.\n"
        "- unknown: not enough information to tell.\n"
        "Respond in EXACTLY this format, nothing else:\n"
        "TYPE: <meme|utility|unknown>\n"
        "REASON: <one short factual sentence>\n\n"
        f"Token: ${token_context.get('symbol', '?')} ({token_context.get('name', '?')})\n"
        f"Description: {token_context.get('description') or 'No description provided'}\n"
        f"Socials: {token_context.get('socials') or 'None'}"
    )

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 100,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
    }

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        async with session.post(
            ANTHROPIC_API,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"[haiku] Classify HTTP {resp.status}: {text[:200]}")
                return None
            data = await resp.json()
            content = data.get("content", [])
            if not content or content[0].get("type") != "text":
                return None
            return _parse_classification(content[0]["text"])
    except Exception as e:
        print(f"[haiku] Classify error: {e}")
        return None


async def get_or_classify_token_type(
    session: aiohttp.ClientSession,
    mint: str,
    token_context: dict,
) -> dict:
    """
    Cache-aware meme/utility classification. Checks the DB cache first
    (keyed by mint, no TTL — a token's category doesn't change), otherwise
    calls Haiku and caches the result.
    Always returns a dict with "type" and "reason" — never raises, never
    blocks the alert pipeline. Falls back to {"type": "unknown", "reason": ""}
    when Haiku is unavailable.
    """
    cached = db.get_token_classification(mint)
    if cached is not None:
        return cached

    result = await classify_token_type(session, token_context)
    if result is None:
        result = {"type": "unknown", "reason": ""}

    db.set_token_classification(mint, result)
    return result
