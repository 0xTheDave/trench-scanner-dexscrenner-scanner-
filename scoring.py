# scoring.py
# Token quality scoring system.
# Base score (0-75) from DexScreener market data,
# safety score (-45 to +30) from RugCheck on-chain data.

ALPHA_THRESHOLD = 65    # score >= this -> #alpha-picks
GEM_THRESHOLD = 45      # score >= this -> #early-gems, below -> rejected
RESCORE_TRIGGER = 50    # watchlist tokens above this get full re-score with RugCheck


def compute_base_score(pair: dict, age_hours: float) -> tuple[int, list[str]]:
    """
    Score 0-75 from market data already fetched from DexScreener.
    Returns (score, breakdown_lines).
    """
    score = 0
    breakdown = []

    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol = (pair.get("volume") or {}).get("h24") or 0
    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    total_txns = buys + sells
    info = pair.get("info") or {}
    boosts = (pair.get("boosts") or {}).get("active", 0)

    # Liquidity depth (max 15)
    if liq >= 30_000:
        score += 15
        breakdown.append("Liquidity $30k+ (+15)")
    elif liq >= 20_000:
        score += 10
        breakdown.append("Liquidity $20k+ (+10)")
    elif liq >= 10_000:
        score += 5
        breakdown.append("Liquidity $10k+ (+5)")

    # Organic volume: vol/liq sweet spot (max 15)
    vol_liq = vol / liq if liq > 0 else 0
    if 2.0 <= vol_liq <= 10.0:
        score += 15
        breakdown.append(f"Organic vol/liq {vol_liq:.1f}x (+15)")
    elif 1.0 <= vol_liq < 2.0 or 10.0 < vol_liq <= 20.0:
        score += 8
        breakdown.append(f"Acceptable vol/liq {vol_liq:.1f}x (+8)")

    # Healthy buy pressure (max 10)
    if total_txns > 0:
        buy_ratio = buys / total_txns
        if 0.55 <= buy_ratio <= 0.75:
            score += 10
            breakdown.append(f"Healthy buys {buy_ratio:.0%} (+10)")
        elif 0.50 <= buy_ratio < 0.55 or 0.75 < buy_ratio <= 0.85:
            score += 5
            breakdown.append(f"OK buys {buy_ratio:.0%} (+5)")

    # Socials presence (max 15)
    has_twitter = any(
        (s.get("platform") or "").lower() == "twitter"
        for s in (info.get("socials") or [])
    )
    has_website = bool(info.get("websites"))
    if has_twitter and has_website:
        score += 15
        breakdown.append("Twitter + website (+15)")
    elif has_twitter or has_website:
        score += 8
        breakdown.append("Partial socials (+8)")

    # Paid boosts = team spending money (max 10)
    if boosts and boosts > 0:
        score += 10
        breakdown.append(f"{boosts} active boosts (+10)")

    # Age sweet spot: survived first hour, still early (max 10)
    if 2.0 <= age_hours <= 12.0:
        score += 10
        breakdown.append(f"Age {age_hours:.1f}h sweet spot (+10)")
    elif 1.0 <= age_hours < 2.0 or 12.0 < age_hours <= 24.0:
        score += 5
        breakdown.append(f"Age {age_hours:.1f}h OK (+5)")

    return score, breakdown


def compute_safety_score(rugcheck: dict | None) -> tuple[int, list[str], bool]:
    """
    Score adjustment from RugCheck data.
    Returns (score_delta, breakdown_lines, hard_fail).
    hard_fail=True means the token should be rejected entirely.
    """
    if rugcheck is None:
        return 0, ["RugCheck data unavailable (0)"], False

    # Rugged flag — RugCheck already confirmed this token rugged
    if rugcheck.get("rugged") is True:
        return 0, ["Marked as RUGGED by RugCheck ⛔"], True

    delta = 0
    breakdown = []
    hard_fail = False

    # Mint authority — active means team can print new supply
    if rugcheck.get("mint_authority_active") is False:
        delta += 10
        breakdown.append("Mint revoked ✅ (+10)")
    elif rugcheck.get("mint_authority_active") is True:
        delta -= 20
        breakdown.append("Mint ACTIVE ⚠️ (-20)")
        hard_fail = True

    # Freeze authority — active means team can freeze your tokens
    if rugcheck.get("freeze_authority_active") is False:
        delta += 5
        breakdown.append("Not freezable ✅ (+5)")
    elif rugcheck.get("freeze_authority_active") is True:
        delta -= 15
        breakdown.append("Freezable ⚠️ (-15)")

    # LP locked/burned (now measured on the MOST LIQUID market)
    lp_locked_pct = rugcheck.get("lp_locked_pct")
    if lp_locked_pct is not None:
        if lp_locked_pct >= 90:
            delta += 10
            breakdown.append(f"LP locked {lp_locked_pct:.0f}% ✅ (+10)")
        elif lp_locked_pct >= 50:
            delta += 5
            breakdown.append(f"LP locked {lp_locked_pct:.0f}% (+5)")
        else:
            delta -= 10
            breakdown.append(f"LP locked only {lp_locked_pct:.0f}% ⚠️ (-10)")

    # Top holders concentration (AMM pool accounts excluded)
    top10_pct = rugcheck.get("top10_holders_pct")
    if top10_pct is not None:
        if top10_pct <= 30:
            delta += 5
            breakdown.append(f"Top10 hold {top10_pct:.0f}% ✅ (+5)")
        elif top10_pct >= 60:
            delta -= 15
            breakdown.append(f"Top10 hold {top10_pct:.0f}% ⚠️ (-15)")

    # RugCheck's own normalised risk score (0-100, higher = riskier).
    # Only the normalised scale is used — raw score is unbounded.
    risk_norm = rugcheck.get("risk_score_normalised")
    if isinstance(risk_norm, (int, float)):
        if risk_norm >= 60:
            delta -= 10
            breakdown.append(f"RugCheck risk {risk_norm:.0f}/100 ⚠️ (-10)")
        elif risk_norm <= 10:
            delta += 5
            breakdown.append(f"RugCheck risk {risk_norm:.0f}/100 ✅ (+5)")

    return delta, breakdown, hard_fail