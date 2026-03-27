"""
gemscore.py — Scuby's gem scoring engine. UPGRADED v3.

Key changes vs v2:
  - Pattern bonus now has a smart HEURISTIC fallback when historical data is
    thin (< 10 resolved tokens). The heuristic is based on real Solana edge:
    vol/mcap > 1x + age 30min-3h + liq/mcap > 0.15 is a genuine early signal.
  - Age scoring sharpened: 30-90 minute window gets the highest score,
    reflecting the pump.fun graduation window and post-initial-dump sweet spot.
  - Graduation bonus: tokens with mcap $60K-$90K and age 1-3h get a bonus
    (post-pump.fun graduation, survived the most dangerous phase).
  - Parabolic price penalty: +200%+ 1h is scored slightly lower than +50-200%
    because parabolics have a high dump-incoming rate on Solana.

Scores a token 0–100 using 8 weighted signals:
  1. Liquidity depth          (20pts)
  2. Pair age                 (15pts) — SHARPENED: peaks at 30-90m
  3. Vol/MCap ratio           (20pts)
  4. MCap range               (15pts)
  5. Price momentum           (10pts) — parabolic penalty added
  6. Pattern/heuristic bonus  (15pts) — HYBRID: real data + smart fallback
  7. Rug risk penalty        (-15pts)
  8. Liq/MCap ratio            (5pts)
  9. Vol consistency           (5pts)

Grades:
  💎 90-100  Diamond
  🔥 75-89   Hot
  ✅ 55-74   Solid
  ⚠️ 35-54   Weak
  💀 0-34    Danger
"""

import logging
import time
from datetime import datetime, timezone

from utils import safe_float, escape_md

logger = logging.getLogger(__name__)


# ─── Weights ──────────────────────────────────────────────────────────────────

WEIGHTS = {
    "liq_score":       20,
    "age_score":       15,
    "vol_mcap_score":  20,
    "mcap_score":      15,
    "price_change":    10,
    "pattern_bonus":   15,
    "liq_mcap_score":   5,
    "vol_consistency":  5,
    "risk_penalty":   -15,
}

_MAX_POS = sum(v for v in WEIGHTS.values() if v > 0)

# How many resolved tokens we need before trusting the pattern engine
_PATTERN_MIN_RESOLVED = 10


# ─── Individual scorers ───────────────────────────────────────────────────────

def _score_liq(liq: float) -> tuple[float, str]:
    """Liquidity depth. Sweet spot: $10K–$100K for early Solana gems."""
    if liq >= 200_000: return 1.0, f"${liq/1000:.0f}K ultra-deep liq 💧💧"
    if liq >= 100_000: return 1.0, f"${liq/1000:.0f}K deep liq 💧"
    if liq >=  50_000: return 0.9, f"${liq/1000:.0f}K solid liq 💧"
    if liq >=  20_000: return 0.8, f"${liq/1000:.0f}K good liq 💧"
    if liq >=  10_000: return 0.7, f"${liq/1000:.0f}K ok liq 💧"
    if liq >=   5_000: return 0.5, f"${liq/1000:.1f}K low liq ⚠️"
    if liq >=   2_000: return 0.25, f"${liq:.0f} very low liq 🚨"
    return 0.0, f"${liq:.0f} dangerously low liq 💀"


def _score_age(age_h: float) -> tuple[float, str]:
    """
    Pair age scoring — SHARPENED for Solana reality.

    The real gem window on Solana is 30-90 minutes post-launch:
    - < 10min: bonding curve still filling, no signal yet
    - 10-30min: very early, high dump risk still active
    - 30-90min: SWEET SPOT — initial dump risk passed, crowd hasn't found it
    - 90min-3h: still excellent, slightly more competition
    - 3-6h: early but not bleeding-edge
    - > 24h at micro mcap: usually a dead cat, not a hidden gem
    """
    age_m = age_h * 60
    if age_h < 0.1:       return 0.3, f"{age_m:.0f}m old — too new, no signal yet ⚡"
    if age_h < 0.17:      return 0.5, f"{age_m:.0f}m old — very early, dump risk high 🆕"
    if age_h < 0.5:       return 0.8, f"{age_m:.0f}m old — early entry zone 🆕"
    if age_h < 1.0:       return 1.0, f"{age_m:.0f}m old — SWEET SPOT 🎯🎯"   # 30-60 min peak
    if age_h < 1.5:       return 1.0, f"{age_m:.0f}m old — prime window 🎯"    # 60-90 min peak
    if age_h < 3.0:       return 0.9, f"{age_h:.1f}h old — still early ✅"
    if age_h < 6.0:       return 0.8, f"{age_h:.1f}h old — early 📅"
    if age_h < 12.0:      return 0.6, f"{age_h:.1f}h old — moderate age"
    if age_h < 24.0:      return 0.4, f"{age_h:.1f}h old — getting older"
    if age_h < 72.0:      return 0.2, f"{age_h/24:.1f}d old — ask why it hasn't moved"
    if age_h < 168.0:     return 0.1, f"{age_h/24:.0f}d old — aged"
    return 0.05, f"{age_h/24:.0f}d old — very old 📅"


def _score_vol_mcap(vol1h: float, mcap: float) -> tuple[float, str]:
    """Vol/MCap ratio — the single strongest early momentum signal."""
    if mcap <= 0:
        return 0.3, "no mcap data"
    ratio = vol1h / mcap
    if ratio >= 5.0:   return 1.0, f"vol/mcap {ratio:.1f}x 🌙 EXTREME momentum"
    if ratio >= 2.0:   return 1.0, f"vol/mcap {ratio:.1f}x 🚀 extreme momentum"
    if ratio >= 1.0:   return 0.9, f"vol/mcap {ratio:.1f}x 🔥 strong momentum"
    if ratio >= 0.5:   return 0.8, f"vol/mcap {ratio:.2f} 📈 good momentum"
    if ratio >= 0.2:   return 0.6, f"vol/mcap {ratio:.2f} 📊 moderate momentum"
    if ratio >= 0.05:  return 0.4, f"vol/mcap {ratio:.3f} low activity"
    return 0.15, f"vol/mcap {ratio:.4f} very low activity 📉"


def _score_mcap(mcap: float) -> tuple[float, str]:
    """MCap range. Sweet spot for early Solana gems: $10K–$100K."""
    if mcap <= 0:           return 0.3, "no mcap data"
    if mcap < 3_000:        return 0.4, f"${mcap:,.0f} ultra-micro ⚡ (very high risk)"
    if mcap < 10_000:       return 0.7, f"${mcap:,.0f} micro mcap ⚡"
    if mcap < 25_000:       return 1.0, f"${mcap/1000:.0f}K mcap 💎 prime early zone"
    if mcap < 50_000:       return 0.95, f"${mcap/1000:.0f}K mcap 🔥 early gem range"
    if mcap < 100_000:      return 0.85, f"${mcap/1000:.0f}K mcap 🔥"
    if mcap < 250_000:      return 0.75, f"${mcap/1000:.0f}K mcap ✅"
    if mcap < 500_000:      return 0.6,  f"${mcap/1000:.0f}K mcap — getting pricier"
    if mcap < 2_000_000:    return 0.45, f"${mcap/1_000_000:.1f}M mcap — mid"
    if mcap < 10_000_000:   return 0.3,  f"${mcap/1_000_000:.1f}M mcap — large"
    return 0.15, f"${mcap/1_000_000:.0f}M mcap — very large"


def _score_price_change(h1: float, h6: float) -> tuple[float, str]:
    """
    1h price momentum with parabolic penalty.

    > +200% in 1h is a yellow flag on Solana — the crowd has already found it
    and a dump is likely incoming. Score it slightly below the 50-200% range.
    """
    if h1 >= 200:    return 0.80, f"+{h1:.0f}% 1h 🌙 parabolic — watch for dump ⚠️"
    if h1 >= 100:    return 1.0,  f"+{h1:.0f}% 1h 🌙 parabolic"
    if h1 >= 50:     return 0.95, f"+{h1:.0f}% 1h 🚀"
    if h1 >= 20:     return 0.85, f"+{h1:.0f}% 1h 📈"
    if h1 >= 5:      return 0.7,  f"+{h1:.0f}% 1h ✅"
    if h1 >= 0:      return 0.5,  f"+{h1:.1f}% 1h flat"
    if h1 >= -10:    return 0.4,  f"{h1:.0f}% 1h 📉 slight dip"
    if h1 >= -30:    return 0.25, f"{h1:.0f}% 1h 📉"
    if h1 >= -60:    return 0.1,  f"{h1:.0f}% 1h 🩸"
    return 0.05, f"{h1:.0f}% 1h 💀"


def _score_liq_mcap(liq: float, mcap: float) -> tuple[float, str]:
    """Liquidity/MCap ratio — backing quality."""
    if mcap <= 0:
        return 0.3, "no mcap"
    ratio = liq / mcap
    if ratio >= 0.5:   return 1.0, f"liq/mcap {ratio:.2f} ✅ well-backed"
    if ratio >= 0.2:   return 0.8, f"liq/mcap {ratio:.2f} decent backing"
    if ratio >= 0.1:   return 0.6, f"liq/mcap {ratio:.2f}"
    if ratio >= 0.05:  return 0.4, f"liq/mcap {ratio:.3f} thin backing ⚠️"
    return 0.1, f"liq/mcap {ratio:.4f} very thin backing 🚨"


def _score_vol_consistency(vol1h: float, vol24h: float) -> tuple[float, str]:
    """
    Volume consistency — is 1h vol sustainable vs 24h baseline?
    Single-candle spikes are suspicious; sustained vol is real.
    """
    if vol24h <= 0 or vol1h <= 0:
        return 0.5, "no 24h vol data"
    expected_1h = vol24h / 24
    if expected_1h <= 0:
        return 0.5, "no 24h vol data"
    ratio = vol1h / expected_1h
    if 0.5 <= ratio <= 3.0:  return 1.0, f"vol pace {ratio:.1f}x 24h avg ✅ sustained"
    if 0.2 <= ratio <= 6.0:  return 0.7, f"vol pace {ratio:.1f}x 24h avg"
    if ratio > 10:            return 0.4, f"vol {ratio:.0f}x normal ⚠️ spike — one-off?"
    return 0.5, f"vol pace {ratio:.1f}x 24h avg"


def _score_pattern_heuristic(
    vol_mcap: float,
    age_h: float,
    liq: float,
    liq_mcap: float,
    mcap: float,
) -> tuple[float, str]:
    """
    HEURISTIC pattern score — used when historical data is thin.

    Based on real Solana edge that doesn't require training data:
    The strongest early signal is: vol/mcap > 1x AND age 30-90min AND
    liq > $5K AND liq/mcap > 0.10. This combination has historically
    produced 2-5x moves within 1-3 hours on Solana at high rates.

    Secondary signals that add to the score:
    - Pump.fun graduation zone: mcap $60K-$90K with age 1-2h
      (survived the graduation, now in free market price discovery)
    - Very tight liq/mcap > 0.3 (deep pool relative to mcap)
    - Vol/mcap > 3x (extreme momentum signal)
    """
    age_m = age_h * 60
    score = 0.5  # neutral baseline
    notes = []

    # Core signal: vol/mcap momentum + sweet spot age + real liquidity
    if vol_mcap >= 1.0 and 30 <= age_m <= 150 and liq >= 5_000 and liq_mcap >= 0.10:
        score = 0.90
        if vol_mcap >= 3.0:
            score = 1.0
            notes.append(f"🎯 PRIME SETUP: vol/mcap {vol_mcap:.1f}x, {age_m:.0f}m old, well-backed")
        else:
            notes.append(f"🎯 Strong early setup: vol/mcap {vol_mcap:.1f}x, {age_m:.0f}m old")

    # Pump.fun graduation zone bonus
    elif 60_000 <= mcap <= 90_000 and 1.0 <= age_h <= 2.5:
        score = max(score, 0.80)
        notes.append(f"🎓 Graduation zone: ${mcap/1000:.0f}K mcap, {age_h:.1f}h — survived bonding curve")

    # Moderate setup: good vol/mcap but slightly outside sweet spot
    elif vol_mcap >= 0.5 and age_m >= 15 and liq >= 3_000:
        score = max(score, 0.65)
        notes.append(f"📈 Decent setup: vol/mcap {vol_mcap:.2f}, {age_m:.0f}m old")

    # Deep pool relative to mcap is a standalone positive signal
    if liq_mcap >= 0.3 and score < 0.85:
        score = min(score + 0.1, 0.85)
        notes.append("💧 Deep pool")

    # Extreme volume is always notable
    if vol_mcap >= 5.0 and score < 1.0:
        score = min(score + 0.15, 1.0)
        notes.append(f"🚀 Extreme momentum: {vol_mcap:.1f}x")

    if not notes:
        notes.append("building signal data...")

    return score, " · ".join(notes)


def _score_pattern_match(attrs: dict, token_perf: dict, chat_id: str) -> tuple[float, str]:
    """
    HYBRID pattern scorer:
    1. If enough resolved tokens exist, use the real pattern engine
    2. Otherwise, use the heuristic (always provides signal)
    """
    vol_mcap  = attrs.get("vol_mcap", 0)
    age_h     = attrs.get("age_h",    0)
    liq       = attrs.get("liq",      0)
    mcap      = attrs.get("mcap",     0)
    liq_mcap  = attrs.get("liq", 0) / attrs.get("mcap", 1) if attrs.get("mcap", 0) > 0 else 0

    # Count resolved tokens in this chat
    resolved_count = sum(
        1 for rec in token_perf.values()
        if rec.get("perf", {}).get("1h")
        and (not chat_id or rec.get("chat_id") == chat_id)
    )

    # If we have enough data, use the real pattern engine
    if resolved_count >= _PATTERN_MIN_RESOLVED and token_perf:
        try:
            from memory import analyse_patterns, _bucket, MCAP_BUCKETS, LIQ_BUCKETS, AGE_BUCKETS, VOLMCAP_BUCKETS
            patterns = analyse_patterns(token_perf, chat_id)
            if patterns:
                mcap_b    = _bucket(attrs.get("mcap",     0), MCAP_BUCKETS)
                liq_b     = _bucket(attrs.get("liq",      0), LIQ_BUCKETS)
                age_b     = _bucket(attrs.get("age_h",    0), AGE_BUCKETS)
                volmcap_b = _bucket(attrs.get("vol_mcap", 0), VOLMCAP_BUCKETS)

                my_keys = {
                    f"mcap:{mcap_b}", f"liq:{liq_b}",
                    f"age:{age_b}",   f"volmcap:{volmcap_b}",
                    f"mcap:{mcap_b}+liq:{liq_b}",
                    f"mcap:{mcap_b}+age:{age_b}",
                    f"liq:{liq_b}+volmcap:{volmcap_b}",
                    f"age:{age_b}+liq:{liq_b}",
                }

                best = None
                for pattern in patterns[:15]:
                    if pattern["key"] in my_keys and pattern["pump_rate"] >= 0.35:
                        if best is None or pattern["pump_rate"] > best["pump_rate"]:
                            best = pattern

                if best:
                    pr = best["pump_rate"]
                    ct = best["count"]
                    if pr >= 0.65: return 1.0, f"💎 strong pattern ({pr:.0%} pump rate, {ct} tokens)"
                    if pr >= 0.5:  return 0.85, f"✅ good pattern ({pr:.0%} pump rate, {ct} tokens)"
                    if pr >= 0.35: return 0.65, f"matches pattern ({pr:.0%} pump rate)"
        except Exception as ex:
            logger.debug(f"_score_pattern_match real engine: {ex}")

    # Fallback: heuristic (always gives meaningful signal)
    raw, note = _score_pattern_heuristic(vol_mcap, age_h, liq, liq_mcap, mcap)
    suffix = f" _(heuristic, {resolved_count} tokens tracked)_" if resolved_count < _PATTERN_MIN_RESOLVED else ""
    return raw, note + suffix


def _score_risk(risk_report: dict) -> tuple[float, str]:
    """Rugcheck risk penalty."""
    if not risk_report:
        return 0.0, "rugcheck unavailable"
    score  = safe_float(risk_report.get("score", 0))
    risks  = risk_report.get("risks", []) or []
    levels = {r.get("level", "").lower() for r in risks}
    names  = [r.get("name", "").lower() for r in risks]

    critical = any(
        kw in n for n in names
        for kw in ("mint authority", "freeze authority", "unlocked lp", "rugged")
    )

    if "danger" in levels or score >= 700:
        return -1.0, f"🚨 HIGH rug risk (score {score:.0f})" + (" — mint/freeze risk!" if critical else "")
    if "warn" in levels or score >= 300:
        return -0.5, f"⚠️ MEDIUM rug risk (score {score:.0f})"
    if score >= 100:
        return -0.1, f"🟡 Minor flags (score {score:.0f})"
    return 0.0, f"✅ LOW rug risk (score {score:.0f})"


# ─── Main scorer ──────────────────────────────────────────────────────────────

def calculate_gem_score(
    pair:        dict,
    risk_report: dict,
    token_perf:  dict,
    chat_id:     str = "",
) -> dict:
    """Calculate a GemScore for a token pair. Returns full scoring breakdown."""
    mcap   = safe_float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq    = safe_float((pair.get("liquidity")   or {}).get("usd",  0))
    vol1h  = safe_float((pair.get("volume")      or {}).get("h1",   0))
    vol24h = safe_float((pair.get("volume")      or {}).get("h24",  0))
    h1     = safe_float((pair.get("priceChange") or {}).get("h1",   0))
    h6     = safe_float((pair.get("priceChange") or {}).get("h6",   0))

    age_h   = 0.0
    created = pair.get("pairCreatedAt")
    if created:
        age_h = (time.time() * 1000 - created) / 3_600_000

    vol_mcap = vol1h / mcap if mcap > 0 else 0
    attrs    = {"mcap": mcap, "liq": liq, "age_h": age_h, "vol_mcap": vol_mcap}

    liq_raw,   liq_note   = _score_liq(liq)
    age_raw,   age_note   = _score_age(age_h)
    vm_raw,    vm_note    = _score_vol_mcap(vol1h, mcap)
    mc_raw,    mc_note    = _score_mcap(mcap)
    pc_raw,    pc_note    = _score_price_change(h1, h6)
    pat_raw,   pat_note   = _score_pattern_match(attrs, token_perf, chat_id)
    lm_raw,    lm_note    = _score_liq_mcap(liq, mcap)
    vc_raw,    vc_note    = _score_vol_consistency(vol1h, vol24h)
    risk_raw,  risk_note  = _score_risk(risk_report)

    raw_score = (
        liq_raw  * WEIGHTS["liq_score"]
        + age_raw  * WEIGHTS["age_score"]
        + vm_raw   * WEIGHTS["vol_mcap_score"]
        + mc_raw   * WEIGHTS["mcap_score"]
        + pc_raw   * WEIGHTS["price_change"]
        + pat_raw  * WEIGHTS["pattern_bonus"]
        + lm_raw   * WEIGHTS["liq_mcap_score"]
        + vc_raw   * WEIGHTS["vol_consistency"]
        + risk_raw * abs(WEIGHTS["risk_penalty"])
    )

    score = max(0.0, min(100.0, (raw_score / _MAX_POS) * 100))

    if score >= 90:   grade, gem_emoji = "💎 Diamond",  "💎"
    elif score >= 75: grade, gem_emoji = "🔥 Hot",      "🔥"
    elif score >= 55: grade, gem_emoji = "✅ Solid",     "✅"
    elif score >= 35: grade, gem_emoji = "⚠️ Weak",      "⚠️"
    else:             grade, gem_emoji = "💀 Danger",    "💀"

    return {
        "score":     round(score, 1),
        "grade":     grade,
        "gem_emoji": gem_emoji,
        "breakdown": {
            "Liquidity":   (liq_raw,  liq_note,  WEIGHTS["liq_score"]),
            "Age":         (age_raw,  age_note,  WEIGHTS["age_score"]),
            "Momentum":    (vm_raw,   vm_note,   WEIGHTS["vol_mcap_score"]),
            "MCap range":  (mc_raw,   mc_note,   WEIGHTS["mcap_score"]),
            "Price action":(pc_raw,   pc_note,   WEIGHTS["price_change"]),
            "Pattern":     (pat_raw,  pat_note,  WEIGHTS["pattern_bonus"]),
            "Liq quality": (lm_raw,   lm_note,   WEIGHTS["liq_mcap_score"]),
            "Vol health":  (vc_raw,   vc_note,   WEIGHTS["vol_consistency"]),
            "Rug risk":    (risk_raw, risk_note, WEIGHTS["risk_penalty"]),
        },
        "meta": {
            "mcap":    mcap,
            "liq":     liq,
            "vol1h":   vol1h,
            "age_h":   age_h,
            "h1":      h1,
            "vol_mcap": vol_mcap,
        }
    }


def format_gem_score(result: dict, symbol: str, ca: str) -> str:
    """Format a GemScore result into a Telegram MarkdownV2 block."""
    e        = escape_md
    score    = result["score"]
    grade    = result["grade"]
    emoji    = result["gem_emoji"]
    bd       = result["breakdown"]
    meta     = result.get("meta", {})

    filled = int(score / 10)
    bar    = "█" * filled + "░" * (10 - filled)

    lines = [
        f"\n━━━━━━━━━━━━━━━━━━━",
        f"{emoji} *GemScore: {e(str(score))}/100 — {e(grade)}*",
        f"`{e(bar)}` {e(str(score))}",
        "",
    ]

    for category, (raw, note, weight) in bd.items():
        if category == "Rug risk":
            if raw < 0:
                lines.append(f"🚨 *{e(category)}:* {e(note)}")
            else:
                lines.append(f"✅ *{e(category)}:* {e(note)}")
        else:
            pct      = max(0, min(100, int(raw * 100)))
            bar_mini = "▓" * (pct // 20) + "░" * (5 - pct // 20)
            lines.append(f"`{bar_mini}` {e(category)}: {e(note)}")

    age_h  = meta.get("age_h", 0)
    vol_mc = meta.get("vol_mcap", 0)
    if score >= 75:
        tip = "🐾 Strong early signal — watch closely, Raggy\\!"
    elif score >= 55:
        tip = "🐾 Decent setup — standard risk, keep an eye on it\\."
    elif score >= 35:
        tip = "🐾 Notable red flags — proceed with extra caution\\."
    else:
        tip = "🐾 Multiple danger signs — Scuby says be very careful\\!"

    lines.append(f"\n_{e(tip)}_")
    lines.append(f"_⚠️ GemScore is informational only\\. DYOR, Raggy\\!_")
    return "\n".join(lines)
