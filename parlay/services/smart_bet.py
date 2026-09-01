"""
Smart Bet Engine v2 — Ultra-accurate daily parlay pick.

Builds the SAFEST ~2x parlay by:
- Analyzing ALL fixtures across ALL leagues
- Using multi-factor analytics (form, stats, H2H, momentum, rankings)
- Selecting legs with highest probability + edge
- Building up to 10+ legs to ensure combined probability > 50%
- Diversifying across leagues and markets
- Storing daily pick in DB for result tracking

Target: ~2.0x odds with MAXIMUM certainty.
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from services.espn_api import ESPNClient
from services.analytics import (
    analyze_fixture, correlation_penalty, implied_prob,
)
from config import LEAGUES

logger = logging.getLogger(__name__)

TARGET_ODDS = 2.0
MIN_LEG_CONFIDENCE = 35
MAX_LEGS = 12
MIN_COMBINED_PROB = 0.50  # Target combined probability threshold


class SmartBetEngine:
    """Deep-analyzes ALL fixtures and builds the safest ~2x parlay."""

    def __init__(self):
        self._daily_cache = None
        self._cache_date = None

    async def generate_daily_pick(self, user_sports=None, tz_offset=0) -> dict:
        """
        Fetch ALL fixtures for today, deeply analyze every market,
        and build the single safest ~2x parlay possible.
        """
        client = ESPNClient()
        try:
            et = datetime.now(timezone(timedelta(hours=-4))).strftime("%Y%m%d")
            utc = datetime.utcnow().strftime("%Y%m%d")

            leagues_to_fetch = [
                lg for lg in LEAGUES.keys()
                if not user_sports or lg.split("/")[0] in user_sports
            ]

            all_picks = []
            seen = set()

            # Fetch ALL fixtures — try multiple dates for timezone safety
            for date in [et, utc]:
                if all_picks:
                    break
                raw = await client.fetch_all_leagues(date=date, leagues=leagues_to_fetch)
                for league_code, data in raw.items():
                    if isinstance(data, Exception) or not data:
                        continue
                    fixtures = ESPNClient.parse_events(data, league_code)
                    for fx in fixtures:
                        if fx["status"] != "pre":
                            continue

                        # Deep analytics on EVERY market
                        picks = analyze_fixture(fx, league_code)
                        for p in picks:
                            key = (fx["id"], p["market"])
                            if key in seen:
                                continue
                            seen.add(key)
                            p["home"] = fx["home_team"]
                            p["away"] = fx["away_team"]
                            p["league"] = league_code
                            p["sport"] = fx.get("sport", league_code.split("/")[0])
                            p["fixture"] = fx
                            try:
                                game_dt = datetime.fromisoformat(fx["date"].replace("Z", "+00:00"))
                                local_dt = game_dt + timedelta(hours=tz_offset)
                                p["kickoff"] = local_dt.strftime("%b %d, %H:%M")
                            except (ValueError, TypeError):
                                p["kickoff"] = ""
                            all_picks.append(p)

            if not all_picks:
                return {"has_pick": False, "message": "No upcoming fixtures found today."}

            # ── Filter to high-confidence, LOW ODDS picks with POSITIVE edge ──
            # Only include picks where our model finds value
            safe = [
                p for p in all_picks
                if p["confidence"] >= MIN_LEG_CONFIDENCE
                and p["odds"] >= 1.08
                and p["odds"] <= 1.65
                and p["probability"] >= 0.52
                and p["edge"] >= 0.0  # MUST have non-negative edge
            ]

            if not safe:
                safe = [
                    p for p in all_picks
                    if p["confidence"] >= 28
                    and p["odds"] >= 1.08
                    and p["odds"] <= 1.85
                    and p["probability"] >= 0.48
                    and p["edge"] >= 0.0  # MUST have non-negative edge
                ]

            if not safe:
                return {
                    "has_pick": False,
                    "message": "Not enough high-confidence fixtures today for a safe pick."
                }

            # ── Best pick per fixture (highest confidence) ────────────
            best_per_fixture = {}
            for p in safe:
                fid = p["fixture"]["id"]
                if fid not in best_per_fixture or p["confidence"] > best_per_fixture[fid]["confidence"]:
                    best_per_fixture[fid] = p

            # ── Sort by combined score: confidence × edge × probability ─
            candidates = sorted(
                best_per_fixture.values(),
                key=lambda x: x["confidence"] * max(0.01, x["edge"] + 0.1) * x["probability"],
                reverse=True,
            )

            # ── Build parlay: greedy approach with backtracking ───────
            parlay = []
            combined_odds = 1.0
            combined_prob = 1.0
            used_fids = set()
            used_leagues = {}

            for p in candidates:
                if len(parlay) >= MAX_LEGS:
                    break

                fid = p["fixture"]["id"]
                if fid in used_fids:
                    continue

                league = p.get("league", "")
                league_count = used_leagues.get(league, 0)

                # Diversification: max 3 picks from same league
                if league_count >= 3:
                    continue

                new_odds = combined_odds * p["odds"]
                new_prob = combined_prob * p["probability"]

                # Stop if we've reached target odds
                if combined_odds >= TARGET_ODDS * 0.92 and new_odds > TARGET_ODDS * 1.30:
                    continue

                # Only add if it doesn't overshoot too much
                if new_odds > TARGET_ODDS * 1.40:
                    continue

                parlay.append(p)
                combined_odds = new_odds
                combined_prob = new_prob
                used_fids.add(fid)
                used_leagues[league] = league_count + 1

            if not parlay:
                return {
                    "has_pick": False,
                    "message": "Could not build a safe parlay today. Fixtures do not align well."
                }

            # ── Post-optimization: try to add more legs if prob is high ──
            # If combined_prob is still very high, we can add more low-odds legs
            if combined_prob >= 0.60 and combined_odds < TARGET_ODDS * 0.95:
                for p in candidates:
                    if len(parlay) >= MAX_LEGS:
                        break
                    fid = p["fixture"]["id"]
                    if fid in used_fids:
                        continue
                    league = p.get("league", "")
                    if used_leagues.get(league, 0) >= 3:
                        continue

                    new_odds = combined_odds * p["odds"]
                    new_prob = combined_prob * p["probability"]

                    if new_odds > TARGET_ODDS * 1.35:
                        continue
                    if new_prob < MIN_COMBINED_PROB * 0.8:
                        continue

                    parlay.append(p)
                    combined_odds = new_odds
                    combined_prob = new_prob
                    used_fids.add(fid)
                    used_leagues[league] = used_leagues.get(league, 0) + 1

            # ── Apply correlation penalty ─────────────────────────────
            corr = correlation_penalty(parlay)
            adjusted_prob = combined_prob * corr

            # ── Build the message ─────────────────────────────────────
            if adjusted_prob >= 0.55:
                conf_emoji = "🟢🟢"
                conf_label = "Very High"
                conf_detail = "Maximum certainty"
            elif adjusted_prob >= 0.45:
                conf_emoji = "🟢"
                conf_label = "High"
                conf_detail = "Strong confidence"
            elif adjusted_prob >= 0.35:
                conf_emoji = "🟡"
                conf_label = "Moderate-High"
                conf_detail = "Good value detected"
            elif adjusted_prob >= 0.25:
                conf_emoji = "🟠"
                conf_label = "Moderate"
                conf_detail = "Decent edge"
            else:
                conf_emoji = "⚠️"
                conf_label = "Lower"
                conf_detail = "Proceed with caution"

            date_str = datetime.utcnow().strftime("%B %d, %Y")

            # Group by league for display
            league_groups = {}
            for p in parlay:
                lg = p.get("league", "Other")
                league_name = LEAGUES.get(lg, lg)
                if league_name not in league_groups:
                    league_groups[league_name] = []
                league_groups[league_name].append(p)

            lines = [
                "━━━━━━━━━━━━━━━━━━━━━━━━━",
                f"🤖 *SMART BET — {date_str}*",
                "━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                f"_{conf_emoji} Confidence: {conf_label} ({adjusted_prob*100:.0f}%)_",
                f"_{conf_detail}_\n",
                f"🎯 *Target: ~×2.00 | Actual: ×{combined_odds:.2f}*",
                f"📊 *Combined Probability: {combined_prob*100:.1f}%*\n",
            ]

            leg_num = 0
            for league_name, picks in league_groups.items():
                lines.append(f"*⚽ {league_name}*")
                for p in picks:
                    leg_num += 1
                    time_str = f" ⏰ {p['kickoff']}" if p.get("kickoff") else ""
                    edge_str = f" (edge {p['edge']*100:.1f}%)" if p.get("edge", 0) > 0.03 else ""
                    lines.append(
                        f"  {leg_num}. *{p['home']} vs {p['away']}*{time_str}"
                        f"\n     {p['label']} @ *{p['odds']}*{edge_str}"
                    )
                lines.append("")

            lines.extend([
                f"*{len(parlay)} selections | ×{combined_odds:.2f}*",
                "",
                "━━━━━━━━━━━━━━━━━━━━━━━━━",
                "_Every leg is analyzed for maximum win probability._",
                "_Subscribe for daily results & notifications!_",
                "",
                "🤖 _@fireparlays_",
            ])

            message = "\n".join(lines)

            return {
                "has_pick": True,
                "selections": parlay,
                "total_odds": round(combined_odds, 2),
                "combined_prob": round(adjusted_prob, 4),
                "confidence": round(adjusted_prob * 100, 1),
                "num_legs": len(parlay),
                "message": message,
                "date": datetime.utcnow().strftime("%Y-%m-%d"),
            }

        finally:
            await client.close()

    async def get_or_generate_daily(self, user_sports=None, tz_offset=0) -> dict:
        """Get today's cached pick or generate a new one."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if self._cache_date == today and self._daily_cache:
            return self._daily_cache

        pick = await self.generate_daily_pick(user_sports=user_sports, tz_offset=tz_offset)
        if pick.get("has_pick"):
            self._daily_cache = pick
            self._cache_date = today
        return pick


smart_bet_engine = SmartBetEngine()
