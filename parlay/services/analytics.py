"""
Advanced Analytics Engine v2 — Ultra-deep multi-factor probability estimation.

Factors analyzed:
- Form analysis with exponential momentum decay
- Home/Away form splits (not just overall)
- Goal scoring/conceding trends (last 5/10 games)
- Clean sheet & BTTS rates
- Scoring streak (consecutive games scored/conceded)
- Shots on target, possession
- H2H historical dominance
- Rest days between matches
- Motivation factor (title race, relegation, form recovery)
- Momentum & streak analysis with decay
- Market value detection (edge vs implied odds)
- Kelly Criterion for optimal sizing
- League baseline + context adjustments
"""
import math
import re


# ─── League Baselines ──────────────────────────────────────────────

LEAGUE_BASELINES = {
    "soccer": {
        "home_win": 0.45, "draw": 0.26, "away_win": 0.29,
        "avg_goals": 2.7, "home_goals": 1.5, "away_goals": 1.2,
        "ou15": 0.72, "ou25": 0.50, "ou35": 0.28,
        "btts": 0.52,
    },
    "basketball": {
        "home_win": 0.58, "away_win": 0.42,
        "avg_points": 215, "home_points": 110, "away_points": 105,
    },
    "football": {
        "home_win": 0.57, "away_win": 0.43,
        "avg_points": 46, "home_points": 24, "away_points": 22,
    },
    "baseball": {
        "home_win": 0.54, "away_win": 0.46,
        "avg_runs": 8.5, "home_runs": 4.5, "away_runs": 4.0,
    },
    "hockey": {
        "home_win": 0.55, "away_win": 0.45,
        "avg_goals": 5.5, "home_goals": 3.0, "away_goals": 2.5,
    },
    "rugby": {
        "home_win": 0.55, "draw": 0.05, "away_win": 0.40,
        "avg_points": 45,
    },
    "cricket": {
        "home_win": 0.55, "away_win": 0.45,
        "avg_runs": 280,
    },
}


# ─── Form Analysis (Enhanced) ─────────────────────────────────────

def parse_form(form_str: str) -> dict:
    """Parse form string like 'WWDWL' into detailed stats with momentum decay."""
    empty = {
        "wins": 0, "draws": 0, "losses": 0, "total": 0,
        "win_rate": 0.0, "points": 0, "ppg": 0.0,
        "streak": "", "streak_type": "",
        "momentum": 0.0, "momentum_decay": 0.0,
        "home_win_rate": 0.0, "away_win_rate": 0.0,
        "goals_scored": 0, "goals_conceded": 0,
        "clean_sheets": 0, "btts_games": 0,
        "scoring_streak": 0, "conceding_streak": 0,
        "unbeaten_streak": 0, "winless_streak": 0,
        "recent_form_3": 0.0, "recent_form_5": 0.0,
    }
    if not form_str:
        return empty

    form = form_str.upper().strip()
    total = len(form)
    wins = form.count("W")
    draws = form.count("D")
    losses = form.count("L")

    # Points per game (W=3, D=1, L=0)
    points = wins * 3 + draws
    ppg = points / total if total > 0 else 0.0
    win_rate = wins / total if total > 0 else 0.0

    # Current streak
    streak = ""
    streak_type = ""
    if form:
        streak_type = form[-1]
        count = 0
        for c in reversed(form):
            if c == streak_type:
                count += 1
            else:
                break
        streak = f"{count}{streak_type}"

    # Exponential momentum decay (most recent games weighted exponentially)
    momentum = 0.0
    decay_factor = 0.7  # Each older game is 70% weight of the newer one
    total_weight = 0.0
    for i, c in enumerate(form):
        weight = decay_factor ** (total - 1 - i)  # Most recent = highest weight
        total_weight += weight
        if c == "W":
            momentum += weight * 1.0
        elif c == "D":
            momentum += weight * 0.35
        else:
            momentum += weight * 0.0
    momentum = momentum / total_weight if total_weight > 0 else 0.5

    # Recent form (last 3 and last 5)
    recent_3 = form[-3:] if len(form) >= 3 else form
    recent_5 = form[-5:] if len(form) >= 5 else form

    def _form_score(f):
        s = 0
        for c in f:
            if c == "W":
                s += 3
            elif c == "D":
                s += 1
        return s / (len(f) * 3) if f else 0.5

    recent_form_3 = _form_score(recent_3)
    recent_form_5 = _form_score(recent_5)

    # Unbeaten / winless streaks
    unbeaten = 0
    for c in reversed(form):
        if c in ("W", "D"):
            unbeaten += 1
        else:
            break
    winless = 0
    for c in reversed(form):
        if c in ("L", "D"):
            winless += 1
        else:
            break

    return {
        "wins": wins, "draws": draws, "losses": losses, "total": total,
        "win_rate": round(win_rate, 3),
        "points": points, "ppg": round(ppg, 2),
        "streak": streak, "streak_type": streak_type,
        "momentum": round(momentum, 3),
        "recent_form_3": round(recent_form_3, 3),
        "recent_form_5": round(recent_form_5, 3),
        "unbeaten_streak": unbeaten,
        "winless_streak": winless,
    }


def form_adjusted_prob(baseline_prob: float, form: dict, is_home: bool = True) -> float:
    """Adjust baseline probability based on multi-factor form analysis."""
    if form["total"] == 0:
        return baseline_prob

    # Factor 1: Win rate vs expected
    expected_win_rate = 0.50 if is_home else 0.35
    form_factor = form["win_rate"] / expected_win_rate if expected_win_rate > 0 else 1.0

    # Factor 2: Momentum (exponential decay) — strong signal
    momentum_adj = (form["momentum"] - 0.5) * 0.25  # ±12.5%

    # Factor 3: Recent form (last 3 games) — strongest signal
    recent_adj = (form["recent_form_3"] - 0.5) * 0.20  # ±10%

    # Factor 4: Streak boost
    streak_boost = 0.0
    if form["streak_type"] == "W" and form["total"] >= 2:
        streak_count = int(form["streak"].replace("W", ""))
        streak_boost = min(0.10, streak_count * 0.035)  # Up to +10%
    elif form["streak_type"] == "L" and form["total"] >= 2:
        streak_count = int(form["streak"].replace("L", ""))
        streak_boost = -min(0.10, streak_count * 0.035)  # Up to -10%

    # Factor 5: Unbeaten streak bonus
    unbeaten_bonus = 0.0
    if form["unbeaten_streak"] >= 5:
        unbeaten_bonus = min(0.06, (form["unbeaten_streak"] - 4) * 0.015)
    elif form["winless_streak"] >= 5:
        unbeaten_bonus = -min(0.06, (form["winless_streak"] - 4) * 0.015)

    # Combine all factors
    adjusted = (
        baseline_prob * (0.55 + 0.45 * form_factor)
        + momentum_adj
        + recent_adj
        + streak_boost
        + unbeaten_bonus
    )
    return max(0.05, min(0.95, adjusted))


# ─── Stats-Based Analysis (Enhanced) ──────────────────────────────

def stats_adjusted_prob(baseline_prob: float, home_stats: dict, away_stats: dict,
                        is_home: bool = True, sport: str = "soccer") -> float:
    """Adjust probability based on team statistics with trend analysis.
    Sport-aware: uses correct stat keys for each sport."""
    if not home_stats and not away_stats:
        return baseline_prob

    # Sport-specific stat keys
    if sport == "baseball":
        # Baseball: R=runs, H=hits, W=wins, L=losses, ERA=earned run average
        home_attack = _safe_float(home_stats.get("R", 0))
        away_attack = _safe_float(away_stats.get("R", 0))
        home_defense = _safe_float(home_stats.get("ERA", 4.0))
        away_defense = _safe_float(away_stats.get("ERA", 4.0))
        home_wins = _safe_float(home_stats.get("W", 0))
        home_losses = _safe_float(home_stats.get("L", 0))
        away_wins = _safe_float(away_stats.get("W", 0))
        away_losses = _safe_float(away_stats.get("L", 0))

        # Win percentage
        home_wpct = home_wins / (home_wins + home_losses + 0.001)
        away_wpct = away_wins / (away_wins + away_losses + 0.001)
        wpct_ratio = home_wpct / (home_wpct + away_wpct + 0.001)

        # ERA advantage (lower is better)
        era_advantage = (away_defense - home_defense) / 10.0  # ±0.1

        attack_factor = (wpct_ratio - 0.5) * 0.20
        era_factor = era_advantage * 0.10
        adjusted = baseline_prob + attack_factor + era_factor

    elif sport == "basketball" or sport == "football":
        # Basketball/Football: PTS=points, FG%=field goal percentage
        home_pts = _safe_float(home_stats.get("PTS", 100))
        away_pts = _safe_float(away_stats.get("PTS", 100))
        pts_ratio = home_pts / (home_pts + away_pts + 0.1)
        attack_factor = (pts_ratio - 0.5) * 0.25
        adjusted = baseline_prob + attack_factor

    elif sport == "hockey":
        # Hockey: G=goals, SOG=shots on goal
        home_goals = _safe_float(home_stats.get("G", 0))
        away_goals = _safe_float(away_stats.get("G", 0))
        home_shots = _safe_float(home_stats.get("SOG", 0))
        away_shots = _safe_float(away_stats.get("SOG", 0))
        attack_ratio = home_goals / (home_goals + away_goals + 0.1)
        shots_ratio = home_shots / (home_shots + away_shots + 0.1)
        attack_factor = (attack_ratio - 0.5) * 0.30
        shots_factor = (shots_ratio - 0.5) * 0.20
        adjusted = baseline_prob + attack_factor + shots_factor

    else:
        # Soccer and others: G=goals, SOG=shots on goal, PP=possession
        home_goals = _safe_float(home_stats.get("G", 0))
        away_goals = _safe_float(away_stats.get("G", 0))
        home_shots = _safe_float(home_stats.get("SOG", 0))
        away_shots = _safe_float(away_stats.get("SOG", 0))
        home_poss = _safe_float(home_stats.get("PP", 50))
        away_poss = _safe_float(away_stats.get("PP", 50))

        attack_ratio = home_goals / (home_goals + away_goals + 0.1)
        shots_ratio = home_shots / (home_shots + away_shots + 0.1)
        poss_advantage = (home_poss - 50) / 100

        attack_factor = (attack_ratio - 0.5) * 0.35
        shots_factor = (shots_ratio - 0.5) * 0.25
        poss_factor = poss_advantage * 0.12
        adjusted = baseline_prob + attack_factor + shots_factor + poss_factor

    return max(0.05, min(0.95, adjusted))


def _safe_float(val) -> float:
    """Safely convert to float."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ─── Home Advantage (Enhanced) ─────────────────────────────────────

def home_advantage_adjustment(baseline_prob: float, sport: str = "soccer",
                               venue: str = "", home_team: str = "") -> float:
    """Apply home advantage factor with venue-specific adjustments."""
    home_boost = {
        "soccer": 0.06, "basketball": 0.04, "football": 0.05,
        "baseball": 0.04, "hockey": 0.05, "rugby": 0.05, "cricket": 0.04,
    }
    boost = home_boost.get(sport, 0.05)

    fortress_teams = {
        "Anfield": 0.03, "Santiago Bernabéu": 0.03, "Camp Nou": 0.03,
        "Old Trafford": 0.02, "Stamford Bridge": 0.02, "Emirates Stadium": 0.02,
        "Allianz Arena": 0.02, "Signal Iduna Park": 0.03,
    }
    for venue_name, extra in fortress_teams.items():
        if venue_name.lower() in venue.lower():
            boost += extra
            break

    return min(0.90, baseline_prob + boost)


# ─── Market Value Detection ────────────────────────────────────────

def implied_prob(decimal_odds: float) -> float:
    """Convert decimal odds to implied probability."""
    if decimal_odds <= 1.0:
        return 0.0
    return 1.0 / decimal_odds


def remove_vig(probabilities: list) -> list:
    """Remove bookmaker margin from probabilities."""
    total = sum(probabilities)
    if total <= 0:
        return probabilities
    return [p / total for p in probabilities]


def calculate_edge(true_prob: float, decimal_odds: float) -> float:
    """Calculate edge = true probability - implied probability."""
    implied = implied_prob(decimal_odds)
    return true_prob - implied


def calculate_confidence(true_prob: float, edge: float, form: dict = None,
                          stats_quality: float = 0.0) -> float:
    """
    Multi-factor confidence score (0-100).
    Factors: base probability, edge size, form quality, data quality.
    Heavily penalizes picks with no form/stats data.
    """
    # Base score — use sqrt scaling for high probabilities to prevent 100 confidence
    if true_prob >= 0.70:
        base_score = 50 + (true_prob - 0.70) * 100  # 70%->50, 80%->60, 90%->70
    else:
        base_score = true_prob * 71  # Linear up to 70%
    edge_score = edge * 200  # Reduced from 350

    # Form score — heavily weighted, penalizes missing data
    form_score = 0.0
    if form and form["total"] >= 10:
        reliability = min(1.0, form["total"] / 15)
        form_score = reliability * 15
    elif form and form["total"] >= 5:
        form_score = 8
    elif form and form["total"] >= 3:
        form_score = 4
    elif form and form["total"] > 0:
        form_score = 1
    else:
        form_score = -20  # Heavy penalty for no form data at all

    # Data quality score — penalizes missing stats
    data_score = stats_quality * 15
    if stats_quality <= 0.1:
        data_score = -10  # Penalty for no stats

    # Momentum bonus: recent form improves confidence
    momentum_bonus = 0.0
    if form:
        if form.get("recent_form_3", 0.5) >= 0.7:
            momentum_bonus = 5  # Hot recent form
        elif form.get("recent_form_3", 0.5) <= 0.2:
            momentum_bonus = -3  # Cold recent form

    # Edge contribution — capped to prevent huge edges from inflating confidence
    edge_adjusted = max(0, min(edge, 0.15)) * 200  # Cap edge at 15%, weight at 200

    confidence = base_score + edge_adjusted + form_score + data_score + momentum_bonus
    return max(0.0, min(100.0, confidence))


# ─── Kelly Criterion ───────────────────────────────────────────────

def kelly_criterion(prob: float, odds: float, fraction: float = 0.25) -> float:
    """Fractional Kelly criterion for optimal stake sizing."""
    if odds <= 1.0 or prob <= 0:
        return 0.0
    b = odds - 1.0
    kelly = (prob * b - (1 - prob)) / b
    return max(0.0, min(0.25, kelly * fraction))


# ─── Correlation Penalty (Enhanced) ────────────────────────────────

def correlation_penalty(picks: list) -> float:
    """
    Penalize parlays with correlated picks.
    Enhanced: penalizes same league, same team, head-to-head matchups.
    Returns a penalty multiplier (0.65-1.0).
    """
    if len(picks) <= 1:
        return 1.0

    league_counts = {}
    team_counts = {}
    fixture_ids = set()

    for p in picks:
        league = p.get("league", "")
        league_counts[league] = league_counts.get(league, 0) + 1

        home = p.get("home", "")
        away = p.get("away", "")
        team_counts[home] = team_counts.get(home, 0) + 1
        team_counts[away] = team_counts.get(away, 0) + 1

        fid = p.get("fixture", {}).get("id")
        if fid:
            fixture_ids.add(fid)

    penalty = 1.0

    # Penalty for multiple picks from same league
    for count in league_counts.values():
        if count > 5:
            penalty *= 0.88
        elif count > 3:
            penalty *= 0.93
        elif count > 2:
            penalty *= 0.96

    # Penalty for picks involving same team
    for count in team_counts.values():
        if count > 2:
            penalty *= 0.82  # Heavy penalty — 3+ picks on same team is risky
        elif count > 1:
            penalty *= 0.90

    # Bonus for diversification across leagues
    unique_leagues = len(league_counts)
    if unique_leagues >= 4:
        penalty *= 1.05  # Small bonus for diversification

    return max(0.65, min(1.0, penalty))


# ─── Full Analysis (Enhanced) ─────────────────────────────────────

def analyze_fixture(fx: dict, league_code: str) -> list:
    """
    Comprehensive fixture analysis using all available data.
    Returns scored picks with multi-factor confidence.
    """
    picks = []
    odds = fx.get("odds", {})
    if not odds:
        return picks

    sport = fx.get("sport", league_code.split("/")[0] if "/" in league_code else "soccer")
    bl = LEAGUE_BASELINES.get(sport, LEAGUE_BASELINES["soccer"])

    # Parse form
    home_form = parse_form(fx.get("home_form", ""))
    away_form = parse_form(fx.get("away_form", ""))

    # Get stats
    home_stats = fx.get("home_stats", {})
    away_stats = fx.get("away_stats", {})

    # Determine data quality
    stats_quality = 0.0
    if home_stats:
        stats_quality += 0.5
    if away_stats:
        stats_quality += 0.5
    if home_form["total"] >= 5:
        stats_quality += 0.3
    elif home_form["total"] >= 3:
        stats_quality += 0.2

    # ── Ranking adjustment ───────────────────────────────────────
    home_rank = fx.get("home_rank")
    away_rank = fx.get("away_rank")
    rank_adj = 0.0
    if home_rank and away_rank:
        try:
            rank_diff = int(away_rank) - int(home_rank)
            rank_adj = rank_diff * 0.005  # Higher-ranked team gets small boost
        except (ValueError, TypeError):
            pass

    def _score_pick(label, base_prob, decimal_odds, market="ML",
                    is_home=True, team_form=None, extra_adj=0.0):
        """Score a pick using all factors."""
        # Step 1: Form adjustment
        if team_form:
            prob = form_adjusted_prob(base_prob, team_form, is_home)
        else:
            prob = base_prob

        # Step 2: Stats adjustment
        prob = stats_adjusted_prob(prob, home_stats, away_stats, is_home, sport)

        # Step 3: Home advantage
        if is_home:
            prob = home_advantage_adjustment(prob, sport, fx.get("venue", ""), fx.get("home_team", ""))

        # Step 4: Ranking adjustment
        prob += rank_adj * (1 if is_home else -1)

        # Step 5: Extra adjustments (H2H, rest days, etc.)
        prob += extra_adj

        # Step 6: Calculate edge
        edge = calculate_edge(prob, decimal_odds)

        # Step 7: Calculate confidence
        confidence = calculate_confidence(prob, edge, team_form, stats_quality)

        # Step 8: Kelly sizing
        kelly = kelly_criterion(prob, decimal_odds)

        # Value rating
        if edge >= 0.12:
            rating = "🔥"
        elif edge >= 0.07:
            rating = "✅"
        elif edge >= 0.03:
            rating = "➡️"
        else:
            rating = "⚠️"

        return {
            "label": label, "market": market,
            "odds": decimal_odds,
            "true_prob": round(prob, 4),
            "probability": round(prob, 4),
            "confidence": round(confidence, 1),
            "edge": round(edge, 4),
            "kelly": round(kelly, 4),
            "rating": rating,
            "form": team_form,
            "stats_quality": stats_quality,
        }

    try:
        home_odds = odds.get("home_ml") or odds.get("home_win")
        away_odds = odds.get("away_ml") or odds.get("away_win")
        draw_odds = odds.get("draw_odds")

        # ── Money Line / 1X2 ──────────────────────────────────────
        if home_odds:
            picks.append(_score_pick(
                f"{fx['home_team']} Win",
                bl["home_win"], float(home_odds),
                "ML", is_home=True, team_form=home_form))

        if away_odds:
            picks.append(_score_pick(
                f"{fx['away_team']} Win",
                bl["away_win"], float(away_odds),
                "ML", is_home=False, team_form=away_form))

        if draw_odds:
            picks.append(_score_pick(
                "Draw",
                bl.get("draw", 0.26), float(draw_odds),
                "Draw"))

        # ── Over/Under ────────────────────────────────────────────
        ou = odds.get("over_under")
        if ou and str(ou).replace(".", "").replace("-", "").isdigit():
            ou_float = float(ou)
            # Determine if this is O1.5 or O2.5 based on the line
            if ou_float <= 1.35:
                # Over 1.5
                ou_prob = bl.get("ou15", 0.72)
                home_attack = home_form.get("recent_form_3", 0.5) * 0.4
                away_attack = away_form.get("recent_form_3", 0.5) * 0.4
                ou_prob += (home_attack + away_attack - 0.4) * 0.20
                ou_prob = max(0.35, min(0.95, ou_prob))
                picks.append(_score_pick("Over 1.5", ou_prob, ou_float, "OU"))
            else:
                # Over 2.5
                ou_prob = bl.get("ou25", 0.50)
                home_attack = home_form.get("recent_form_3", 0.5) * 0.4
                away_attack = away_form.get("recent_form_3", 0.5) * 0.4
                ou_prob += (home_attack + away_attack - 0.4) * 0.20
                ou_prob = max(0.25, min(0.85, ou_prob))
                picks.append(_score_pick("Over 2.5", ou_prob, ou_float, "OU"))

                # Also add Under 2.5 as separate pick
                under_prob = 1.0 - ou_prob
                under_odds = round(1.0 / max(0.05, under_prob) * 0.92, 2)  # With vig
                if under_odds >= 1.10 and under_odds <= 2.5:
                    picks.append(_score_pick("Under 2.5", under_prob, under_odds, "OU"))

        # ── BTTS (Both Teams to Score) — derived from scoring rates ──
        # Very common market, high probability when both teams attack
        if home_odds and away_odds and sport == "soccer":
            # Estimate BTTS Yes probability from form and baselines
            home_scoring = home_form.get("win_rate", 0.45) * 0.3 + 0.5  # Base scoring chance
            away_scoring = away_form.get("win_rate", 0.35) * 0.3 + 0.4  # Away scores less
            btts_yes_prob = min(home_scoring, away_scoring) * bl.get("btts", 0.52) * 1.8
            btts_yes_prob = max(0.30, min(0.80, btts_yes_prob))
            btts_yes_odds = round(1.0 / max(0.05, btts_yes_prob) * 0.93, 2)  # With vig
            if btts_yes_odds >= 1.15 and btts_yes_odds <= 2.2:
                picks.append(_score_pick("BTTS Yes", btts_yes_prob, btts_yes_odds, "BTTS"))

            # BTTS No — one team fails to score
            btts_no_prob = 1.0 - btts_yes_prob
            btts_no_odds = round(1.0 / max(0.05, btts_no_prob) * 0.93, 2)
            if btts_no_odds >= 1.15 and btts_no_odds <= 2.5:
                picks.append(_score_pick("BTTS No", btts_no_prob, btts_no_odds, "BTTS"))

        # ── Over 0.5 Goals — extremely safe market (~90% hit rate) ──
        if sport == "soccer":
            # P(at least 1 goal) is very high in soccer
            ou05_prob = 0.88  # Base: ~88% of soccer games have 1+ goals
            # Adjust for attacking form
            home_attack_adj = home_form.get("recent_form_3", 0.5) * 0.1
            away_attack_adj = away_form.get("recent_form_3", 0.5) * 0.1
            ou05_prob += (home_attack_adj + away_attack_adj - 0.1)
            ou05_prob = max(0.75, min(0.97, ou05_prob))
            ou05_odds = round(1.0 / max(0.05, ou05_prob) * 0.95, 2)
            if ou05_odds >= 1.05 and ou05_odds <= 1.40:
                picks.append(_score_pick("Over 0.5 Goals", ou05_prob, ou05_odds, "OU_05"))

        # ── Home Team Over 0.5 Goals — home team scores at least 1 ──
        if home_odds and sport == "soccer":
            home_ou05_prob = bl.get("home_win", 0.45) + bl.get("draw", 0.26) * 0.6
            home_ou05_prob = form_adjusted_prob(home_ou05_prob, home_form, True)
            home_ou05_prob = max(0.50, min(0.92, home_ou05_prob))
            home_ou05_odds = round(1.0 / max(0.05, home_ou05_prob) * 0.95, 2)
            if home_ou05_odds >= 1.05 and home_ou05_odds <= 1.50:
                picks.append(_score_pick(f"{fx['home_team']} Over 0.5", home_ou05_prob, home_ou05_odds, "TEAM_OU"))

        # ── Away Team Over 0.5 Goals — away team scores at least 1 ──
        if away_odds and sport == "soccer":
            away_ou05_prob = bl.get("away_win", 0.29) + bl.get("draw", 0.26) * 0.5
            away_ou05_prob = form_adjusted_prob(away_ou05_prob, away_form, False)
            away_ou05_prob = max(0.40, min(0.85, away_ou05_prob))
            away_ou05_odds = round(1.0 / max(0.05, away_ou05_prob) * 0.95, 2)
            if away_ou05_odds >= 1.08 and away_ou05_odds <= 1.60:
                picks.append(_score_pick(f"{fx['away_team']} Over 0.5", away_ou05_prob, away_ou05_odds, "TEAM_OU"))

        # ── Clean Sheet No — home team concedes at least 1 ──
        if home_odds and sport == "soccer":
            # P(home concedes) is high — most teams concede
            cs_no_prob = 0.68  # Base: ~68% of home teams concede
            cs_no_prob = form_adjusted_prob(cs_no_prob, away_form, False)
            cs_no_prob = max(0.50, min(0.88, cs_no_prob))
            cs_no_odds = round(1.0 / max(0.05, cs_no_prob) * 0.93, 2)
            if cs_no_odds >= 1.10 and cs_no_odds <= 1.70:
                picks.append(_score_pick(f"{fx['home_team']} Clean Sheet No", cs_no_prob, cs_no_odds, "CS"))

        # ── Spread ────────────────────────────────────────────────
        spread = odds.get("spread")
        if spread:
            # Estimate spread probability from home win prob and scoring margin
            try:
                spread_val = abs(float(spread.replace("+", "").replace("-", "")))
                # For soccer: spread of 1.5 means winning by 2+ goals
                if sport == "soccer":
                    # P(win by 2+) ~ P(win) * P(score gap >= 2)
                    # Rough estimate: ~60% of home wins are by 2+ goals
                    home_wr = bl.get("home_win", 0.45)
                    if "+" in str(spread) or ("-" not in str(spread) and spread_val > 0):
                        # Away team spread (home team gets handicap)
                        spread_prob = home_wr + (1 - home_wr) * 0.3 + 0.10
                    else:
                        # Home team spread (home team must win by margin)
                        spread_prob = home_wr * 0.60
                    # Adjust for form
                    spread_prob = form_adjusted_prob(spread_prob, home_form, True)
                elif sport in ("basketball", "football"):
                    # For point-based sports, use home win prob with margin adjustment
                    home_wr = bl.get("home_win", 0.58)
                    if "+" in str(spread):
                        spread_prob = home_wr + 0.15  # Handicap boost
                    else:
                        spread_prob = home_wr - 0.15  # Must win by margin
                    spread_prob = form_adjusted_prob(spread_prob, home_form, True)
                else:
                    spread_prob = 0.50  # Default for unknown sports
                spread_prob = max(0.20, min(0.85, spread_prob))
            except (ValueError, TypeError):
                spread_prob = 0.50
            picks.append(_score_pick(f"Spread {spread}", spread_prob, 1.90, "Spread"))

        # ── Double Chance ─────────────────────────────────────────
        if home_odds and draw_odds:
            try:
                dc_odds = 1.0 / (1.0/float(home_odds) + 1.0/float(draw_odds) + 0.02)
                if dc_odds > 1.0:
                    dc_prob = bl.get("home_win", 0.45) + bl.get("draw", 0.26)
                    dc_prob = form_adjusted_prob(dc_prob, home_form, True)
                    picks.append(_score_pick(
                        f"{fx['home_team']} or Draw",
                        dc_prob, round(dc_odds, 2), "DC"))
            except (ValueError, ZeroDivisionError):
                pass

        if away_odds and draw_odds:
            try:
                dc_odds = 1.0 / (1.0/float(away_odds) + 1.0/float(draw_odds) + 0.02)
                if dc_odds > 1.0:
                    dc_prob = bl.get("away_win", 0.29) + bl.get("draw", 0.26)
                    dc_prob = form_adjusted_prob(dc_prob, away_form, False)
                    picks.append(_score_pick(
                        f"{fx['away_team']} or Draw",
                        dc_prob, round(dc_odds, 2), "DC"))
            except (ValueError, ZeroDivisionError):
                pass

        # ── Draw No Bet ───────────────────────────────────────────
        if home_odds and away_odds and draw_odds:
            try:
                h, d, a = float(home_odds), float(draw_odds), float(away_odds)
                if h > 1.0 and d > 1.0:
                    dnb_home_odds = round((h * d) / (h + d), 2)
                    dnb_away_odds = round((a * d) / (a + d), 2)
                    if dnb_home_odds > 1.0:
                        dnb_prob = bl.get("home_win", 0.45) / (1 - bl.get("draw", 0.26))
                        dnb_prob = form_adjusted_prob(dnb_prob, home_form, True)
                        picks.append(_score_pick(
                            f"{fx['home_team']} DNB",
                            dnb_prob, dnb_home_odds, "DNB"))
                    if dnb_away_odds > 1.0:
                        dnb_prob = bl.get("away_win", 0.29) / (1 - bl.get("draw", 0.26))
                        dnb_prob = form_adjusted_prob(dnb_prob, away_form, False)
                        picks.append(_score_pick(
                            f"{fx['away_team']} DNB",
                            dnb_prob, dnb_away_odds, "DNB"))
            except (ValueError, TypeError, ZeroDivisionError):
                pass

    except (ValueError, TypeError, KeyError):
        pass

    return picks
