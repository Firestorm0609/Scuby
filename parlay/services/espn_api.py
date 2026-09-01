import aiohttp
import asyncio
import time
import logging
from config import ESPN_BASE, LEAGUES

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=15)

# ── Rate limiting ─────────────────────────────────────────────────────────
_last_request_time = 0.0
_MIN_REQUEST_INTERVAL = 0.15  # 150ms between requests
_request_lock = asyncio.Lock()

# ── In-memory response cache (TTL-based) ──────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 180  # 3 minutes default for pre-match


def _cache_key(url: str, params: dict = None) -> str:
    if not params:
        return url
    sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return f"{url}?{sorted_params}"


def get_cached(key: str, ttl: int = CACHE_TTL):
    """Return cached value if fresh, else None."""
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < ttl:
            return val
        del _cache[key]
    return None


def set_cached(key: str, val):
    _cache[key] = (time.time(), val)
    # Prune stale entries periodically
    if len(_cache) > 200:
        now = time.time()
        stale = [k for k, (ts, _) in _cache.items() if now - ts > 600]
        for k in stale:
            del _cache[k]


def _american_to_decimal(american: str) -> float:
    """Convert American odds string to decimal. E.g. '+110' -> 2.10, '-150' -> 1.67."""
    if not american or american == "OFF":
        return None
    try:
        american = str(american).replace("+", "").strip()
        val = float(american)
        if val > 0:
            return round(1 + val / 100, 2)
        else:
            return round(1 + 100 / abs(val), 2)
    except (ValueError, TypeError):
        return None


def _extract_ou(ou_section: dict) -> float:
    """Extract Over/Under total from ESPN format."""
    if not ou_section:
        return None
    # ESPN format: total.over.current.line = 'o5.5' or 'u5.5'
    for sub in ["over", "under"]:
        sub_section = ou_section.get(sub, {})
        current = sub_section.get("current", {})
        line = current.get("line", "")
        if line:
            try:
                return float(line.replace("o", "").replace("u", "").strip())
            except (ValueError, TypeError):
                pass
        # Fall back to open/close
        for key in ["open", "close"]:
            section = sub_section.get(key, {})
            line = section.get("line", "")
            if line:
                try:
                    return float(line.replace("o", "").replace("u", "").strip())
                except (ValueError, TypeError):
                    pass
    return None


def _extract_spread(spread_section: dict) -> str:
    """Extract spread from ESPN format."""
    if not spread_section:
        return None
    current = spread_section.get("home", {}).get("current", {})
    line = current.get("line", "")
    if line:
        return line
    # Fall back to open/close
    for key in ["open", "close"]:
        section = spread_section.get("home", {}).get(key, {})
        if section and section.get("line"):
            return section["line"]
    return None


class ESPNClient:
    def __init__(self):
        self.session = None

    async def _get_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=_TIMEOUT)
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch_scoreboard(self, league: str, date: str = None, retries: int = 2):
        """Fetch fixtures + odds from ESPN with rate limiting and retry."""
        url = ESPN_BASE.format(path=league)
        params = {"dates": date} if date else {}

        # Check cache first
        ck = _cache_key(url, params)
        cached = get_cached(ck, ttl=120)  # 2 min for live data, 3 min for pre-match
        if cached is not None:
            return cached

        # Rate limit: wait between requests
        global _last_request_time
        async with _request_lock:
            elapsed = time.time() - _last_request_time
            if elapsed < _MIN_REQUEST_INTERVAL:
                await asyncio.sleep(_MIN_REQUEST_INTERVAL - elapsed)
            _last_request_time = time.time()

        session = await self._get_session()
        for attempt in range(retries + 1):
            try:
                async with session.get(url, params=params) as r:
                    if r.status == 403:
                        # Rate limited — wait and retry
                        if attempt < retries:
                            wait = 2 ** (attempt + 1)  # 2s, 4s
                            logger.warning(f"ESPN 403 for {league}, retrying in {wait}s...")
                            await asyncio.sleep(wait)
                            continue
                        return None
                    if r.status != 200:
                        return None
                    data = await r.json()
                    set_cached(ck, data)
                    return data
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(1)
                    continue
                return None
        return None

    async def fetch_all_leagues(self, date: str = None, leagues=None):
        """Fetch all leagues in parallel with a semaphore to avoid rate limits."""
        leagues = leagues or list(LEAGUES.keys())
        sem = asyncio.Semaphore(4)  # max 4 concurrent ESPN requests

        async def _fetch_one(lg):
            async with sem:
                return lg, await self.fetch_scoreboard(lg, date)

        tasks = [_fetch_one(lg) for lg in leagues]
        results = {}
        for coro in asyncio.as_completed(tasks):
            try:
                lg, data = await coro
                results[lg] = data
            except Exception:
                pass
        return results

    @staticmethod
    def parse_events(raw, league_code):
        """Extract normalized fixture data with odds. Supports multiple sports."""
        if not raw or "events" not in raw:
            return []

        fixtures = []
        sport = league_code.split("/")[0] if "/" in league_code else "soccer"

        for event in raw["events"]:
            try:
                comp = event["competitions"][0]
                competitors = comp["competitors"]

                if sport == "soccer":
                    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                else:
                    home = competitors[0] if len(competitors) > 0 else None
                    away = competitors[1] if len(competitors) > 1 else None

                if not home or not away:
                    continue

                home_form = home.get("form", "") if sport == "soccer" else ""
                away_form = away.get("form", "") if sport == "soccer" else ""
                _raw_odds = comp.get("odds")
                _odds_obj = _raw_odds[0] if _raw_odds and len(_raw_odds) > 0 and _raw_odds[0] else {}
                draw_odds = _odds_obj.get("drawOdds") if sport == "soccer" else None

                # Extract live game details (period, clock, situation)
                status_detail = event["status"].get("type", {}).get("shortDetail", "") or event["status"].get("type", {}).get("detail", "")
                status_state = event["status"]["type"]["state"]
                period = event["status"].get("period", 0)
                clock = event["status"].get("displayClock", "")

                # Extract rich team stats
                def _extract_team_stats(team_data):
                    stats = {}
                    for s in team_data.get("statistics", []):
                        abbr = s.get("abbreviation", "")
                        val = s.get("displayValue", "0")
                        try:
                            stats[abbr] = float(val)
                        except (ValueError, TypeError):
                            stats[abbr] = val
                    return stats

                home_stats = _extract_team_stats(home)
                away_stats = _extract_team_stats(away)
                home_rank = home.get("curatedRank", {}).get("current")
                away_rank = away.get("curatedRank", {}).get("current")

                fixture = {
                    "id": event["id"],
                    "league": league_code,
                    "sport": sport,
                    "date": event["date"],
                    "status": status_state,
                    "status_detail": status_detail,
                    "period": period,
                    "clock": clock,
                    "home_team": home["team"]["displayName"],
                    "away_team": away["team"]["displayName"],
                    "home_score": int(home.get("score", 0) or 0),
                    "away_score": int(away.get("score", 0) or 0),
                    "home_form": home_form,
                    "away_form": away_form,
                    "home_stats": home_stats,
                    "away_stats": away_stats,
                    "home_rank": home_rank,
                    "away_rank": away_rank,
                    "venue": comp.get("venue", {}).get("fullName", ""),
                    "odds": None,
                }

                _raw_odds = comp.get("odds")
                o = _raw_odds[0] if _raw_odds and len(_raw_odds) > 0 and _raw_odds[0] else None
                if o and isinstance(o, dict):

                    def _extract_odds(section):
                        """Extract moneyline odds from ESPN format, handling OFF/open/close."""
                        if not section:
                            return None
                        current = section.get("current", {}).get("odds")
                        if current and current != "OFF":
                            return _american_to_decimal(current)
                        # Fall back to close, then open
                        close = section.get("close", {}).get("odds")
                        if close and close != "OFF":
                            return _american_to_decimal(close)
                        open_val = section.get("open", {}).get("odds")
                        if open_val and open_val != "OFF":
                            return _american_to_decimal(open_val)
                        return None

                    ml = o.get("moneyline", {})
                    ou_section = o.get("total", {})
                    spread_section = o.get("pointSpread", {})

                    fixture["odds"] = {
                        "details":    o.get("details", ""),
                        "over_under": _extract_ou(ou_section),
                        "spread":     _extract_spread(spread_section),
                        "home_ml":    _extract_odds(ml.get("home")),
                        "away_ml":    _extract_odds(ml.get("away")),
                        "draw_odds":  _extract_odds(ml.get("draw")),
                        "provider":   o.get("provider", {}).get("name", "DraftKings"),
                    }
                fixtures.append(fixture)
            except Exception as e:
                continue
        return fixtures
