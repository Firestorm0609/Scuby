"""Scraper for efhub.com player data.

efhub.com uses Next.js App Router (not Pages Router), so player data is
embedded in RSC (React Server Components) payload chunks:

    self.__next_f.push([1,"<json-escaped RSC text>"])

Each chunk's string is JSON-encoded, so \" -> " after json.loads().
The unescaped text contains lines like:
    f:["$","$L3f",null,{"baseStats":{...},"name":"...","position":"..."}]

The RSC payload contains TWO useful sources for player data:

  1. Top-level sibling object (alongside baseStats):
       { baseStats, position, additionalPositions, height, weakFootAccuracy,
         playingStyle, initialBoostLeftId, initialLevelCap, children }

  2. Nested player object (inside children[][][player]):
       { id, name, nameJa, team, league, nationality, overallRating, age,
         height, weight, preferredFoot, weakFootUsage, weakFootAccuracy,
         form, condition, injuryResistance, skills, comSkills, imageUrl,
         position, playingStyle, additionalPositions (as RSC ref — skip),
         stats (as RSC ref — skip) }

  3. maxStats object (elsewhere in RSC):
       { offensiveAwareness, ballControl, ... } — stats at level cap
"""
import re
import json
import logging
import requests
from typing import Any, Optional

logger = logging.getLogger(__name__)

BASE_URL  = "https://efhub.com"
INDEX_URL = f"{BASE_URL}/search/player-index.json"
BOOSTS_URL = f"{BASE_URL}/data/boosts.json"

# Next.js injects UI label strings before real player data in the RSC tree.
# Any extracted string that matches one of these is a placeholder, not a value.
_UI_PLACEHOLDER_VALUES: frozenset = frozenset({
    "Card Type", "Position", "Playing Style", "Name",
    "Next.MetadataOutlet", "Player Name", "Club", "Nation",
})

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_RSC_PUSH_RE = re.compile(
    r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)',
    re.DOTALL,
)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _collect_rsc_text(html: str) -> str:
    """Unescape and concatenate all RSC push chunks from the page HTML."""
    parts = []
    for m in _RSC_PUSH_RE.finditer(html):
        try:
            parts.append(json.loads(f'"{m.group(1)}"'))
        except (json.JSONDecodeError, ValueError):
            continue
    return "\n".join(parts)


def _extract_json_object(text: str, key: str) -> Optional[dict]:
    """Extract a JSON object for *key* using brace-depth counting."""
    pattern = rf'"{re.escape(key)}"\s*:\s*\{{'
    m = re.search(pattern, text)
    if not m:
        return None

    start = m.end() - 1
    depth = 0
    in_string = False
    escape_next = False

    for i in range(start, len(text)):
        ch = text[i]
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                raw = text[start: i + 1]
                try:
                    return json.loads(raw)
                except json.JSONDecodeError as exc:
                    logger.debug("JSON parse error extracting '%s': %s", key, exc)
                    return None
    return None


def _extract_json_array(text: str, key: str) -> Optional[list]:
    """Extract a JSON array for *key* using bracket-depth counting."""
    pattern = rf'"{re.escape(key)}"\s*:\s*\['
    m = re.search(pattern, text)
    if not m:
        return None

    start = m.end() - 1
    depth = 0
    in_string = False
    escape_next = False

    for i in range(start, len(text)):
        ch = text[i]
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                raw = text[start: i + 1]
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return None
    return None


def _extract_str(text: str, key: str) -> Optional[str]:
    m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', text)
    return m.group(1) if m else None


def _extract_int(text: str, *keys: str) -> Optional[int]:
    for key in keys:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*(\d+)', text)
        if m:
            return int(m.group(1))
    return None


def _safe_str(val: Any) -> Optional[str]:
    """Return val if it's a non-empty string not in the placeholder set."""
    if isinstance(val, str) and val and val not in _UI_PLACEHOLDER_VALUES:
        return val
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_index() -> list:
    """Fetch the full player index (id, name, overall)."""
    try:
        r = requests.get(INDEX_URL, timeout=15, headers=HEADERS)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.error("Failed to fetch player index: %s", exc)
        return []


def search_players(query: str, index_data: Optional[list] = None) -> list:
    """Search players by name. Returns list of {id, name, overall, ...}."""
    if index_data is None:
        index_data = fetch_player_index()
    if not index_data:
        return []

    q = query.lower().strip()
    results = []
    for p in index_data:
        name = p.get("e", "")
        if q in name.lower():
            # eFootball 2027 index: {i, e, j, c, o}
            #   i = player ID, e = English name, j = Japanese name,
            #   c = team/nationality, o = boosted overall (with max boosts)
            entry: dict = {
                "id":      p["i"],
                "name":    name,
                "overall": p.get("o", 0),  # boosted OVR (shown as max potential)
            }
            if p.get("c"):
                entry["team"] = p["c"]
            results.append(entry)

    # Show more results — default sort by boosted OVR descending
    # Use sort_by="newest" to sort by player ID (higher = newer card)
    return sorted(results, key=lambda x: -x["overall"])[:20]


def fetch_player_detail(player_id: int) -> Optional[dict]:
    """Fetch full player detail from the efhub.com RSC payload.

    Reads every available field:
      baseStats, maxStats, name, nameJa, position, additionalPositions,
      playingStyle, overallRating, levelCap, age, height, weight,
      preferredFoot, weakFootAccuracy, weakFootUsage, form, condition,
      injuryResistance, skills, comSkills, team, league, nationality,
      boostId, imageUrl, slug
    """
    url = f"{BASE_URL}/en/players/{player_id}"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("HTTP error fetching player %s: %s", player_id, exc)
        return None

    html = r.text
    rsc  = _collect_rsc_text(html)

    if not rsc:
        logger.warning("No RSC chunks found for player %s", player_id)
        return None
    if "baseStats" not in rsc:
        logger.warning("No baseStats found in RSC payload for player %s", player_id)
        return None

    # ------------------------------------------------------------------
    # Locate the RSC line that contains baseStats — everything we need
    # is on this single line (or searchable within it).
    # ------------------------------------------------------------------
    bs_pos     = rsc.find('"baseStats"')
    line_start = rsc.rfind("\n", 0, bs_pos) + 1
    line_end   = rsc.find("\n", bs_pos)
    chunk      = rsc[line_start: line_end if line_end != -1 else len(rsc)]

    # ------------------------------------------------------------------
    # SOURCE 1 — baseStats object
    # ------------------------------------------------------------------
    base_stats = _extract_json_object(chunk, "baseStats")
    if not base_stats:
        logger.warning("Could not parse baseStats for player %s", player_id)
        return None

    player_data: dict = {
        "playerId":  str(player_id),
        "baseStats": base_stats,
    }

    # ------------------------------------------------------------------
    # SOURCE 2 — player object (inside children[][][player])
    # This is the richest source: name, skills, team, league, etc.
    # Note: some fields in here are RSC references ("$f:props:...") —
    # json.loads still parses them as strings; we just skip them below.
    # ------------------------------------------------------------------
    player_obj = _extract_json_object(chunk, "player")

    if player_obj:
        # String fields
        for src, dst in (
            ("name",          "name"),
            ("nameJa",        "nameJa"),
            ("team",          "team"),
            ("league",        "league"),
            ("nationality",   "nationality"),
            ("nationalityCode","nationalityCode"),
            ("playingStyle",  "playingStyle"),
            ("position",      "position"),
            ("preferredFoot", "preferredFoot"),
            ("slug",          "slug"),
            ("imageUrl",      "imageUrl"),
        ):
            v = _safe_str(player_obj.get(src))
            if v:
                player_data[dst] = v

        # Integer fields
        for src, dst in (
            ("overallRating",   "overall"),
            ("age",             "age"),
            ("height",          "height"),
            ("weight",          "weight"),
            ("weakFootAccuracy","weakFootAccuracy"),
            ("weakFootUsage",   "weakFootUsage"),
            ("form",            "form"),
            ("condition",       "condition"),
            ("injuryResistance","injuryResistance"),
        ):
            v = player_obj.get(src)
            if isinstance(v, int):
                player_data[dst] = v

        # List fields — only keep if actually a list (not an RSC "$" reference)
        for src, dst in (
            ("skills",    "skills"),
            ("comSkills", "comSkills"),
        ):
            v = player_obj.get(src)
            if isinstance(v, list):
                player_data[dst] = v

        # Player model (hidden body stats)
        player_model = player_obj.get("playerModel")
        if isinstance(player_model, dict):
            player_data["playerModel"] = player_model

    # ------------------------------------------------------------------
    # SOURCE 3 — playerSkills array (in a separate RSC chunk, referenced
    # by the player object as "$f:props:children:props:playerSkills")
    # ------------------------------------------------------------------
    if "skills" not in player_data or not player_data.get("skills"):
        skills_arr = _extract_json_array(rsc, "playerSkills")
        if skills_arr and isinstance(skills_arr, list):
            player_data["skills"] = skills_arr

    # ------------------------------------------------------------------
    # SOURCE 4 — top-level sibling keys (alongside baseStats in same obj)
    # Fill in anything player_obj didn't provide.
    # ------------------------------------------------------------------

    # additionalPositions lives here as the real array (player_obj has a ref)
    if "additionalPositions" not in player_data:
        arr = _extract_json_array(chunk, "additionalPositions")
        if arr:
            player_data["additionalPositions"] = arr

    # Scalar sibling fields
    _sibling_int = [
        ("levelCap",        ("initialLevelCap", "levelCap")),
        ("boostId",         ("initialBoostLeftId",)),
        ("overall",         ("overallRating", "overall")),
        ("height",          ("height",)),
        ("weakFootAccuracy",("weakFootAccuracy",)),
    ]
    for dst, src_keys in _sibling_int:
        if dst not in player_data:
            v = _extract_int(chunk, *src_keys)
            if v is not None:
                player_data[dst] = v

    _sibling_str = [
        ("position",    ("position",)),
        ("playingStyle",("playingStyle",)),
    ]
    for dst, src_keys in _sibling_str:
        if dst not in player_data:
            for k in src_keys:
                v = _extract_str(chunk, k)
                if _safe_str(v):
                    player_data[dst] = v
                    break

    # ------------------------------------------------------------------
    # SOURCE 5 — name fallback chain
    # ------------------------------------------------------------------
    if not player_data.get("name"):
        v = _extract_str(chunk, "name") or _extract_str(rsc, "name")
        if not v:
            title_m = re.search(r"<title[^>]*>([^<]+)</title>", html)
            if title_m:
                v = title_m.group(1).split("\u2014")[0].split("--")[0].strip()
        player_data["name"] = v or f"Player {player_id}"

    # ------------------------------------------------------------------
    # SOURCE 6 — imageUrl fallback to og:image meta tag
    # ------------------------------------------------------------------
    if not player_data.get("imageUrl"):
        for pat in (
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        ):
            og_m = re.search(pat, html)
            if og_m:
                player_data["imageUrl"] = og_m.group(1)
                break

    return player_data


def fetch_boosts() -> list:
    """Fetch booster definitions."""
    try:
        r = requests.get(BOOSTS_URL, timeout=15, headers=HEADERS)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.error("Failed to fetch boosts: %s", exc)
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing player index fetch...")
    idx = fetch_player_index()
    print(f"Got {len(idx)} players in index")
    messi = [p for p in idx if "messi" in p.get("e", "").lower()]
    if messi:
        pid = messi[0]["i"]
        print(f"\nTesting detail fetch for {messi[0]['e']} (id={pid})")
        detail = fetch_player_detail(pid)
        if detail:
            skip = {"baseStats", "maxStats", "playerModel"}
            for k, v in detail.items():
                if k not in skip:
                    print(f"  {k:<22} {v}")
            print(f"\n  baseStats  ({len(detail.get('baseStats', {}))} stats):")
            for sk, sv in sorted(detail.get("baseStats", {}).items()):
                max_v = detail.get("maxStats", {}).get(sk, "?")
                print(f"    {sk:<26} base={sv:<4} max={max_v}")
        else:
            print("Failed to fetch detail")
    else:
        print("No Messi found in index")
