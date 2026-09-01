"""
dump_rsc_fields.py
------------------
Run this locally to see EVERY field efhub.com exposes for a player.

Usage:
    python dump_rsc_fields.py                    # uses default player (Messi)
    python dump_rsc_fields.py <player_id>        # e.g. python dump_rsc_fields.py 89136409091415

It prints:
  1. Every unique JSON key found anywhere in the RSC payload
  2. The full baseStats object
  3. Any keys that sit alongside baseStats in the same JSON object
  4. The raw RSC line containing baseStats (so you can inspect it directly)
"""

import re
import json
import sys
import requests
from typing import Any

BASE_URL = "https://efhub.com"
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


def collect_rsc(html: str) -> str:
    parts = []
    for m in _RSC_PUSH_RE.finditer(html):
        try:
            parts.append(json.loads(f'"{m.group(1)}"'))
        except (json.JSONDecodeError, ValueError):
            continue
    return "\n".join(parts)


def all_json_keys(text: str) -> set:
    """Extract every unique JSON key from a blob of text."""
    return set(re.findall(r'"([^"\\]+)"\s*:', text))


def extract_json_object(text: str, key: str):
    """Extract a JSON object by key using brace-depth counting."""
    pattern = rf'"{re.escape(key)}"\s*:\s*\{{'
    m = re.search(pattern, text)
    if not m:
        return None
    start = m.end() - 1
    depth = in_string = escape_next = False
    depth = 0
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
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def sibling_keys(rsc: str) -> dict:
    """
    Find the JSON object that contains 'baseStats' and return all its
    sibling keys with their raw values (truncated if long).
    """
    bs_pos = rsc.find('"baseStats"')
    if bs_pos < 0:
        return {}

    # Walk backwards to find the opening { of the parent object
    depth = 0
    in_str = esc = False
    start = -1
    for i in range(bs_pos, -1, -1):
        ch = rsc[i]
        if ch == "{" and not in_str:
            depth -= 1
            if depth <= -1:
                start = i
                break
        elif ch == "}" and not in_str:
            depth += 1

    if start < 0:
        return {}

    # Now walk forward to find the closing }
    depth = 0
    in_str = esc = False
    end = -1
    for i in range(start, len(rsc)):
        ch = rsc[i]
        if esc:
            esc = False
            continue
        if ch == "\\" and in_str:
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break

    if end < 0:
        return {}

    raw = rsc[start:end + 1]
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: just extract all keys from the slice
        return {k: "< parse error >" for k in all_json_keys(raw)}

    def _preview(v: Any) -> str:
        if isinstance(v, dict):
            return f"{{ {', '.join(list(v.keys())[:8])} {'...' if len(v) > 8 else ''} }}"
        if isinstance(v, list):
            return f"[ {len(v)} items ]"
        s = str(v)
        return s if len(s) <= 80 else s[:77] + "..."

    return {k: _preview(v) for k, v in obj.items()}


def main():
    # Accept a player ID as argument or fall back to a default (Messi)
    if len(sys.argv) > 1:
        player_id = sys.argv[1]
    else:
        # Fetch the index and pick the first result for "messi"
        print("No player ID given — fetching index to find Messi...")
        try:
            idx = requests.get(f"{BASE_URL}/search/player-index.json",
                               headers=HEADERS, timeout=15).json()
            messi = [p for p in idx if "messi" in p.get("e", "").lower()]
            if not messi:
                print("Could not find Messi in index. Pass a player ID manually.")
                sys.exit(1)
            player_id = messi[0]["i"]
            print(f"Using: {messi[0]['e']} (id={player_id})\n")
        except Exception as e:
            print(f"Index fetch failed: {e}")
            sys.exit(1)

    url = f"{BASE_URL}/en/players/{player_id}"
    print(f"Fetching: {url}\n")
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()

    rsc = collect_rsc(r.text)
    if not rsc:
        print("ERROR: No RSC payload found.")
        sys.exit(1)

    print("=" * 60)
    print("1. ALL JSON KEYS IN ENTIRE RSC PAYLOAD")
    print("=" * 60)
    for k in sorted(all_json_keys(rsc)):
        print(f"  {k}")

    print()
    print("=" * 60)
    print("2. baseStats CONTENTS")
    print("=" * 60)
    base_stats = extract_json_object(rsc, "baseStats")
    if base_stats:
        for k, v in sorted(base_stats.items()):
            print(f"  {k:<30} {v}")
    else:
        print("  (not found)")

    print()
    print("=" * 60)
    print("3. SIBLING KEYS (sit alongside baseStats in same object)")
    print("=" * 60)
    for k, v in sibling_keys(rsc).items():
        print(f"  {k:<30} {v}")

    print()
    print("=" * 60)
    print("4. RAW RSC LINE CONTAINING baseStats (first 2000 chars)")
    print("=" * 60)
    bs_pos = rsc.find('"baseStats"')
    if bs_pos >= 0:
        line_start = rsc.rfind("\n", 0, bs_pos) + 1
        line_end = rsc.find("\n", bs_pos)
        line = rsc[line_start: line_end if line_end >= 0 else len(rsc)]
        print(line[:2000])
        if len(line) > 2000:
            print(f"\n  ... ({len(line) - 2000} more chars)")
    else:
        print("  (baseStats not found in RSC)")


if __name__ == "__main__":
    main()
