"""
Run this locally:  python debug_efhub.py <player_id>
It dumps everything useful about the page so we can fix the scraper.
"""
import re
import sys
import json
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

player_id = sys.argv[1] if len(sys.argv) > 1 else "89135067035344"
url = f"https://efhub.com/en/players/{player_id}"
print(f"Fetching: {url}\n")

r = requests.get(url, timeout=15, headers=HEADERS)
print(f"Status: {r.status_code}")
html = r.text
print(f"HTML length: {len(html):,} chars\n")

# ── 1. __NEXT_DATA__ (Pages Router) ─────────────────────────────────────────
nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
if nd:
    print("✅ __NEXT_DATA__ FOUND")
    try:
        data = json.loads(nd.group(1))
        print(json.dumps(data, indent=2)[:3000])
    except Exception as e:
        print(f"  Parse error: {e}")
        print(nd.group(1)[:500])
else:
    print("❌ __NEXT_DATA__ NOT FOUND — likely App Router (RSC)")

# ── 2. RSC chunks  self.__next_f.push([...]) ────────────────────────────────
rsc_chunks = re.findall(r'self\.__next_f\.push\(\[(.*?)\]\)', html, re.DOTALL)
print(f"\n{'✅' if rsc_chunks else '❌'} RSC chunks found: {len(rsc_chunks)}")
for i, chunk in enumerate(rsc_chunks[:5]):
    print(f"\n--- RSC chunk {i} (first 400 chars) ---")
    print(chunk[:400])

# ── 3. Raw search for "baseStats" anywhere ──────────────────────────────────
if "baseStats" in html:
    idx = html.index("baseStats")
    print(f"\n✅ 'baseStats' found at char {idx}")
    print("  Context (200 chars around it):")
    print(repr(html[max(0, idx-50):idx+150]))
else:
    print("\n❌ 'baseStats' NOT found anywhere in the HTML")

# ── 4. What JSON-ish script tags exist? ─────────────────────────────────────
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
print(f"\nTotal <script> tags: {len(scripts)}")
for i, s in enumerate(scripts):
    s = s.strip()
    if s and not s.startswith("//") and len(s) > 20:
        print(f"\n  script[{i}] ({len(s)} chars): {repr(s[:120])}")

# ── 5. Any JSON API endpoints referenced? ───────────────────────────────────
api_urls = re.findall(r'https?://[^\s"\'<>]+(?:json|api)[^\s"\'<>]*', html)
print(f"\nAPI-like URLs found: {set(api_urls)}")

# ── 6. Save full HTML for manual inspection ──────────────────────────────────
with open(f"efhub_{player_id}.html", "w", encoding="utf-8") as f:
    f.write(html)
print(f"\nFull HTML saved to efhub_{player_id}.html")

