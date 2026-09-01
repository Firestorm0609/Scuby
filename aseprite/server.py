#!/usr/bin/env python3
"""
Pixel Art Generator Server
Run on your VPS: python3 server.py
Listens on port 5050 for requests from Aseprite.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import urllib.request
import re
import os

# ── Load .env ────────────────────────────────────────────────────────────────
def load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

load_env()

# ── CONFIG ───────────────────────────────────────────────────────────────────
PORT            = 5050
GEMINI_KEY      = os.environ.get("GEMINI_KEY", "")
GEMINI_URL      = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent?key=" + GEMINI_KEY
)
DEFAULT_SIZE    = 16
MAX_SIZE        = 64
MAX_COLORS      = 16   # we allow up to 16 palette colors (0–9 + a–f)
MAX_RETRIES     = 2    # extra attempts if Gemini returns a malformed grid
# ─────────────────────────────────────────────────────────────────────────────

# NOTE ON "ITEMS": there is intentionally no hardcoded list of drawable
# objects anywhere in this file. The `object` string from the client is
# passed straight into the prompt below ("Draw pixel art of: {object_name}").
# Gemini does the generalization — any text works, no per-item code needed.
# The only thing that scales with complexity is grid SIZE and COLOR count,
# which is what the retry/validation logic below is for.

# Palette keys: 0-9 then a-f for up to 16 colors
PALETTE_KEYS = [str(i) for i in range(10)] + list("abcdef")

def build_system_prompt(size: int, colors: int) -> str:
    key_list = PALETTE_KEYS[:colors]
    example_row = (key_list * (size // colors + 1))[:size]
    example_row_str = "".join(example_row)
    valid_chars = "".join(key_list)

    return f"""You are an expert pixel art designer. Your job is to create clear, recognizable pixel art sprites.

STEP 1 — Before outputting JSON, silently reason (do NOT output this):
  • What does this object look like in real life?
  • What is the best viewing angle? (side-view for furniture/vehicles/characters, front for faces/shields, top-down for maps)
  • What are its key colors?
  • How should it be proportioned to fill a {size}×{size} grid?

STEP 1b — ONLY if the object is a geometric or symbolic silhouette (star,
arrow, heart, lightning bolt, gear, cross, crescent, diamond, etc. — anything
defined by its outline shape rather than realistic rendering), do this
additional reasoning before drawing:
  • List the shape's vertices as approximate (col, row) grid coordinates,
    numbered in order around the perimeter, using a coordinate space of
    0..{size-1} for both col and row. Example for a 5-pointed star on a
    {size}×{size} grid: 5 outer points and 5 inner points alternating
    around the center, 10 vertices total.
  • Check: does the vertex list actually alternate between "points sticking
    out" and "notches cutting in"? A star, arrow, or lightning bolt MUST have
    at least one concave (inward-cutting) vertex somewhere on its outline —
    if every vertex you listed is convex (sticking straight out, like a
    square or diamond rotated 45°), you have drawn the WRONG shape and must
    redo the vertex list before continuing.
  • Only after the vertex list passes that concave/convex check, fill in the
    rows so the outline passes through those vertices.

STEP 2 — Output ONLY a valid JSON object. No markdown, no code fences, no explanation, no comments.

Format:
{{
  "palette": {{
    "0": [r, g, b],
    "1": [r, g, b]
  }},
  "rows": [
    "{example_row_str}",
    "{example_row_str}"
  ]
}}

Strict rules:
- Palette keys are ONLY these characters (in order): {valid_chars}
- You may use up to {colors} palette entries (keys "{key_list[0]}" through "{key_list[-1]}").
- Key "0" is ALWAYS the background and is ALWAYS flat [0, 0, 0] with no
  exceptions. Never use "0" for foreground, and never apply shading,
  gradients, or a second background tone to key "0" — the background must be
  a single flat color so it can be made transparent. Any shading goes on the
  foreground colors only (keys "1" and up).
- Each row string is EXACTLY {size} characters, using only your palette key characters.
- There are EXACTLY {size} rows — count them before you finish. This is critical at larger grid sizes.
- No trailing commas. All keys double-quoted.
- Return ONLY the JSON. Nothing else.

Drawing quality rules:
- Center the subject and make it fill at least 70% of the canvas.
- Use shading on the SUBJECT only: add a darker variant of main colors for
  shadows, lighter for highlights. Never shade the background (see key "0"
  rule above).
- Outlines: use a dark (not pure black) border around the subject to make it pop.
- Avoid single-pixel noise — shapes should have clean, intentional edges.
- For text/symbols, make them bold and legible.
- At {size}×{size}, you have enough pixels to show real detail — use them.
"""

def repair_json(s: str) -> str:
    s = re.sub(r'^```(?:json)?\s*', '', s.strip())
    s = re.sub(r'\s*```$', '', s.strip())
    s = re.sub(r"'([^']*)'", r'"\1"', s)
    s = re.sub(r',\s*([}\]])', r'\1', s)
    s = re.sub(r'//[^\n]*', '', s)
    s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)
    return s.strip()

def extract_json_block(s: str) -> str:
    start = s.find('{')
    if start == -1:
        raise ValueError("No JSON object found in response")
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(s[start:], start):
        if esc:
            esc = False; continue
        if ch == '\\' and in_str:
            esc = True; continue
        if ch == '"':
            in_str = not in_str; continue
        if in_str:
            continue
        if ch == '{': depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return s[start:i+1]
    raise ValueError("Incomplete JSON — response was truncated")

def validate_grid(result: dict, size: int, colors: int) -> list:
    """Check the parsed grid actually matches the requested dimensions/palette.
    Returns a list of problem descriptions (empty list = valid)."""
    problems = []
    rows    = result.get("rows", [])
    palette = result.get("palette", {})
    valid_keys = set(PALETTE_KEYS[:colors])

    if not palette:
        problems.append("palette is empty")

    if len(rows) != size:
        problems.append(f"expected {size} rows, got {len(rows)}")

    for i, row in enumerate(rows):
        row = str(row)
        if len(row) != size:
            problems.append(f"row {i} has length {len(row)}, expected {size}")
        bad_chars = set(row) - valid_keys
        if bad_chars:
            problems.append(f"row {i} has invalid characters: {sorted(bad_chars)}")

    return problems

def call_gemini(object_name: str, size: int, colors: int) -> dict:
    system_prompt = build_system_prompt(size, colors)

    # Output (JSON grid) budget scales with canvas size: 32×32 needs ~4× more
    # tokens than 16×16. This is just the JSON — thinking gets its own slice below.
    json_budget = max(4096, size * size * 6)

    # Thinking budget: the system prompt has an explicit "STEP 1 — silently
    # reason" step (viewing angle, proportions, key colors) that is the main
    # defense against geometrically-wrong output (e.g. a "star" coming out
    # round, or an "arrow" with no point) for ANY object name, not just
    # hardcoded ones. thinkingBudget=0 disables that reasoning entirely.
    #
    # IMPORTANT: Gemini counts thinking tokens against the SAME maxOutputTokens
    # pool as the visible response. If we just turn thinking on without adding
    # tokens, the model can burn the whole budget thinking and truncate the
    # JSON — which looks identical to a parse failure ("Incomplete JSON").
    # So we give thinking its own fixed slice and size maxOutputTokens as
    # thinking_budget + json_budget, capped at Flash's 24576 thinking limit
    # and a sane overall ceiling.
    thinking_budget = 4096          # fixed, predictable latency/cost.
                                     # Bumped from 2048: the prompt now has an
                                     # extra vertex-coordinate reasoning step
                                     # (STEP 1b) for geometric/symbolic shapes
                                     # like stars and arrows, which needs more
                                     # room to work through than the original
                                     # four bullet points did.
    # thinking_budget = -1          # alt: let the model decide (dynamic) —
                                     # better quality on hard/unusual objects,
                                     # but latency and cost become unpredictable
                                     # since Flash can occasionally think for a
                                     # long time. Fine for offline batch use,
                                     # less fine for "user is waiting in a dialog".

    max_output_tokens = (
        min(thinking_budget + json_budget, 32768)
        if thinking_budget > 0
        else min(json_budget, 32768)
    )

    payload = {
        "contents": [{
            "parts": [
                {"text": system_prompt},
                {"text": f"Draw pixel art of: {object_name}"}
            ]
        }],
        "generationConfig": {
            "temperature": 0.5,
            "maxOutputTokens": max_output_tokens,
            "thinkingConfig": {
                "thinkingBudget": thinking_budget
            }
        }
    }

    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        GEMINI_URL,
        data    = data,
        headers = {"Content-Type": "application/json"},
        method  = "POST"
    )

    with urllib.request.urlopen(req, timeout=90) as resp:
        body = json.loads(resp.read().decode("utf-8"))

    if "error" in body:
        raise ValueError(f"Gemini API error: {body['error'].get('message', body['error'])}")

    raw = body["candidates"][0]["content"]["parts"][0]["text"]
    print(f"  Raw ({len(raw)} chars): {raw[:120]!r}...")

    raw = repair_json(raw)
    raw = extract_json_block(raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  JSON still broken after repair: {e}")
        print(f"  Near error: {raw[max(0,e.pos-30):e.pos+30]!r}")
        raise ValueError(f"Could not parse Gemini response: {e}")


def call_gemini_with_retry(object_name: str, size: int, colors: int, max_retries: int = MAX_RETRIES) -> dict:
    """Wraps call_gemini with grid validation + retry. This is what actually
    makes larger grids (32x32, 48x48, 64x64) reliable — a single malformed
    row is far more likely the bigger the grid gets, so we check for it and
    just ask again instead of silently shipping broken pixels."""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            result = call_gemini(object_name, size, colors)
            problems = validate_grid(result, size, colors)
            if problems:
                raise ValueError("Grid validation failed: " + "; ".join(problems))
            return result
        except Exception as exc:
            last_error = exc
            print(f"  attempt {attempt + 1}/{max_retries + 1} failed: {exc}")
    raise last_error


def expand_palette_art(result: dict, size: int) -> list:
    palette = result.get("palette", {})
    rows    = result.get("rows", [])

    pal = {}
    for k, v in palette.items():
        pal[str(k)] = [int(v[0]), int(v[1]), int(v[2])]

    pixels = []
    for row in rows[:size]:
        row = str(row).ljust(size, '0')[:size]
        for ch in row:
            pixels.append(pal.get(ch, [0, 0, 0]))

    while len(pixels) < size * size:
        pixels.append([0, 0, 0])

    return pixels


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def send_json(self, code: int, obj: dict):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        if self.path != "/generate":
            self.send_json(404, {"error": "Not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length).decode("utf-8"))
        name   = body.get("object", "").strip()

        if not name:
            self.send_json(400, {"error": "Missing 'object' field"})
            return

        # Dynamic size + color count from request — any object, any size in range.
        size   = int(body.get("size",   DEFAULT_SIZE))
        colors = int(body.get("colors", 10))
        size   = max(8, min(size, MAX_SIZE))
        colors = max(2, min(colors, MAX_COLORS))

        print(f"  → generating: {name!r} at {size}×{size} with {colors} colors")
        try:
            result = call_gemini_with_retry(name, size, colors)
            pixels = expand_palette_art(result, size)
            print(f"  ✓ done: {len(pixels)} pixels")
            self.send_json(200, {
                "pixels": pixels,
                "width":  size,
                "height": size
            })

        except Exception as exc:
            print(f"  ✗ error: {exc}")
            self.send_json(500, {"error": str(exc)})


if __name__ == "__main__":
    if not GEMINI_KEY:
        print("✗ GEMINI_KEY not set! Create a .env file with:")
        print("    GEMINI_KEY=your_key_here")
        exit(1)

    httpd = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"✓ Pixel Art server running on port {PORT}")
    print(f"  POST http://<your-vps-ip>:{PORT}/generate")
    print('  Body: {"object": "tree", "size": 32, "colors": 12}')
    print("  Ctrl+C to stop.\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
