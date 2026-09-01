"""
Step 3 (cartoon mode) – Real cartoon-style scene generator.

Replaces moving-colour-gradient backgrounds with actual AI-generated
cartoon illustrations, one per script segment. Free, no API key required.

Engine: Pollinations.ai (https://pollinations.ai)
  - Open-source, free image generation over a plain HTTP GET request.
  - No signup / API key needed for normal use.
  - Backed by the Flux model by default.

How it works
────────────
• Each segment's keyword/topic (already extracted by segmenter.py) is
  wrapped in a cartoon-style prompt template and sent to Pollinations.
• A FIXED per-job seed is reused across all segments of the same video,
  combined with a shared style prefix, so every scene in one video looks
  like it belongs to the same cartoon "world" (consistent palette/line
  style) even though each picture is a different illustration.
• The image is downloaded as a still frame. assemble.py already applies
  Ken Burns zoom/pan + word-pop overlays to still-image backgrounds, so
  no changes are needed there — it gets real cartoon art instead of a
  plain photo or a colour gradient.
• On any failure (network, bad response, timeout) this module falls back
  to the existing animated gradient generator, so the pipeline never
  hard-fails on a single bad request.

Set in .env
───────────
BACKGROUND_MODE     cartoon
CARTOON_STYLE       flat_vector        ← see STYLES below
CARTOON_SEED        (optional int)     ← fixes the recurring "world" look

Available styles
─────────────────
  flat_vector   clean flat-color vector cartoon, bold outlines (default)
  storybook     soft painterly children's-book illustration
  anime         anime / manga-influenced character art
  retro_toon    classic 90s Saturday-morning cartoon look
  doodle        simple whiteboard / hand-drawn doodle style
"""

import os
import time
import random
import urllib.parse
import requests
import config

# ── Pollinations endpoint ──────────────────────────────────────────────────────
_BASE_URL = "https://image.pollinations.ai/prompt/{prompt}"
_TIMEOUT = 45
_RETRIES = 2

# ── style prefixes (prepended to every prompt for visual consistency) ────────
_STYLES = {
    "flat_vector": (
        "flat vector cartoon illustration, bold clean outlines, "
        "simple shapes, bright flat colours, no gradients, "
        "modern children's animation style"
    ),
    "storybook": (
        "soft painterly storybook illustration, warm colours, "
        "gentle lighting, children's picture-book art style"
    ),
    "anime": (
        "anime style illustration, clean cel-shaded colours, "
        "expressive character art, manga influenced"
    ),
    "retro_toon": (
        "classic 1990s Saturday morning cartoon style, "
        "thick black outlines, bright primary colours, retro animation cel"
    ),
    "doodle": (
        "simple hand-drawn doodle illustration, whiteboard marker style, "
        "playful loose line art, minimal colour"
    ),
}

_NEGATIVE_SUFFIX = (
    "no text, no watermark, no logo, no signature, "
    "not photorealistic, not a photo"
)


def _build_prompt(topic: str, style: str) -> str:
    style_desc = _STYLES.get(style, _STYLES["flat_vector"])
    topic = (topic or "an interesting scene").strip()
    return f"{style_desc}, depicting {topic}, {_NEGATIVE_SUFFIX}"


def generate_cartoon_bg(out_path: str, topic: str = None,
                         style: str = None, seed: int = None,
                         clip_idx: int = 0) -> str:
    """
    Download one AI-generated cartoon illustration as the background image.

    Args:
        out_path : destination path; extension will be forced to .jpg
        topic    : short text describing this segment's content
                   (pass the keyword already extracted by segmenter.py)
        style    : key from _STYLES; falls back to CARTOON_STYLE config
        seed     : int seed; same seed + similar prompt = same art "world".
                   Falls back to CARTOON_SEED config, or a random per-run seed.
        clip_idx : segment index — nudges the seed slightly per clip so
                   scenes differ while staying stylistically related.

    Returns:
        out_path (always .jpg)
    """
    style = style or getattr(config, "CARTOON_STYLE", "flat_vector")
    if style not in _STYLES:
        print(f"[cartoon_bg] Unknown style '{style}', falling back to flat_vector")
        style = "flat_vector"

    base_seed = seed if seed is not None else getattr(config, "CARTOON_SEED", None)
    if base_seed is None:
        base_seed = random.randint(1, 999_999)
    # Vary slightly per clip so scenes aren't identical, but stay related
    # (small offset keeps Flux's composition "family" similar across a video).
    this_seed = int(base_seed) + clip_idx

    prompt = _build_prompt(topic, style)
    encoded_prompt = urllib.parse.quote(prompt)

    w, h = config.VIDEO_WIDTH, config.VIDEO_HEIGHT
    url = _BASE_URL.format(prompt=encoded_prompt)
    params = {
        "width": w,
        "height": h,
        "seed": this_seed,
        "nologo": "true",
        "model": "flux",
    }

    jpg_path = out_path.rsplit(".", 1)[0] + ".jpg"
    os.makedirs(os.path.dirname(os.path.abspath(jpg_path)), exist_ok=True)

    last_err = None
    for attempt in range(1, _RETRIES + 2):
        try:
            resp = requests.get(url, params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "image" not in content_type or len(resp.content) < 1000:
                raise ValueError(
                    f"Unexpected response (content-type={content_type!r}, "
                    f"size={len(resp.content)}B)"
                )
            with open(jpg_path, "wb") as f:
                f.write(resp.content)
            print(f"[cartoon_bg] '{style}' cartoon (clip {clip_idx}, seed {this_seed}) "
                  f"-> {jpg_path}  topic={topic!r}")
            return jpg_path
        except Exception as e:
            last_err = e
            print(f"[cartoon_bg] attempt {attempt} failed: {e}")
            if attempt <= _RETRIES:
                time.sleep(2 * attempt)

    raise RuntimeError(f"[cartoon_bg] All attempts failed: {last_err}")


def get_cartoon_background(out_path: str, query: str = None, clip_idx: int = 0) -> str:
    """
    Public entry point — mirrors background.get_background()'s signature
    so it drops straight into main.py's existing call site.

    Falls back to the animated gradient generator (background.py) on
    any failure, so a flaky network call never kills the whole pipeline.
    """
    try:
        return generate_cartoon_bg(out_path, topic=query, clip_idx=clip_idx)
    except Exception as e:
        print(f"[cartoon_bg] Falling back to animated gradient bg: {e}")
        from scripts.background import generate_animated_bg
        mp4 = out_path.rsplit(".", 1)[0] + ".mp4"
        return generate_animated_bg(mp4, clip_idx=clip_idx)
