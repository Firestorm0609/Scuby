"""
Central config — all values come from .env (or environment variables).
Copy .env.example to .env and fill in your values before running.
"""
import os
from pathlib import Path

# ── load .env file if present (no external dependency needed) ─────────────────
_ENV_PATH = Path(__file__).parent / ".env"
if _ENV_PATH.exists():
    with open(_ENV_PATH) as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _key, _, _val = _line.partition("=")
            # Don't overwrite values already set in the real environment
            os.environ.setdefault(_key.strip(), _val.strip())


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)

def _get_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default


# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN: str       = _get("TELEGRAM_TOKEN")
TELEGRAM_ALLOWED_IDS: str = _get("TELEGRAM_ALLOWED_IDS")   # raw CSV string

# ── API keys ──────────────────────────────────────────────────────────────────
PEXELS_API_KEY: str = _get("PEXELS_API_KEY")

# ── TTS ───────────────────────────────────────────────────────────────────────
TTS_VOICE: str  = _get("TTS_VOICE",  "en-US-GuyNeural")
TTS_RATE: str   = _get("TTS_RATE",   "+0%")
TTS_PITCH: str  = _get("TTS_PITCH",  "+0Hz")

# ── Whisper ───────────────────────────────────────────────────────────────────
WHISPER_MODEL: str  = _get("WHISPER_MODEL",  "base")
WHISPER_DEVICE: str = _get("WHISPER_DEVICE", "cpu")

# ── Video ─────────────────────────────────────────────────────────────────────
VIDEO_WIDTH:  int = _get_int("VIDEO_WIDTH",  1080)
VIDEO_HEIGHT: int = _get_int("VIDEO_HEIGHT", 1920)
FPS:          int = _get_int("FPS",          30)

# ── Background ────────────────────────────────────────────────────────────────
# 'cartoon' generates real AI cartoon illustrations — free, no API key needed.
# 'animated' generates moving colour gradients locally — no API key required.
# Other options: image, stock_video (both need PEXELS_API_KEY), color
BACKGROUND_MODE:    str = _get("BACKGROUND_MODE",    "cartoon")
BACKGROUND_COLOR:   str = _get("BACKGROUND_COLOR",   "#0d0d0d")
STOCK_QUERY:        str = _get("STOCK_QUERY",         "dark moody forest")

# Visual style for animated mode.
# Options: dark_wave | neon | plasma | aurora | minimal | sunset | ocean
ANIMATED_BG_STYLE:  str = _get("ANIMATED_BG_STYLE",  "dark_wave")

# Visual style for cartoon mode.
# Options: flat_vector | storybook | anime | retro_toon | doodle
CARTOON_STYLE: str = _get("CARTOON_STYLE", "flat_vector")
# Optional fixed seed so every scene in a video shares a consistent "world"
# look. Leave blank for a random seed each run.
CARTOON_SEED = _get_int("CARTOON_SEED", None) if _get("CARTOON_SEED") else None

# ── Captions ──────────────────────────────────────────────────────────────────
FONT_SIZE:         int = _get_int("FONT_SIZE", 64)
CAPTION_COLOR:     str = _get("CAPTION_COLOR",     "white")
HIGHLIGHT_COLOR:   str = _get("HIGHLIGHT_COLOR",   "yellow")
CAPTION_MAX_WORDS: int = _get_int("CAPTION_MAX_WORDS", 3)

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE_DIR = Path(__file__).parent

FONT_PATH:   str = str(_BASE_DIR / "assets" / "font.ttf")
OUTPUT_DIR:  str = str(_BASE_DIR / "output")
ASSETS_DIR:  str = str(_BASE_DIR / "assets")

# ── Segmentation ──────────────────────────────────────────────────────────────
SENTENCES_PER_CHUNK: int = _get_int("SENTENCES_PER_CHUNK", 2)
ANTHROPIC_API_KEY:   str = _get("ANTHROPIC_API_KEY")

# expose to os.environ so segmenter.py can pick it up via os.environ.get()
if ANTHROPIC_API_KEY:
    os.environ.setdefault("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)
