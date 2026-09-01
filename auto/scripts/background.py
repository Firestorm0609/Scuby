"""
Step 3 – Animated background generator (free, fully local, no API needed).

Replaces the Pexels stock-photo approach with smooth animated gradients
generated entirely by ffmpeg's geq (general equation) filter.

How it works
────────────
• Each segment clip gets its own short looping MP4 (~8 s).
• The clip is generated at 360×640 internally (for speed), then bicubic-
  upscaled to the configured output resolution (default 1080×1920).
• assemble.py streams each clip in a loop with -stream_loop -1 so it covers
  however long the spoken segment lasts.
• A phase offset (clip_idx × π/3) shifts the colour per clip so they look
  distinct even when the same style is used throughout.

Set in .env
───────────
BACKGROUND_MODE   animated          ← new default
ANIMATED_BG_STYLE dark_wave         ← see STYLES below

Available styles
────────────────
  dark_wave   slow dark blue/purple wave  (good for most topics)
  neon        bright purple/cyan          (tech, gaming)
  plasma      colourful interference      (entertainment, music)
  aurora      dark green/teal             (nature, science)
  minimal     near-black, gentle shift    (clean, podcast-style)
  sunset      warm orange-red gradient    (lifestyle, motivation)
  ocean       deep blue/cyan              (travel, wellness)

Pexels fallback
───────────────
Set BACKGROUND_MODE=image or stock_video (and supply PEXELS_API_KEY)
to restore the original stock-media behaviour.
"""

import subprocess
import os
import config

# ── internal render resolution (upscaled to output by ffmpeg) ─────────────────
# 360×640 = 1/9 the pixels of 1080×1920 → ~9× faster geq computation.
_GEN_W = 360
_GEN_H = 640

# Length of each looping background clip (seconds).
# Segments longer than this are covered by -stream_loop -1 in assemble.py.
_LOOP_SECS = 8

# ── geq colour formulas ───────────────────────────────────────────────────────
# Each entry is (R_expr, G_expr, B_expr).
# {P} is replaced with a per-clip float phase offset (radians).
# Available geq variables: X, Y, W, H (source dims), T (time s), N (frame).
# Keep all channel values safely within 0–255.

_STYLES = {
    # Dark blue/purple slow wave — versatile default
    "dark_wave": (
        "25+15*sin(X/50+T*0.6+{P})",
        "10+8*sin(X/67+T*0.5+{P}+1.047)",
        "60+40*sin(Y/40-T*0.4+{P})",
    ),
    # Bright neon purple/cyan — high energy
    "neon": (
        "80+60*sin(X/27+T*1.2+{P})*cos(Y/40+T*0.8)",
        "15+12*sin(X/33-T+{P})",
        "120+80*sin(Y/23+T*1.5+{P})",
    ),
    # Colourful interference pattern — eye-catching
    "plasma": (
        "128+80*sin(X/20+T*1.5+{P})*sin(Y/27+T*0.9)",
        "100+60*sin(X/27-T*0.8+{P})*cos(Y/33+T*0.5)",
        "150+70*sin((X+Y)/17+T*2+{P})",
    ),
    # Dark teal/green aurora
    "aurora": (
        "10+8*sin(X/67+T*0.4+{P})",
        "60+40*sin(Y/33+T*0.6+{P})*cos(X/50+T*0.3)",
        "40+30*sin(X/40-T*0.5+{P}+0.785)",
    ),
    # Near-black with very subtle temporal colour shift
    "minimal": (
        "12+8*sin(T*0.3+{P})",
        "5+4*sin(T*0.25+1.571+{P})",
        "30+20*sin(T*0.2+3.142+{P})",
    ),
    # Warm orange-red gradient, dark top → bright bottom
    "sunset": (
        "100+70*(Y/H)+20*sin(T*0.4+{P})",
        "50+40*(Y/H)+15*sin(T*0.35+{P})",
        "20+10*(Y/H)+5*sin(T*0.3+{P})",
    ),
    # Deep navy top → bright cyan bottom with ripple
    "ocean": (
        "5+5*sin(T*0.4+{P})",
        "30+40*(Y/H)+20*sin(Y/50+T*0.5+{P})",
        "80+80*(1-Y/H)+40*sin(X/40+T*0.7+{P})",
    ),
}


def generate_animated_bg(out_path, style=None, clip_idx=0):
    """
    Render a short animated-gradient MP4 clip.

    Args:
        out_path  : destination path; must end in .mp4
        style     : key from _STYLES; falls back to ANIMATED_BG_STYLE config
        clip_idx  : segment index — shifts colour phase so clips look distinct

    Returns:
        out_path
    """
    style = style or getattr(config, "ANIMATED_BG_STYLE", "dark_wave")
    if style not in _STYLES:
        print(f"[bg] Unknown style '{style}', falling back to dark_wave")
        style = "dark_wave"

    phase = clip_idx * 1.047          # π/3 rad apart → 6 distinct phases
    r_t, g_t, b_t = _STYLES[style]
    r = r_t.replace("{P}", f"{phase:.4f}")
    g = g_t.replace("{P}", f"{phase:.4f}")
    b = b_t.replace("{P}", f"{phase:.4f}")

    ow, oh = config.VIDEO_WIDTH, config.VIDEO_HEIGHT
    fps     = config.FPS
    gw, gh  = _GEN_W, _GEN_H

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    vf = (
        f"geq=r='{r}':g='{g}':b='{b}',"
        f"scale={ow}:{oh}:flags=bicubic"
    )

    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c=black:s={gw}x{gh}:r={fps}",
        "-vf", vf,
        "-t", str(_LOOP_SECS),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-preset", "fast",
        out_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"[bg] ffmpeg animated bg failed:\n{result.stderr[-800:]}"
        )

    print(f"[bg] Generated '{style}' animated bg (clip {clip_idx}) → {out_path}")
    return out_path


# ── solid-colour fallback (no ffmpeg encoding quirks possible) ────────────────

def make_solid_background(out_path):
    """Fallback: one-frame solid-colour JPEG via ffmpeg."""
    jpg = out_path.rsplit(".", 1)[0] + ".jpg"
    cmd = [
        "ffmpeg", "-y", "-f", "lavfi",
        "-i", (
            f"color=c={config.BACKGROUND_COLOR}"
            f":s={config.VIDEO_WIDTH}x{config.VIDEO_HEIGHT}:d=1"
        ),
        "-frames:v", "1", jpg,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    print(f"[bg] Solid colour fallback → {jpg}")
    return jpg


# ── Pexels helpers (kept for BACKGROUND_MODE=image / stock_video) ─────────────

def _fetch_pexels_image(query, out_path):
    import requests
    headers = {"Authorization": config.PEXELS_API_KEY}
    url = (
        f"https://api.pexels.com/v1/search"
        f"?query={query}&per_page=5&orientation=portrait"
    )
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    if not data.get("photos"):
        raise ValueError(f"No Pexels photos for: {query!r}")
    img_data = requests.get(
        data["photos"][0]["src"]["portrait"], timeout=30
    ).content
    with open(out_path, "wb") as f:
        f.write(img_data)
    print(f"[bg] Pexels image ({query!r}) → {out_path}")
    return out_path


def _fetch_pexels_video(query, out_path):
    import requests
    headers = {"Authorization": config.PEXELS_API_KEY}
    url = (
        f"https://api.pexels.com/videos/search"
        f"?query={query}&per_page=1&orientation=portrait"
    )
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    if not data.get("videos"):
        raise ValueError(f"No Pexels videos for: {query!r}")
    files = sorted(
        data["videos"][0]["video_files"], key=lambda f: f.get("width", 0)
    )
    chosen = next((f for f in files if f["width"] >= 720), files[-1])
    video_data = requests.get(chosen["link"], timeout=60).content
    with open(out_path, "wb") as f:
        f.write(video_data)
    print(f"[bg] Pexels video ({query!r}) → {out_path}")
    return out_path


# ── public router ──────────────────────────────────────────────────────────────

def get_background(out_path, query=None, clip_idx=0):
    """
    Return the path of a ready background file (image or video).

    Routing (BACKGROUND_MODE in .env):
      cartoon     → real AI-generated cartoon illustration (free, no API key)
      animated    → generate_animated_bg()            [default]
      image       → Pexels photo  (needs PEXELS_API_KEY)
      stock_video → Pexels video  (needs PEXELS_API_KEY)
      color       → make_solid_background()

    Falls back to animated when BACKGROUND_MODE=image but no key is set.
    The returned path may differ in extension from out_path.
    """
    mode = getattr(config, "BACKGROUND_MODE", "animated")

    # ── cartoon (real AI cartoon art, free) ──────────────────────────────────
    if mode == "cartoon":
        from scripts.cartoon_bg import get_cartoon_background
        return get_cartoon_background(out_path, query=query, clip_idx=clip_idx)

    # ── animated (default) ───────────────────────────────────────────────────
    if mode == "animated":
        mp4 = out_path.rsplit(".", 1)[0] + ".mp4"
        try:
            return generate_animated_bg(mp4, clip_idx=clip_idx)
        except Exception as e:
            print(f"[bg] Animated bg error: {e}  →  solid colour fallback")
            return make_solid_background(mp4)

    # ── solid colour ─────────────────────────────────────────────────────────
    if mode == "color":
        return make_solid_background(out_path)

    # ── Pexels image ─────────────────────────────────────────────────────────
    if mode == "image":
        if not config.PEXELS_API_KEY:
            print("[bg] BACKGROUND_MODE=image but no PEXELS_API_KEY — using animated.")
            mp4 = out_path.rsplit(".", 1)[0] + ".mp4"
            return generate_animated_bg(mp4, clip_idx=clip_idx)
        jpg = out_path.rsplit(".", 1)[0] + ".jpg"
        return _fetch_pexels_image(query or config.STOCK_QUERY, jpg)

    # ── Pexels video ─────────────────────────────────────────────────────────
    if mode == "stock_video":
        if not config.PEXELS_API_KEY:
            print("[bg] BACKGROUND_MODE=stock_video but no PEXELS_API_KEY — using animated.")
            mp4 = out_path.rsplit(".", 1)[0] + ".mp4"
            return generate_animated_bg(mp4, clip_idx=clip_idx)
        mp4 = out_path.rsplit(".", 1)[0] + ".mp4"
        return _fetch_pexels_video(query or config.STOCK_QUERY, mp4)

    raise ValueError(f"Unknown BACKGROUND_MODE: {mode!r}")
