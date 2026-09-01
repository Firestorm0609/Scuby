"""
Step 4: Assemble final video with ffmpeg.

Segmented mode: one Pexels image per script chunk, each rendered as a clip
timed exactly to the audio, with:
  - Ken Burns zoom
  - Animated word-pop overlays (big bold words flying in, colour flashes)
  - Synced SRT captions burned in
Clips are concatenated into the final MP4.
"""
import subprocess
import json
import os
import re
import tempfile
import config


# ── helpers ───────────────────────────────────────────────────────────────────

def get_audio_duration(audio_path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "csv=p=0", audio_path,
    ]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return float(out.stdout.strip())


def build_srt(words: list, max_words: int, out_srt_path: str) -> list:
    """Groups word timings into N-word caption chunks, writes .srt."""
    groups = []
    for i in range(0, len(words), max_words):
        chunk = words[i:i + max_words]
        groups.append({
            "text": " ".join(w["word"] for w in chunk),
            "start": chunk[0]["start"],
            "end":   chunk[-1]["end"],
        })

    def _t(t):
        h = int(t // 3600); m = int((t % 3600) // 60); s = t % 60
        return f"{h:02}:{m:02}:{s:06.3f}".replace(".", ",")

    with open(out_srt_path, "w") as f:
        for idx, g in enumerate(groups, 1):
            f.write(f"{idx}\n{_t(g['start'])} --> {_t(g['end'])}\n{g['text']}\n\n")

    print(f"[assemble] Wrote SRT -> {out_srt_path}")
    return groups


def _srt_for_clip(all_words: list, max_words: int,
                  t_start: float, t_end: float,
                  out_path: str) -> str:
    """
    Write a self-contained SRT for one clip.
    Timestamps are RELATIVE to t_start so they are correct inside the clip.
    """
    clip_words = [w for w in all_words if w["end"] > t_start and w["start"] < t_end]

    groups = []
    for i in range(0, len(clip_words), max_words):
        chunk = clip_words[i:i + max_words]
        groups.append({
            "text":  " ".join(w["word"] for w in chunk),
            "start": max(0.0, chunk[0]["start"]  - t_start),
            "end":   max(0.0, chunk[-1]["end"]   - t_start),
        })

    def _t(t):
        h = int(t // 3600); m = int((t % 3600) // 60); s = t % 60
        return f"{h:02}:{m:02}:{s:06.3f}".replace(".", ",")

    with open(out_path, "w") as f:
        for idx, g in enumerate(groups, 1):
            f.write(f"{idx}\n{_t(g['start'])} --> {_t(g['end'])}\n{g['text']}\n\n")

    return out_path


def _caption_style() -> str:
    return (
        "FontName=DejaVu Sans Bold,FontSize=22,"
        "PrimaryColour=&H00ffffff&,OutlineColour=&H00000000&,"
        "BorderStyle=1,Outline=3,Shadow=1,Alignment=2,MarginV=160"
    )


# ── segment boundary detection ────────────────────────────────────────────────

def _segment_boundaries(words: list, chunks: list) -> list:
    """
    Align each text chunk to the word timings using fuzzy token matching.
    """
    def _tokens(text: str) -> list:
        return [re.sub(r"[^a-z0-9]", "", w.lower())
                for w in text.split() if re.sub(r"[^a-z0-9]", "", w.lower())]

    spoken_tokens = [re.sub(r"[^a-z0-9]", "", w["word"].lower()) for w in words]

    boundaries = []
    search_from = 0

    for chunk in chunks:
        chunk_tokens = _tokens(chunk)
        if not chunk_tokens:
            prev_end = boundaries[-1][1] if boundaries else 0.0
            boundaries.append((prev_end, prev_end))
            continue

        first = chunk_tokens[0]
        best_idx = search_from
        for j in range(search_from, len(spoken_tokens)):
            if spoken_tokens[j] == first:
                best_idx = j
                break

        last = chunk_tokens[-1]
        end_idx = min(best_idx + len(chunk_tokens) + 5, len(spoken_tokens) - 1)
        for j in range(end_idx, best_idx - 1, -1):
            if spoken_tokens[j] == last:
                end_idx = j
                break

        t_start = words[best_idx]["start"]
        t_end   = words[end_idx]["end"]
        boundaries.append((t_start, t_end))
        search_from = end_idx + 1

    if boundaries:
        boundaries[-1] = (boundaries[-1][0], words[-1]["end"])

    return boundaries


# ── animated word-pop overlay ─────────────────────────────────────────────────

_HIGHLIGHT_COLOURS = [
    "yellow",
    "orange",
    "lime",
    "magenta",
    "gold",
]


def _build_drawtext_filters(words: list, t_start: float, t_end: float) -> list:
    """
    For each word in [t_start, t_end), produce a drawtext filter that pops
    the word in bold at the upper-centre of the frame with a cycling colour.
    Timestamps are relative to t_start for this clip.
    """
    clip_words = [w for w in words if w["end"] > t_start and w["start"] < t_end]
    filters = []
    h = config.VIDEO_HEIGHT

    for i, wd in enumerate(clip_words):
        text = wd["word"].strip().upper()
        if not text:
            continue

        # Escape characters that are special inside ffmpeg filter strings.
        # Order matters: backslash first, then the others.
        text = text.replace("\\", "\\\\")
        text = text.replace("'",  "\u2019")   # replace straight quote with curly to avoid escaping hell
        text = text.replace(":",  "\\:")
        text = text.replace(",",  "\\,")
        text = text.replace("[",  "\\[")
        text = text.replace("]",  "\\]")
        text = text.replace(";",  "\\;")

        rel_start = max(0.0, wd["start"] - t_start)
        rel_end   = max(rel_start + 0.05, wd["end"] - t_start)
        colour    = _HIGHLIGHT_COLOURS[i % len(_HIGHLIGHT_COLOURS)]
        font_size = max(52, 96 - len(text) * 3)
        y_pos     = int(h * 0.28) + (i % 3) * 10

        filters.append(
            f"drawtext=text='{text}'"
            f":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
            f":fontsize={font_size}"
            f":fontcolor={colour}"
            f":borderw=4:bordercolor=black"
            f":x=(w-text_w)/2"
            f":y={y_pos}"
            f":enable='between(t,{rel_start:.3f},{rel_end:.3f})'"
            f":alpha='if(lt(t,{rel_start:.3f}+0.05),((t-{rel_start:.3f})/0.05)"
            f",if(lt(t,{rel_end:.3f}-0.08),1,({rel_end:.3f}-t)/0.08))'"
        )

    return filters


# ── clip renderer ─────────────────────────────────────────────────────────────

def _render_clip(bg_path: str, audio_path: str,
                 all_words: list,
                 t_start: float, t_end: float,
                 clip_idx: int, tmp_dir: str,
                 out_path: str) -> str:

    duration = max(0.1, t_end - t_start)
    w, h, fps = config.VIDEO_WIDTH, config.VIDEO_HEIGHT, config.FPS
    total_frames = max(1, int(duration * fps))

    # ── detect background type ────────────────────────────────────────────────
    is_video_bg = bg_path.lower().endswith(".mp4")

    # ── build filter chain ────────────────────────────────────────────────────
    pop_filters = _build_drawtext_filters(all_words, t_start, t_end)

    if is_video_bg:
        # Animated bg already has motion — skip Ken Burns, keep word pops only.
        vf = ",".join(pop_filters) if pop_filters else "null"
        bg_input_args = ["-stream_loop", "-1", "-i", bg_path]
    else:
        # Still image — apply Ken Burns zoom + word pops.
        if clip_idx % 2 == 0:
            x_expr, y_expr = "0", "0"
        else:
            x_expr, y_expr = "iw/2*(1-1/zoom)", "ih/2*(1-1/zoom)"
        zoompan = (
            f"scale={w*2}:{h*2},"
            f"zoompan=z='min(zoom+0.0008,1.12)':x='{x_expr}':y='{y_expr}'"
            f":d={total_frames}:s={w}x{h}:fps={fps}"
        )
        vf = ",".join([zoompan] + pop_filters)
        bg_input_args = ["-loop", "1", "-i", bg_path]

    # Write filter chain to a temp file to avoid ARG_MAX shell limits.
    # Dozens of drawtext entries can easily exceed 128 KB on the command line.
    vf_script = os.path.join(tmp_dir, f"_clip_{clip_idx:03d}.vf")
    with open(vf_script, "w") as f:
        f.write(vf)

    cmd = [
        "ffmpeg", "-y",
        *bg_input_args,
        "-ss", str(t_start), "-i", audio_path,
        "-filter_script:v", vf_script,
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-t", str(duration),
        out_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    # clean up temp filter script
    try:
        os.remove(vf_script)
    except OSError:
        pass

    if result.returncode != 0:
        print(f"[assemble] ffmpeg stderr (clip {clip_idx}):\n{result.stderr[-2000:]}")
        raise subprocess.CalledProcessError(result.returncode, cmd)

    return out_path


# ── concat ────────────────────────────────────────────────────────────────────

def _concat_clips(clip_paths: list, out_path: str):
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        list_path = f.name
        for p in clip_paths:
            f.write(f"file '{os.path.abspath(p)}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_path,
        "-c", "copy",
        out_path,
    ]
    subprocess.run(cmd, check=True)
    os.unlink(list_path)
    print(f"[assemble] Concatenated {len(clip_paths)} clips -> {out_path}")


# ── single-image fallback ─────────────────────────────────────────────────────

def _render_single(bg_path: str, audio_path: str, srt_path: str,
                   duration: float, out_path: str):
    w, h, fps = config.VIDEO_WIDTH, config.VIDEO_HEIGHT, config.FPS
    is_video_bg = bg_path.lower().endswith(".mp4")

    if is_video_bg:
        # Animated bg — just burn in subtitles, no Ken Burns needed
        vf = f"subtitles={srt_path}:force_style='{_caption_style()}'"
        bg_input_args = ["-stream_loop", "-1", "-i", bg_path]
    else:
        total_frames = int(duration * fps)
        zoompan = (
            f"scale={w*2}:{h*2},"
            f"zoompan=z='min(zoom+0.0007,1.15)':d={total_frames}:s={w}x{h}:fps={fps}"
        )
        vf = f"{zoompan},subtitles={srt_path}:force_style='{_caption_style()}'"
        bg_input_args = ["-loop", "1", "-i", bg_path]

    cmd = [
        "ffmpeg", "-y",
        *bg_input_args,
        "-i", audio_path,
        "-vf", vf,
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-t", str(duration), "-shortest",
        out_path,
    ]
    subprocess.run(cmd, check=True)


# ── public API ────────────────────────────────────────────────────────────────

def render_video(
    background_image_path: str,
    audio_path: str,
    words_json_path: str,
    out_path: str,
    segment_images: list = None,
    segment_chunks: list = None,
):
    duration = get_audio_duration(audio_path)

    with open(words_json_path) as f:
        words = json.load(f)

    srt_path = out_path.replace(".mp4", ".srt")
    build_srt(words, config.CAPTION_MAX_WORDS, srt_path)

    # ── segmented mode ────────────────────────────────────────────────────────
    if segment_images and segment_chunks and len(segment_images) >= 1:
        boundaries = _segment_boundaries(words, segment_chunks)
        clip_paths = []
        tmp_dir = os.path.dirname(out_path)

        print(f"[assemble] Rendering {len(segment_images)} segment clips...")
        for i, (bg, chunk, (t0, t1)) in enumerate(
            zip(segment_images, segment_chunks, boundaries)
        ):
            clip_out = os.path.join(tmp_dir, f"_clip_{i:03d}.mp4")
            print(f"  clip {i+1}/{len(segment_images)}  "
                  f"[{t0:.2f}s – {t1:.2f}s]  bg={os.path.basename(bg)}")
            _render_clip(bg, audio_path, words, t0, t1, i, tmp_dir, clip_out)
            clip_paths.append(clip_out)

        _concat_clips(clip_paths, out_path)

        # clean up temp clips and SRTs
        for p in clip_paths:
            try: os.remove(p)
            except OSError: pass

    # ── single-image fallback ─────────────────────────────────────────────────
    else:
        print("[assemble] Rendering single-background video...")
        _render_single(background_image_path, audio_path, srt_path, duration, out_path)

    print(f"[assemble] Done -> {out_path}")
    return out_path
