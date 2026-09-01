"""
Full pipeline: script text -> finished narration video.

Usage:
    python3 main.py "your_script.txt" "output_name"

Or import run_pipeline() directly and pass a string.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import config
from scripts.tts import text_to_speech
from scripts.captions import transcribe_with_timestamps
from scripts.background import get_background
from scripts.assemble import render_video
from scripts.segmenter import chunk_script, extract_keywords


def run_pipeline(script_text: str, job_name: str = "video"):
    out_dir = config.OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    audio_path = f"{out_dir}/{job_name}_narration.mp3"
    words_path = f"{out_dir}/{job_name}_words.json"
    final_path = f"{out_dir}/{job_name}_final.mp4"

    # ── Step 1: TTS ───────────────────────────────────────────────────────────
    print("=== Step 1/4: Text to speech ===")
    try:
        text_to_speech(script_text, audio_path)
    except Exception as e:
        print(f"[main] Edge-TTS unavailable ({e}).\n[main] Falling back to offline Piper TTS...")
        from scripts.tts_piper import text_to_speech_piper
        text_to_speech_piper(script_text, audio_path)

    # ── Step 2: Captions ──────────────────────────────────────────────────────
    print("=== Step 2/4: Generating word-synced captions ===")
    transcribe_with_timestamps(audio_path, words_path)

    # ── Step 3: Backgrounds (one per chunk) ───────────────────────────────────
    print("=== Step 3/4: Generating per-segment backgrounds ===")
    chunks = chunk_script(script_text)
    keywords = extract_keywords(chunks)

    segment_images = []
    for i, (chunk, keyword) in enumerate(zip(chunks, keywords)):
        # Use .mp4 hint; get_background may return .jpg for image/color modes
        bg_hint = f"{out_dir}/{job_name}_bg_{i:03d}.mp4"
        try:
            actual_bg = get_background(bg_hint, query=keyword, clip_idx=i)
        except Exception as e:
            print(f"[main] Background failed for chunk {i} ({keyword!r}): {e} — solid colour fallback")
            from scripts.background import make_solid_background
            actual_bg = make_solid_background(bg_hint.replace(".mp4", ".jpg"))
        segment_images.append(actual_bg)

    # ── Step 4: Render ────────────────────────────────────────────────────────
    print("=== Step 4/4: Rendering final video ===")
    render_video(
        background_image_path=segment_images[0],
        audio_path=audio_path,
        words_json_path=words_path,
        out_path=final_path,
        segment_images=segment_images,
        segment_chunks=chunks,
    )

    print(f"\nDONE. Final video: {final_path}")
    return final_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 main.py <script.txt OR raw text in quotes> [job_name]")
        sys.exit(1)

    arg = sys.argv[1]
    job_name = sys.argv[2] if len(sys.argv) > 2 else "video"

    if os.path.isfile(arg):
        with open(arg) as f:
            script_text = f.read()
    else:
        script_text = arg

    run_pipeline(script_text, job_name)
