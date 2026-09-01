"""
Step 2: Audio -> word-level timestamps using faster-whisper (free, runs on CPU).
This gives us exact timing for each word so captions can sync perfectly
to the narration, including karaoke-style word highlighting.
"""
from faster_whisper import WhisperModel
import json
import config


def transcribe_with_timestamps(audio_path: str, out_json_path: str = None):
    """
    Returns a list of word dicts: [{"word": "Hello", "start": 0.12, "end": 0.45}, ...]
    Also writes them to out_json_path if given, so this step can be cached/reused.
    """
    model = WhisperModel(
        config.WHISPER_MODEL,
        device=config.WHISPER_DEVICE,
        compute_type="int8",  # int8 = much lower RAM use, fine on 4GB VPS
    )

    segments, _info = model.transcribe(audio_path, word_timestamps=True)

    words = []
    for seg in segments:
        for w in seg.words:
            words.append({
                "word": w.word.strip(),
                "start": round(w.start, 3),
                "end": round(w.end, 3),
            })

    if out_json_path:
        with open(out_json_path, "w") as f:
            json.dump(words, f, indent=2)
        print(f"[captions] Saved word timings -> {out_json_path}")

    return words


if __name__ == "__main__":
    words = transcribe_with_timestamps(
        f"{config.OUTPUT_DIR}/test_narration.mp3",
        f"{config.OUTPUT_DIR}/test_words.json",
    )
    print(f"Got {len(words)} words")
