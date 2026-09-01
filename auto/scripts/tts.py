"""
Step 1: Text -> Speech using Edge-TTS (Microsoft, free, no API key required).
Produces an MP3 file from a script string.
"""
import asyncio
import time
import edge_tts
import config


async def _generate(text: str, out_path: str):
    communicate = edge_tts.Communicate(
        text,
        config.TTS_VOICE,
        rate=config.TTS_RATE,
        pitch=config.TTS_PITCH,
    )
    await communicate.save(out_path)


def text_to_speech(text: str, out_path: str, retries: int = 3):
    """
    Synchronous wrapper. Saves narration audio to out_path (mp3).
    Retries a few times since Microsoft's endpoint occasionally returns
    a transient 403/connection error (their server-side token rotates).
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            asyncio.run(_generate(text, out_path))
            print(f"[tts] Saved narration -> {out_path}")
            return out_path
        except Exception as e:
            last_err = e
            print(f"[tts] Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(2 * attempt)  # backoff: 2s, 4s, ...

    raise RuntimeError(
        "Edge-TTS failed after multiple retries. This is usually a temporary "
        "block from Microsoft's servers, not a problem with your script.\n"
        "Things to try:\n"
        "  1. pip install --upgrade edge-tts   (old versions hit 403s constantly)\n"
        "  2. Wait a few minutes and re-run -- this endpoint has frequent transient outages\n"
        "  3. If it persists, switch to the offline fallback: see tts_piper.py"
    ) from last_err


if __name__ == "__main__":
    # quick manual test
    sample = "This is a test of the free text to speech engine. It runs entirely without an API key."
    text_to_speech(sample, f"{config.OUTPUT_DIR}/test_narration.mp3")
