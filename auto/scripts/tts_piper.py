"""
Offline fallback TTS using Piper (https://github.com/rhasspy/piper).
Use this if Edge-TTS keeps getting blocked (403 errors) -- Piper runs
100% locally with no network call, so it can never be rate-limited or blocked.

Voice quality is a notch below Edge-TTS but still very usable for narration.

ONE-TIME SETUP on your VPS:
    pip install piper-tts --break-system-packages
    mkdir -p ~/auto/assets/piper_voices
    cd ~/auto/assets/piper_voices
    # Download a free voice model (example: a clear US English male voice)
    wget https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/joe/medium/en_US-joe-medium.onnx
    wget https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/joe/medium/en_US-joe-medium.onnx.json
    # Browse more voices: https://rhasspy.github.io/piper-samples/

USAGE: swap the import in main.py from `scripts.tts` to `scripts.tts_piper`,
or call text_to_speech_piper() directly.
"""
import subprocess
import os

PIPER_VOICE_MODEL = "/root/auto/assets/piper_voices/en_US-joe-medium.onnx"


def text_to_speech_piper(text: str, out_path: str, voice_model: str = None):
    voice_model = voice_model or PIPER_VOICE_MODEL

    if not os.path.exists(voice_model):
        raise FileNotFoundError(
            f"Piper voice model not found at {voice_model}.\n"
            "Download one first -- see setup instructions in this file's docstring."
        )

    wav_path = out_path.replace(".mp3", ".wav")

    # piper writes wav; pipe text in via stdin
    cmd = ["piper", "--model", voice_model, "--output_file", wav_path]
    subprocess.run(cmd, input=text, text=True, check=True, capture_output=True)

    # convert to mp3 to match the rest of the pipeline (ffmpeg, always available)
    subprocess.run(
        ["ffmpeg", "-y", "-i", wav_path, out_path],
        check=True, capture_output=True,
    )
    os.remove(wav_path)

    print(f"[tts-piper] Saved narration (offline) -> {out_path}")
    return out_path


if __name__ == "__main__":
    text_to_speech_piper(
        "This is a test of the fully offline backup voice engine.",
        "/root/auto/output/test_piper.mp3",
    )
