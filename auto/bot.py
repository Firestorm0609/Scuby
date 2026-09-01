"""
Telegram Bot for the YT Narration Pipeline
==========================================
Commands:
  /start   — welcome + quick help
  /make    — generate a video from a script you type
  /status  — show queue / last job status
  /config  — view current config values
  /setvoice, /setbg, /setquery — tweak config on the fly

Just paste a script after /make and the bot will:
  1. Convert it to speech
  2. Transcribe + sync captions
  3. Fetch a Pexels background
  4. Render the final .mp4 and send it back to you

Setup:
  pip install python-telegram-bot
  export TELEGRAM_TOKEN=your_bot_token
  export TELEGRAM_ALLOWED_IDS=123456789,987654321   # your Telegram user IDs (optional whitelist)
  python3 bot.py
"""

import asyncio
import logging
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

# ── make sure imports work from any cwd ──────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

import config

from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)
from telegram.constants import ParseMode

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("yt_bot")

# ── auth ──────────────────────────────────────────────────────────────────────
TOKEN = config.TELEGRAM_TOKEN
ALLOWED_IDS: set[int] = {
    int(x) for x in config.TELEGRAM_ALLOWED_IDS.split(",") if x.strip().isdigit()
}

# ── conversation states ───────────────────────────────────────────────────────
WAITING_SCRIPT = 1
WAITING_JOB_NAME = 2
WAITING_VOICE = 3
WAITING_BG_MODE = 4
WAITING_BG_QUERY = 5

# ── per-user job state ────────────────────────────────────────────────────────
# { user_id: {"status": str, "started": float, "job_name": str} }
_jobs: dict[int, dict] = {}


# ── helpers ───────────────────────────────────────────────────────────────────

def is_allowed(user_id: int) -> bool:
    return not ALLOWED_IDS or user_id in ALLOWED_IDS


def _escape(text: str) -> str:
    """Minimal MarkdownV2 escaping for dynamic values."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


async def send_typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")


# ── pipeline runner (blocking → run in executor) ──────────────────────────────

def _run_pipeline_sync(script_text: str, job_name: str) -> str:
    """Runs the pipeline synchronously. Called from asyncio.to_thread."""
    from main import run_pipeline
    return run_pipeline(script_text, job_name)


# ── command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        await update.message.reply_text("⛔ Not authorised.")
        return

    text = (
        "👋 *YT Narration Pipeline Bot*\n\n"
        "Send me a script and I'll render a complete vertical video "
        "\\(speech \\+ karaoke captions \\+ background\\)\\.\n\n"
        "*Commands*\n"
        "/make — start a new video job\n"
        "/status — last job status\n"
        "/config — current config\n"
        "/setvoice — change TTS voice\n"
        "/setbg — change background mode\n"
        "/setquery — change Pexels search query\n"
        "/cancel — abort current conversation\n\n"
        "_Tip: you can also just paste your script and I'll pick it up\\._"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return

    lines = [
        f"🎙 Voice: `{config.TTS_VOICE}`",
        f"🖼 Background mode: `{config.BACKGROUND_MODE}`",
        f"🔍 Stock query: `{config.STOCK_QUERY}`",
        f"📝 Whisper model: `{config.WHISPER_MODEL}`",
        f"📐 Resolution: `{config.VIDEO_WIDTH}×{config.VIDEO_HEIGHT}`",
        f"🔤 Caption words: `{config.CAPTION_MAX_WORDS}`",
        f"⚡ Pexels key: `{'set ✅' if config.PEXELS_API_KEY else 'not set ⚠️'}`",
    ]
    await update.message.reply_text(
        "*Current config*\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        return

    job = _jobs.get(uid)
    if not job:
        await update.message.reply_text("No jobs yet. Use /make to start one.")
        return

    elapsed = time.time() - job["started"]
    mins, secs = divmod(int(elapsed), 60)
    await update.message.reply_text(
        f"*Job:* `{_escape(job['job_name'])}`\n"
        f"*Status:* {job['status']}\n"
        f"*Elapsed:* {mins}m {secs}s",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ── /make conversation ────────────────────────────────────────────────────────

async def cmd_make(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    # If script was inline: /make <text>
    if context.args:
        context.user_data["script"] = " ".join(context.args)
        await update.message.reply_text(
            "Got your script! What should I call this job? "
            "(letters/numbers/underscores only, e.g. `my_video_1`)",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return WAITING_JOB_NAME

    await update.message.reply_text(
        "📝 Send me your script — just paste the text you want narrated."
    )
    return WAITING_SCRIPT


async def got_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    script = update.message.text.strip()
    if len(script) < 10:
        await update.message.reply_text("That script looks too short. Try again:")
        return WAITING_SCRIPT

    context.user_data["script"] = script
    await update.message.reply_text(
        f"✅ Got {len(script.split())} words\\. "
        "Now give this job a name \\(e\\.g\\. `video_01`\\):",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return WAITING_JOB_NAME


async def got_job_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        return ConversationHandler.END

    raw = update.message.text.strip()
    # sanitise
    job_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in raw)
    job_name = job_name[:40] or f"video_{int(time.time())}"

    script = context.user_data.get("script", "")
    if not script:
        await update.message.reply_text("No script found. Please /make again.")
        return ConversationHandler.END

    _jobs[uid] = {"status": "⏳ Running…", "started": time.time(), "job_name": job_name}

    await update.message.reply_text(
        f"🚀 Starting job *{_escape(job_name)}*\\.\n"
        "_This takes 1–3 min on a CPU VPS\\. I'll send the video when done\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    # Run pipeline in background so bot stays responsive
    asyncio.create_task(_pipeline_task(update, context, script, job_name, uid))
    return ConversationHandler.END


async def _pipeline_task(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    script: str,
    job_name: str,
    uid: int,
):
    chat_id = update.effective_chat.id
    try:
        # Send "uploading video" action while rendering
        await context.bot.send_chat_action(chat_id=chat_id, action="record_video")
        final_path = await asyncio.to_thread(_run_pipeline_sync, script, job_name)
        _jobs[uid]["status"] = "✅ Done"

        # Send the mp4 back
        if os.path.exists(final_path) and os.path.getsize(final_path) < 50 * 1024 * 1024:
            with open(final_path, "rb") as f:
                await context.bot.send_video(
                    chat_id=chat_id,
                    video=f,
                    caption=f"🎬 {job_name} — done!",
                    supports_streaming=True,
                )
        else:
            size_mb = os.path.getsize(final_path) / 1024 / 1024
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ Video rendered: `{_escape(final_path)}`\n"
                    f"Size: {size_mb:.1f} MB \\(too large to send via Telegram — "
                    "download it from the server directly\\)\\."
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
            )

    except Exception as exc:
        _jobs[uid]["status"] = f"❌ Failed: {exc}"
        tb = traceback.format_exc()
        log.error("Pipeline error:\n%s", tb)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Pipeline failed:\n```\n{str(exc)[:800]}\n```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Cancelled. Use /make to start a new job.")
    return ConversationHandler.END


# ── /setvoice conversation ────────────────────────────────────────────────────

VOICES = [
    "en-US-GuyNeural",
    "en-US-AriaNeural",
    "en-US-ChristopherNeural",
    "en-GB-RyanNeural",
    "en-AU-NatashaNeural",
    "en-IN-NeerjaNeural",
]


async def cmd_setvoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    numbered = "\n".join(f"{i+1}\\. `{v}`" for i, v in enumerate(VOICES))
    await update.message.reply_text(
        f"Current voice: `{_escape(config.TTS_VOICE)}`\n\n"
        f"Pick a number:\n{numbered}\n\nOr type any Edge\\-TTS voice name\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return WAITING_VOICE


async def got_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    text = update.message.text.strip()
    if text.isdigit() and 1 <= int(text) <= len(VOICES):
        voice = VOICES[int(text) - 1]
    else:
        voice = text

    config.TTS_VOICE = voice
    await update.message.reply_text(f"✅ Voice set to `{_escape(voice)}`\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END


# ── /setbg conversation ───────────────────────────────────────────────────────

async def cmd_setbg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    await update.message.reply_text(
        f"Current background mode: `{config.BACKGROUND_MODE}`\n\n"
        "Reply with: `color`, `image`, or `stock_video`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return WAITING_BG_MODE


async def got_bg_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    mode = update.message.text.strip().lower()
    if mode not in ("color", "image", "stock_video"):
        await update.message.reply_text("Invalid. Choose: `color`, `image`, or `stock_video`", parse_mode=ParseMode.MARKDOWN_V2)
        return WAITING_BG_MODE

    config.BACKGROUND_MODE = mode
    await update.message.reply_text(f"✅ Background mode set to `{mode}`\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END


# ── /setquery conversation ────────────────────────────────────────────────────

async def cmd_setquery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    await update.message.reply_text(
        f"Current Pexels query: `{_escape(config.STOCK_QUERY)}`\n\nSend a new search term:",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return WAITING_BG_QUERY


async def got_bg_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return ConversationHandler.END

    query = update.message.text.strip()
    config.STOCK_QUERY = query
    await update.message.reply_text(f"✅ Pexels query set to `{_escape(query)}`\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END


# ── fallback: plain message → treat as script ─────────────────────────────────

async def msg_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return

    text = update.message.text.strip()
    if len(text) > 20:
        context.user_data["script"] = text
        await update.message.reply_text(
            f"📝 Got {len(text.split())} words\\. What should I name this job?",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return WAITING_JOB_NAME

    await update.message.reply_text("Use /make to start a video job, or /start for help.")


# ── error handler ─────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Update caused error: %s", context.error, exc_info=context.error)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        print("ERROR: TELEGRAM_TOKEN is not set.")
        print("  Add it to your .env file:  TELEGRAM_TOKEN=your_token_here")
        sys.exit(1)

    app = Application.builder().token(TOKEN).build()

    # /make conversation
    make_conv = ConversationHandler(
        entry_points=[CommandHandler("make", cmd_make)],
        states={
            WAITING_SCRIPT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, got_script)],
            WAITING_JOB_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_job_name)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    # /setvoice conversation
    voice_conv = ConversationHandler(
        entry_points=[CommandHandler("setvoice", cmd_setvoice)],
        states={WAITING_VOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_voice)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )

    # /setbg conversation
    bg_conv = ConversationHandler(
        entry_points=[CommandHandler("setbg", cmd_setbg)],
        states={WAITING_BG_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_bg_mode)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )

    # /setquery conversation
    query_conv = ConversationHandler(
        entry_points=[CommandHandler("setquery", cmd_setquery)],
        states={WAITING_BG_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_bg_query)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("config", cmd_config))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(make_conv)
    app.add_handler(voice_conv)
    app.add_handler(bg_conv)
    app.add_handler(query_conv)

    # Fallback: plain text → treat as script input
    fallback_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND, msg_fallback)],
        states={
            WAITING_JOB_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_job_name)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )
    app.add_handler(fallback_conv)
    app.add_error_handler(error_handler)

    print("🤖 YT Pipeline Bot is running. Press Ctrl-C to stop.")
    if ALLOWED_IDS:
        print(f"   Whitelist: {ALLOWED_IDS}")
    else:
        print("   ⚠️  No TELEGRAM_ALLOWED_IDS set — bot is open to everyone!")

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
