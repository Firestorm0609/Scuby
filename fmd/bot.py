import os
import re
import shutil
import logging
import asyncio
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import config
from downloader import download_media, cleanup, DownloadError

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("media-bot")

URL_RE = re.compile(r"https?://\S+")
COOKIE_MAX_BYTES = 1_000_000  # 1MB is generous for a cookies.txt

semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)


def is_allowed(user_id: int) -> bool:
    return not config.ALLOWED_USER_IDS or user_id in config.ALLOWED_USER_IDS


def is_admin(user_id: int) -> bool:
    return user_id in config.ADMIN_USER_IDS


def _looks_like_netscape_cookies(data: bytes) -> bool:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
    if not lines:
        return False
    # Netscape format: domain, flag, path, secure, expiration, name, value
    # (tab-separated, 7 fields) — spot-check a few lines rather than all of
    # them in case the file is large.
    sample = lines[:5]
    return all(len(ln.split("\t")) >= 6 for ln in sample)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send me one or more links — YouTube, TikTok, Instagram, Twitter/X, "
        "Facebook, Reddit, Pinterest, SoundCloud, and hundreds of other sites "
        "are supported — and I'll grab the video/image and send it back here.\n\n"
        "If you're an admin: send me a cookies.txt file any time to "
        "add/replace login cookies, or use /cookies to check status."
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_allowed(user.id):
        await update.message.reply_text("You're not authorized to use this bot.")
        logger.warning("Blocked unauthorized user_id=%s", user.id)
        return

    text = update.message.text or ""
    urls = URL_RE.findall(text)
    if not urls:
        await update.message.reply_text("Send me a valid link.")
        return

    for url in urls:
        await process_url(update, url)


async def process_url(update: Update, url: str):
    status_msg = await update.message.reply_text(f"Downloading…\n{url}")

    async with semaphore:
        try:
            items = await download_media(url)
        except DownloadError as e:
            await status_msg.edit_text(f"Couldn't download that link.\nReason: {e}")
            return
        except Exception as e:
            logger.exception("Unexpected error for %s", url)
            await status_msg.edit_text(f"Unexpected error: {e}")
            return

    try:
        for item in items:
            size_mb = os.path.getsize(item.path) / (1024 * 1024)
            if size_mb > config.MAX_FILE_SIZE_MB:
                await update.message.reply_text(
                    f"'{item.title or os.path.basename(item.path)}' is "
                    f"{size_mb:.1f}MB, over the {config.MAX_FILE_SIZE_MB}MB limit, "
                    f"so I can't send it. Run a local Bot API server to raise this "
                    f"limit (see README)."
                )
                continue

            with open(item.path, "rb") as f:
                if item.type == "video":
                    await update.message.reply_video(
                        f,
                        caption=item.title,
                        supports_streaming=True,
                        read_timeout=120,
                        write_timeout=120,
                        connect_timeout=60,
                    )
                elif item.type == "photo":
                    await update.message.reply_photo(f, caption=item.title)
                elif item.type == "audio":
                    await update.message.reply_audio(f, caption=item.title)
                else:
                    await update.message.reply_document(f, caption=item.title)
        await status_msg.delete()
    finally:
        cleanup(items)


async def handle_cookie_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if not document:
        return

    name = (document.file_name or "").lower()
    looks_relevant = name.endswith(".txt") or document.mime_type == "text/plain"
    if not looks_relevant:
        return  # not a .txt upload, don't treat it as a cookies attempt

    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text(
            "You're not authorized to update cookies. Set ADMIN_USER_IDS in "
            ".env to allow your account to do this."
        )
        logger.warning("Blocked cookie upload from unauthorized user_id=%s", user.id)
        return

    if document.file_size and document.file_size > COOKIE_MAX_BYTES:
        await update.message.reply_text(
            "That file is too large to be a cookies.txt — not saving it."
        )
        return

    tg_file = await context.bot.get_file(document.file_id)
    raw = bytes(await tg_file.download_as_bytearray())

    if not _looks_like_netscape_cookies(raw):
        await update.message.reply_text(
            "That doesn't look like a Netscape-format cookies.txt, so I didn't "
            'save it. Export it with a tool like "Get cookies.txt LOCALLY" and '
            "send that file directly."
        )
        return

    target = config.COOKIES_FILE
    target_dir = os.path.dirname(target)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    if os.path.isfile(target):
        shutil.copyfile(target, target + ".bak")  # keep one backup

    with open(target, "wb") as f:
        f.write(raw)
    os.chmod(target, 0o600)  # session cookies — keep it readable by us only

    await update.message.reply_text(
        f"Cookies updated ({len(raw)} bytes). New downloads will use this "
        f"session right away — no restart needed."
    )
    logger.info("Cookies updated by user_id=%s (%d bytes)", user.id, len(raw))


async def cookies_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("You're not authorized to manage cookies.")
        return

    path = config.COOKIES_FILE
    if os.path.isfile(path):
        size = os.path.getsize(path)
        mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        await update.message.reply_text(
            f"cookies.txt is set — {size} bytes, last updated {mtime}.\n"
            f"Send a new file to replace it, or /clearcookies to remove it."
        )
    else:
        await update.message.reply_text(
            "No cookies.txt is set. Send the file exported from your browser "
            "to add one."
        )


async def clear_cookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("You're not authorized to manage cookies.")
        return

    path = config.COOKIES_FILE
    if os.path.isfile(path):
        os.remove(path)
        await update.message.reply_text("cookies.txt removed.")
        logger.info("Cookies cleared by user_id=%s", user.id)
    else:
        await update.message.reply_text("No cookies.txt to remove.")


def main():
    builder = (
        ApplicationBuilder()
        .token(config.BOT_TOKEN)
        .connect_timeout(config.TELEGRAM_CONNECT_TIMEOUT)
        .read_timeout(config.TELEGRAM_READ_TIMEOUT)
        .write_timeout(config.TELEGRAM_WRITE_TIMEOUT)
        .pool_timeout(config.TELEGRAM_POOL_TIMEOUT)
        .get_updates_connect_timeout(config.TELEGRAM_CONNECT_TIMEOUT)
        .get_updates_read_timeout(config.TELEGRAM_READ_TIMEOUT)
    )
    if config.API_BASE_URL:
        builder = builder.base_url(config.API_BASE_URL)
    if config.API_BASE_FILE_URL:
        builder = builder.base_file_url(config.API_BASE_FILE_URL)
    if config.TELEGRAM_PROXY_URL:
        builder = builder.proxy(config.TELEGRAM_PROXY_URL).get_updates_proxy(
            config.TELEGRAM_PROXY_URL
        )

    app = builder.build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("cookies", cookies_status))
    app.add_handler(CommandHandler("clearcookies", clear_cookies))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_cookie_upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot starting…")

    # Telegram connections can blip on flaky VPS links — retry on startup
    # instead of crashing outright. run_polling already retries mid-session;
    # this just covers the very first connection attempt too.
    delay = 5
    max_delay = 60
    while True:
        try:
            app.run_polling(allowed_updates=Update.ALL_TYPES)
            break  # run_polling only returns on a clean shutdown
        except Exception:
            logger.exception(
                "Couldn't connect to Telegram, retrying in %ss "
                "(check network/TELEGRAM_PROXY_URL if this keeps happening)",
                delay,
            )
            import time

            time.sleep(delay)
            delay = min(delay * 2, max_delay)


if __name__ == "__main__":
    main()
