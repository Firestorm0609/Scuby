import os
import glob
import uuid
import shutil
import logging
import asyncio
import subprocess
from dataclasses import dataclass
from typing import List, Optional

import yt_dlp

import config

logger = logging.getLogger("media-bot.downloader")

os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)

VIDEO_EXT = {"mp4", "mkv", "webm", "mov", "avi", "m4v"}
PHOTO_EXT = {"jpg", "jpeg", "png", "webp", "gif", "bmp"}
AUDIO_EXT = {"mp3", "m4a", "wav", "ogg", "opus", "flac"}


@dataclass
class DownloadedItem:
    path: str
    type: str  # "video" | "photo" | "audio" | "document"
    title: Optional[str] = None


class DownloadError(Exception):
    pass


def _classify(path: str) -> str:
    ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    if ext in VIDEO_EXT:
        return "video"
    if ext in PHOTO_EXT:
        return "photo"
    if ext in AUDIO_EXT:
        return "audio"
    return "document"


def _ytdlp_opts(job_dir: str, player_client: Optional[str] = None) -> dict:
    opts = {
        "outtmpl": os.path.join(job_dir, "%(title).80s_%(id)s.%(ext)s"),
        "format": "bv*+ba/best",
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": False,
        "restrictfilenames": True,
        "concurrent_fragment_downloads": 4,
        "retries": 5,
        "fragment_retries": 5,
        "socket_timeout": 30,
    }
    if config.PROXY_URL:
        opts["proxy"] = config.PROXY_URL
    if config.COOKIES_FILE and os.path.isfile(config.COOKIES_FILE):
        opts["cookiefile"] = config.COOKIES_FILE
    if player_client:
        opts["extractor_args"] = {"youtube": {"player_client": [player_client]}}
    return opts


# YouTube's "Sign in to confirm you're not a bot" wall mostly targets the
# default web client on datacenter/VPS IPs. These alternate clients are
# sometimes exempt and don't need cookies — worth trying before giving up.
# This is a moving target (YouTube changes what's exempt over time); if all
# of these eventually get blocked too, cookies become mandatory again.
YOUTUBE_FALLBACK_CLIENTS = ["android", "ios"]


def _try_ytdlp(url: str, job_dir: str) -> List[DownloadedItem]:
    attempts = [None] + YOUTUBE_FALLBACK_CLIENTS
    last_error = None

    for client in attempts:
        opts = _ytdlp_opts(job_dir, player_client=client)
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
            break
        except Exception as e:
            last_error = e
            is_bot_check = "sign in to confirm" in str(e).lower() or "not a bot" in str(e).lower()
            if client is None and is_bot_check:
                logger.info("YouTube bot-check hit for %s, trying alternate clients", url)
                continue
            if client is not None and is_bot_check:
                continue  # try the next fallback client
            raise  # different kind of error, don't bother with fallbacks
    else:
        if last_error and (
            "sign in to confirm" in str(last_error).lower()
            or "not a bot" in str(last_error).lower()
        ):
            raise DownloadError(
                "YouTube is requiring a login to confirm this isn't a bot. "
                "Set COOKIES_FILE in .env to a cookies.txt exported from a "
                "logged-in browser session — see README."
            )
        raise last_error

    if isinstance(info, dict) and info.get("entries"):
        entries = [e for e in info["entries"] if e]
    else:
        entries = [info] if info else []

    items: List[DownloadedItem] = []
    for entry in entries:
        filepath = ydl.prepare_filename(entry)
        if not os.path.isfile(filepath):
            # Postprocessors (merge/remux) can change the final extension
            base, _ = os.path.splitext(filepath)
            matches = glob.glob(base + ".*")
            filepath = matches[0] if matches else filepath
        if os.path.isfile(filepath):
            items.append(
                DownloadedItem(
                    path=filepath, type=_classify(filepath), title=entry.get("title")
                )
            )

    if not items:
        raise DownloadError("yt-dlp ran but no files were produced")
    return items


def _try_gallery_dl(url: str, job_dir: str) -> List[DownloadedItem]:
    cmd = ["gallery-dl", "--dest", job_dir, "-q"]
    if config.PROXY_URL:
        cmd += ["--proxy", config.PROXY_URL]
    if config.COOKIES_FILE and os.path.isfile(config.COOKIES_FILE):
        cmd += ["--cookies", config.COOKIES_FILE]
    cmd.append(url)

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=config.DOWNLOAD_TIMEOUT_SECONDS
    )
    if result.returncode != 0:
        raise DownloadError(f"gallery-dl failed: {result.stderr.strip()[:300]}")

    files = []
    for root, _, filenames in os.walk(job_dir):
        for fn in filenames:
            files.append(os.path.join(root, fn))

    if not files:
        raise DownloadError("gallery-dl ran but no files were produced")

    return [DownloadedItem(path=f, type=_classify(f)) for f in files]


async def download_media(url: str) -> List[DownloadedItem]:
    """
    Downloads media from `url` into a fresh temp folder.
    Tries yt-dlp first (covers ~1800 video/audio sites: YouTube, TikTok,
    Twitter/X, Instagram reels, Facebook, Reddit, SoundCloud, Pinterest...),
    then falls back to gallery-dl which handles pure image posts/carousels
    better (Instagram photo posts, Twitter image tweets, Pinterest pins...).
    Raises DownloadError if neither tool can handle the link.
    """
    job_dir = os.path.join(config.DOWNLOAD_DIR, uuid.uuid4().hex)
    os.makedirs(job_dir, exist_ok=True)

    loop = asyncio.get_running_loop()
    try:
        try:
            items = await loop.run_in_executor(None, _try_ytdlp, url, job_dir)
        except Exception as e:
            logger.info("yt-dlp couldn't handle %s (%s) — trying gallery-dl", url, e)
            items = await loop.run_in_executor(None, _try_gallery_dl, url, job_dir)
        return items
    except Exception as e:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise DownloadError(str(e)) from e


def cleanup(items: List[DownloadedItem]) -> None:
    dirs = {os.path.dirname(i.path) for i in items}
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)
