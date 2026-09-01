import os
from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is not set. Copy .env.example to .env and fill it in."
    )

# Point these at a local Bot API server (https://github.com/tdlib/telegram-bot-api)
# if you want to send files larger than Telegram's default 50MB bot upload limit.
API_BASE_URL = os.getenv("TELEGRAM_API_BASE_URL") or None
API_BASE_FILE_URL = os.getenv("TELEGRAM_API_BASE_FILE_URL") or None

# Proxy used for the bot's OWN connection to Telegram's servers. Only needed
# if your VPS's network/region can't reach api.telegram.org directly (some
# providers/regions block or throttle it). This is separate from PROXY_URL
# below, which is for downloading geo-restricted content.
TELEGRAM_PROXY_URL = os.getenv("TELEGRAM_PROXY_URL") or None

# How long (seconds) to wait on connecting/reading/writing to Telegram before
# giving up. Raise these if your VPS has a slow or flaky link to Telegram.
TELEGRAM_CONNECT_TIMEOUT = float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "30"))
TELEGRAM_READ_TIMEOUT = float(os.getenv("TELEGRAM_READ_TIMEOUT", "30"))
TELEGRAM_WRITE_TIMEOUT = float(os.getenv("TELEGRAM_WRITE_TIMEOUT", "30"))
TELEGRAM_POOL_TIMEOUT = float(os.getenv("TELEGRAM_POOL_TIMEOUT", "30"))

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "3"))
DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "600"))

# Route requests through a proxy, useful for content that's geo-restricted in
# your VPS's region (point it at a proxy located where the content IS available).
PROXY_URL = os.getenv("PROXY_URL") or None

# Path to a Netscape-format cookies.txt for sites that need a login session
# (private/age-restricted posts, some Instagram/Twitter content, etc.)
# Defaults to cookies.txt next to this file so the /cookies upload feature
# below works out of the box even if you never set this explicitly.
COOKIES_FILE = os.getenv("COOKIES_FILE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cookies.txt"
)

# Comma separated Telegram user IDs allowed to use the bot. Empty = everyone.
_allowed = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS = {int(x) for x in _allowed.split(",") if x.strip().isdigit()}

# Comma separated Telegram user IDs allowed to upload/replace cookies.txt and
# manage it via /cookies and /clearcookies. This file contains a live login
# session, so it's a separate, stricter allow-list from ALLOWED_USER_IDS.
# Falls back to ALLOWED_USER_IDS if not set; if BOTH are empty (fully open
# bot), cookie management is disabled entirely for safety.
_admin = os.getenv("ADMIN_USER_IDS", "")
ADMIN_USER_IDS = {int(x) for x in _admin.split(",") if x.strip().isdigit()}
if not ADMIN_USER_IDS:
    ADMIN_USER_IDS = set(ALLOWED_USER_IDS)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
