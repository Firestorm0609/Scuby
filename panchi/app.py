import os
import time
import logging
import sqlite3
import httpx
from contextlib import contextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from dotenv import load_dotenv
import secrets

load_dotenv()

BOT_TOKEN  = os.getenv("BOT_TOKEN")
CHAT_ID    = os.getenv("CHAT_ID")
ADMIN_PASS = os.getenv("ADMIN_PASSWORD")
DB_PATH    = os.getenv("DB_PATH", "panchi.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── STARTUP GUARD ──────────────────────────────────────────
if not ADMIN_PASS:
    raise RuntimeError("ADMIN_PASSWORD env var is not set — refusing to start.")

app = FastAPI()
security = HTTPBasic()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://panchi.page", "https://www.panchi.page"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── RATE LIMITER ───────────────────────────────────────────
# Simple in-process store: { ip: [timestamp, ...] }
# For multi-worker deployments, replace with Redis.
_rate_store: dict[str, list[float]] = {}

def rate_limit(request: Request, max_calls: int, window_seconds: int):
    """
    Raise 429 if the caller has exceeded max_calls within window_seconds.
    Keyed by X-Forwarded-For (first hop) falling back to direct client IP.
    """
    forwarded = request.headers.get("X-Forwarded-For")
    ip = forwarded.split(",")[0].strip() if forwarded else request.client.host

    now = time.monotonic()
    cutoff = now - window_seconds
    bucket = [t for t in _rate_store.get(ip, []) if t > cutoff]
    bucket.append(now)
    _rate_store[ip] = bucket

    if len(bucket) > max_calls:
        raise HTTPException(
            status_code=429,
            detail=f"Too many requests — try again in {window_seconds} seconds.",
        )

def apply_limit(request: Request):
    """5 submissions per IP per hour."""
    rate_limit(request, max_calls=5, window_seconds=3600)

def status_limit(request: Request):
    """30 status checks per IP per minute."""
    rate_limit(request, max_calls=30, window_seconds=60)

# ─── DB SETUP ───────────────────────────────────────────────
@contextmanager
def get_db():
    """
    Context-managed SQLite connection.
    Guarantees close() even if the caller raises.

    Usage:
        with get_db() as conn:
            conn.execute(...)
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS submissions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet       TEXT NOT NULL,
                twitter      TEXT NOT NULL,
                why          TEXT NOT NULL,
                held         TEXT NOT NULL,
                referral     TEXT,
                status       TEXT NOT NULL DEFAULT 'pending',
                submitted_at TEXT NOT NULL
            )
        """)
        conn.commit()

init_db()

# ─── MODELS ─────────────────────────────────────────────────
class Submission(BaseModel):
    wallet:   str
    twitter:  str
    why:      str
    held:     str
    referral: str = ""

class StatusCheck(BaseModel):
    query: str

# ─── HELPERS ────────────────────────────────────────────────
def clean_twitter(handle: str) -> str:
    return handle.strip().lstrip('@').lower()

def escape_html(text: str) -> str:
    """Escape user-supplied text for Telegram HTML mode.
    Only three characters need escaping — nothing user types can break this."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def require_admin(credentials: HTTPBasicCredentials = Depends(security)):
    """Raise 401 if credentials are wrong."""
    correct = secrets.compare_digest(
        credentials.password.encode(),
        ADMIN_PASS.encode(),
    )
    if credentials.username != "panchi" or not correct:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ─── TELEGRAM ───────────────────────────────────────────────
async def notify_telegram(sub: Submission, sub_id: int) -> bool:
    """
    Send a Telegram notification. Returns True on success, False on failure.
    Callers decide whether to surface the failure to the user.
    """
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("Telegram notify skipped — BOT_TOKEN or CHAT_ID not set.")
        return False

    handle           = clean_twitter(sub.twitter)
    why_escaped      = escape_html(sub.why.strip())
    referral_escaped = escape_html(sub.referral.strip() or "—")
    held_escaped     = escape_html(sub.held)

    text = (
        f"🐒 <b>New Panchilist Application #{sub_id}</b>\n\n"
        f"<b>Wallet:</b> <code>{sub.wallet}</code>\n"
        f"<b>Twitter:</b> <a href=\"https://x.com/{handle}\">@{handle}</a>\n"
        f"<b>NFTs held 6m+:</b> {held_escaped}\n"
        f"<b>Referral:</b> {referral_escaped}\n\n"
        f"<b>Why they deserve it:</b>\n<i>{why_escaped}</i>"
    )
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Approve", "callback_data": f"approve:{sub_id}:pending:0"},
            {"text": "❌ Reject",  "callback_data": f"reject:{sub_id}:pending:0"},
        ]]
    }
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":      CHAT_ID,
                    "text":         text,
                    "parse_mode":   "HTML",
                    "reply_markup": keyboard,
                },
                timeout=10,
            )
            r.raise_for_status()
        return True
    except Exception as e:
        logger.error("Telegram notify failed for submission #%s: %s", sub_id, e)
        return False

# ─── ROUTES ─────────────────────────────────────────────────
@app.post("/apply")
async def apply(
    sub: Submission,
    request: Request,
    _: None = Depends(apply_limit),
):
    wallet  = sub.wallet.strip()
    twitter = clean_twitter(sub.twitter)

    if not (wallet.startswith("0x") and len(wallet) == 42):
        raise HTTPException(status_code=400, detail="Invalid wallet address")
    if not twitter:
        raise HTTPException(status_code=400, detail="Twitter handle required")
    if not sub.why.strip():
        raise HTTPException(status_code=400, detail="Why field required")
    if not sub.held:
        raise HTTPException(status_code=400, detail="Held selection required")

    with get_db() as conn:
        # Block if a non-rejected entry already exists.
        # Rejected applicants may reapply — their old entry stays as a record.
        existing = conn.execute(
            """SELECT id FROM submissions
               WHERE (LOWER(wallet) = LOWER(?) OR LOWER(REPLACE(twitter,'@','')) = ?)
                 AND status != 'rejected'
               LIMIT 1""",
            (wallet, twitter),
        ).fetchone()
        if existing:
            raise HTTPException(
                status_code=409,
                detail="An active application already exists for this wallet or handle.",
            )

        now = datetime.utcnow().isoformat()
        cur = conn.execute(
            "INSERT INTO submissions (wallet, twitter, why, held, referral, status, submitted_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                wallet,
                sub.twitter.strip(),
                sub.why.strip(),
                sub.held,
                (sub.referral or "").strip(),
                "pending",
                now,
            ),
        )
        sub_id = cur.lastrowid
        conn.commit()

    telegram_ok = await notify_telegram(sub, sub_id)

    return {
        "ok": True,
        "id": sub_id,
        # Surface a soft warning if the admin didn't receive the notification,
        # so they can check the submissions endpoint manually.
        **({"warning": "Submission saved, but the admin notification failed to send."} if not telegram_ok else {}),
    }


@app.post("/status")
async def check_status(
    body: StatusCheck,
    _: None = Depends(status_limit),
):
    query = body.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query required")

    with get_db() as conn:
        row = conn.execute(
            "SELECT status, submitted_at FROM submissions "
            "WHERE LOWER(wallet) = LOWER(?) ORDER BY id DESC LIMIT 1",
            (query,),
        ).fetchone()

        if not row:
            handle = query.lstrip("@").lower()
            row = conn.execute(
                "SELECT status, submitted_at FROM submissions "
                "WHERE LOWER(REPLACE(twitter,'@','')) = ? ORDER BY id DESC LIMIT 1",
                (handle,),
            ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No application found")

    return {"status": row["status"], "submitted_at": row["submitted_at"]}


@app.post("/admin/update")
async def update_status(
    payload: dict,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    sub_id = payload.get("id")
    status = payload.get("status")
    if status not in ("approved", "rejected", "pending"):
        raise HTTPException(status_code=400, detail="Invalid status")
    if sub_id is None:
        raise HTTPException(status_code=400, detail="Missing id")

    with get_db() as conn:
        conn.execute("UPDATE submissions SET status=? WHERE id=?", (status, sub_id))
        conn.commit()

    return {"ok": True}


@app.get("/submissions")
def get_submissions(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        rows = conn.execute("SELECT * FROM submissions ORDER BY id DESC").fetchall()

    return [dict(r) for r in rows]


@app.get("/admin/export")
def export_approved(
    full: bool = False,
    credentials: HTTPBasicCredentials = Depends(security),
):
    """
    Export approved wallets as CSV.
    ?full=false (default) → bare wallet addresses, one per line, no header
    ?full=true            → wallet, twitter, held, referral, submitted_at
    """
    require_admin(credentials)

    with get_db() as conn:
        rows = conn.execute(
            "SELECT wallet, twitter, held, referral, submitted_at "
            "FROM submissions WHERE status='approved' ORDER BY id ASC"
        ).fetchall()

    if full:
        lines = ["wallet,twitter,held,referral,submitted_at"]
        for r in rows:
            twitter  = r["twitter"].strip().lstrip("@")
            held     = r["held"].replace(",", " ")
            referral = (r["referral"] or "").replace(",", " ")
            lines.append(f"{r['wallet']},{twitter},{held},{referral},{r['submitted_at'][:10]}")
    else:
        lines = [r["wallet"] for r in rows]

    content  = "\n".join(lines)
    filename = "panchi_approved_full.csv" if full else "panchi_approved_wallets.csv"

    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
