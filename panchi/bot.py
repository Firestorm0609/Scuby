import os
import base64
import sqlite3
import httpx
import asyncio
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH   = os.getenv("DB_PATH", "panchi.db")
API_HOST  = os.getenv("API_HOST", "http://127.0.0.1:8001")

# ─── STARTUP GUARD ──────────────────────────────────────────
_chat_id_raw = os.getenv("CHAT_ID")
if not _chat_id_raw:
    raise RuntimeError("CHAT_ID env var is not set — refusing to start.")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set — refusing to start.")
_admin_pass = os.getenv("ADMIN_PASSWORD")
if not _admin_pass:
    raise RuntimeError("ADMIN_PASSWORD env var is not set — refusing to start.")

CHAT_ID    = int(_chat_id_raw)
ADMIN_PASS = _admin_pass
BASE_URL   = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ─── DB ─────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_counts():
    conn = get_db()
    try:
        total    = conn.execute("SELECT COUNT(*) FROM submissions").fetchone()[0]
        pending  = conn.execute("SELECT COUNT(*) FROM submissions WHERE status='pending'").fetchone()[0]
        approved = conn.execute("SELECT COUNT(*) FROM submissions WHERE status='approved'").fetchone()[0]
        rejected = conn.execute("SELECT COUNT(*) FROM submissions WHERE status='rejected'").fetchone()[0]
    finally:
        conn.close()
    return total, pending, approved, rejected

def get_submissions_by_status(status, offset=0):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM submissions WHERE status=? ORDER BY id ASC LIMIT 1 OFFSET ?",
            (status, offset),
        ).fetchone()
        count = conn.execute(
            "SELECT COUNT(*) FROM submissions WHERE status=?", (status,)
        ).fetchone()[0]
    finally:
        conn.close()
    return dict(row) if row else None, count

def get_approved_count():
    conn = get_db()
    try:
        return conn.execute("SELECT COUNT(*) FROM submissions WHERE status='approved'").fetchone()[0]
    finally:
        conn.close()

def update_status(sub_id, status):
    conn = get_db()
    try:
        conn.execute("UPDATE submissions SET status=? WHERE id=?", (status, sub_id))
        conn.commit()
    finally:
        conn.close()

def delete_submission(sub_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM submissions WHERE id=?", (sub_id,))
        conn.commit()
    finally:
        conn.close()

# ─── TELEGRAM HELPERS ───────────────────────────────────────
async def send(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/sendMessage", json=payload, timeout=10)

async def edit(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id, "message_id": message_id,
        "text": text, "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/editMessageText", json=payload, timeout=10)

async def answer_callback(callback_id, text=""):
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{BASE_URL}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=10,
        )

# ─── HELPERS ────────────────────────────────────────────────
def clean_twitter(handle):
    return handle.strip().lstrip("@").lower()

def escape_html(text: str) -> str:
    """Escape user-supplied text for Telegram HTML mode.
    Only three characters need escaping — nothing user types can break this."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

# ─── MENU BUILDERS ──────────────────────────────────────────
def main_menu_text():
    total, pending, approved, rejected = get_counts()
    return (
        f"🐒 <b>Panchi Admin</b>\n\n"
        f"📋 Total applications: <b>{total}</b>\n"
        f"⏳ Pending: <b>{pending}</b>\n"
        f"✅ Approved: <b>{approved}</b>\n"
        f"❌ Rejected: <b>{rejected}</b>"
    )

def main_menu_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "⏳ Applications", "callback_data": "list:pending:0"},
                {"text": "✅ Approved",      "callback_data": "list:approved:0"},
                {"text": "❌ Rejected",      "callback_data": "list:rejected:0"},
            ],
            [
                {"text": "📥 Export",        "callback_data": "export_menu"},
            ],
        ]
    }

def submission_text(sub):
    handle        = clean_twitter(sub["twitter"])
    why_escaped   = escape_html(sub["why"])
    referral_safe = escape_html(sub["referral"] or "—")
    held_escaped  = escape_html(sub["held"])
    date          = escape_html(sub["submitted_at"][:10])

    return (
        f"🐒 <b>Application #{sub['id']}</b>\n\n"
        f"<b>Wallet:</b> <code>{sub['wallet']}</code>\n"
        f"<b>Twitter:</b> <a href=\"https://x.com/{handle}\">@{handle}</a>\n"
        f"<b>NFTs held 6m+:</b> {held_escaped}\n"
        f"<b>Referral:</b> {referral_safe}\n"
        f"<b>Submitted:</b> {date}\n\n"
        f"<b>Why they deserve it:</b>\n<i>{why_escaped}</i>"
    )

def submission_keyboard(sub_id, status, offset, total):
    nav = []
    if offset > 0:
        nav.append({"text": "◀ Prev", "callback_data": f"list:{status}:{offset - 1}"})
    if offset < total - 1:
        nav.append({"text": "Next ▶", "callback_data": f"list:{status}:{offset + 1}"})

    actions = []
    if status == "pending":
        actions.append({"text": "✅ Approve", "callback_data": f"approve:{sub_id}:{status}:{offset}"})
        actions.append({"text": "❌ Reject",  "callback_data": f"reject:{sub_id}:{status}:{offset}"})
    elif status == "approved":
        actions.append({"text": "🔄 Revoke",  "callback_data": f"reject:{sub_id}:{status}:{offset}"})
    elif status == "rejected":
        actions.append({"text": "✅ Approve", "callback_data": f"approve:{sub_id}:{status}:{offset}"})
        actions.append({"text": "⏳ Pending", "callback_data": f"pending:{sub_id}:{status}:{offset}"})

    delete_btn = {"text": "🗑 Delete", "callback_data": f"delete_confirm:{sub_id}:{status}:{offset}"}

    rows = []
    if actions:
        rows.append(actions)
    rows.append([delete_btn])
    if nav:
        rows.append(nav)
    rows.append([{"text": "🔙 Back", "callback_data": "menu"}])

    return {"inline_keyboard": rows}

def delete_confirm_keyboard(sub_id, status, offset):
    return {
        "inline_keyboard": [
            [
                {"text": "⚠️ Yes, delete", "callback_data": f"delete_do:{sub_id}:{status}:{offset}"},
                {"text": "✗ Cancel",        "callback_data": f"list:{status}:{offset}"},
            ]
        ]
    }

# ─── RELOAD LIST ────────────────────────────────────────────
async def reload_list(chat_id, message_id, status, offset, prefix_msg=""):
    sub, count = get_submissions_by_status(status, offset)
    if not sub and offset > 0:
        offset = max(0, offset - 1)
        sub, count = get_submissions_by_status(status, offset)

    if not sub or count == 0:
        body = (
            f"{escape_html(prefix_msg)}\n\nNo more <b>{status}</b> applications."
            if prefix_msg
            else f"No <b>{status}</b> applications."
        )
        await edit(
            chat_id, message_id, body,
            {"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "menu"}]]},
        )
        return

    prefix = f"{escape_html(prefix_msg)}\n\n" if prefix_msg else ""
    txt = prefix + f"<b>{offset + 1} of {count} {status}</b>\n\n" + submission_text(sub)
    kb  = submission_keyboard(sub["id"], status, offset, count)
    await edit(chat_id, message_id, txt, kb)

# ─── EXPORT ─────────────────────────────────────────────────
async def send_export(chat_id, full: bool):
    """Fetch the CSV from the API and send it as a Telegram document."""
    ADMIN_USER = "panchi"
    url   = f"{API_HOST}/admin/export?full={'true' if full else 'false'}"
    token = base64.b64encode(f"{ADMIN_USER}:{ADMIN_PASS}".encode()).decode()

    async with httpx.AsyncClient() as client:
        r = await client.get(
            url,
            headers={"Authorization": f"Basic {token}"},
            timeout=15,
        )

    if r.status_code != 200:
        await send(chat_id, "⚠️ Export failed. Check the API is running.")
        return

    filename = "panchi_approved_full.csv" if full else "panchi_approved_wallets.csv"
    caption  = "Full export (wallet + details)" if full else "Wallets only"

    async with httpx.AsyncClient() as client:
        await client.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"document": (filename, r.content, "text/csv")},
            timeout=20,
        )

# ─── HANDLERS ───────────────────────────────────────────────
async def handle_message(msg):
    chat_id = msg["chat"]["id"]
    if chat_id != CHAT_ID:
        return
    text = msg.get("text", "")
    if text in ("/start", "/menu"):
        await send(chat_id, main_menu_text(), main_menu_keyboard())

async def handle_callback(cb):
    chat_id    = cb["message"]["chat"]["id"]
    message_id = cb["message"]["message_id"]
    cb_id      = cb["id"]
    data       = cb["data"]

    if chat_id != CHAT_ID:
        return

    await answer_callback(cb_id)

    try:
        if data == "menu":
            await edit(chat_id, message_id, main_menu_text(), main_menu_keyboard())
            return

        if data.startswith("list:"):
            _, status, offset_str = data.split(":")
            offset = int(offset_str)
            sub, count = get_submissions_by_status(status, offset)
            if not sub or count == 0:
                await edit(
                    chat_id, message_id,
                    f"No <b>{status}</b> applications.",
                    {"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "menu"}]]},
                )
                return
            txt = f"<b>{offset + 1} of {count} {status}</b>\n\n" + submission_text(sub)
            kb  = submission_keyboard(sub["id"], status, offset, count)
            await edit(chat_id, message_id, txt, kb)
            return

        if data.startswith("approve:"):
            _, sub_id, status, offset_str = data.split(":")
            update_status(int(sub_id), "approved")
            await reload_list(chat_id, message_id, status, int(offset_str), f"✅ Approved #{sub_id}")
            return

        if data.startswith("reject:"):
            _, sub_id, status, offset_str = data.split(":")
            update_status(int(sub_id), "rejected")
            label = "🔄 Revoked" if status == "approved" else "❌ Rejected"
            await reload_list(chat_id, message_id, status, int(offset_str), f"{label} #{sub_id}")
            return

        if data.startswith("pending:"):
            _, sub_id, status, offset_str = data.split(":")
            update_status(int(sub_id), "pending")
            await reload_list(chat_id, message_id, status, int(offset_str), f"⏳ Moved #{sub_id} to pending")
            return

        if data.startswith("delete_confirm:"):
            _, sub_id, status, offset_str = data.split(":")
            await edit(
                chat_id, message_id,
                f"⚠️ <b>Delete application #{sub_id}?</b>\n\nThis cannot be undone.",
                delete_confirm_keyboard(sub_id, status, offset_str),
            )
            return

        if data.startswith("delete_do:"):
            _, sub_id, status, offset_str = data.split(":")
            delete_submission(int(sub_id))
            await reload_list(chat_id, message_id, status, int(offset_str), f"🗑 Deleted #{sub_id}")
            return

        if data == "export_menu":
            count = get_approved_count()
            await edit(
                chat_id, message_id,
                f"📥 <b>Export Approved Wallets</b>\n\n<b>{count}</b> approved address(es) ready.",
                {
                    "inline_keyboard": [
                        [
                            {"text": "💼 Wallets only", "callback_data": "export_do:wallets"},
                            {"text": "📋 Full details",  "callback_data": "export_do:full"},
                        ],
                        [{"text": "🔙 Back", "callback_data": "menu"}],
                    ]
                },
            )
            return

        if data.startswith("export_do:"):
            _, mode = data.split(":")
            full = mode == "full"
            await send_export(chat_id, full)
            return

        print(f"Unhandled callback data: {data!r}")

    except Exception as e:
        print(f"Error handling callback {data!r}: {e}")

# ─── POLLING LOOP ───────────────────────────────────────────
async def poll():
    offset = 0
    print("Panchi bot running...")
    while True:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{BASE_URL}/getUpdates",
                    params={"offset": offset, "timeout": 30},
                    timeout=35,
                )
            data = r.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                if "message" in update:
                    await handle_message(update["message"])
                elif "callback_query" in update:
                    await handle_callback(update["callback_query"])
        except Exception as e:
            print(f"Polling error: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(poll())
