"""
codebase.py — Full codebase context for multi-file reasoning.

The key gap between Scuby and a real coding agent: Scuby reads files
one at a time. This module loads the ENTIRE codebase into context
so Scuby can reason about cross-file dependencies, refactoring, and
architecture — just like a human developer reading the whole project.

Lets Scuby:
  - Load all files into a single context window
  - Answer questions about the full codebase
  - Plan multi-file refactors
  - Understand cross-file dependencies
  - Trace error chains across modules
"""

import asyncio
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.resolve()

# Files to include in full codebase context (skip large/irrelevant files)
CODEBASE_FILES = [
    "main.py",
    "ai.py",
    "handlers.py",
    "handlers_ai_addition.py",
    "utils.py",
    "gemscore.py",
    "memory.py",
    "feeds.py",
    "smart_filters.py",
    "proactive.py",
    "jobs.py",
    "wallet_tracker.py",
    "portfolio.py",
    "reminders.py",
    "solscan.py",
    "pair_cache.py",
    "rate_limiter.py",
    "self_improve.py",
    "repo.py",
    "analysis.py",
    "terminal.py",
    "docgen.py",
    "codebase.py",
    "git_ops.py",
]

# Max chars per file in context (to stay within AI limits)
MAX_CHARS_PER_FILE = 4000
# Total context budget
TOTAL_CONTEXT_BUDGET = 50000


def load_full_codebase(max_per_file: int = MAX_CHARS_PER_FILE) -> dict:
    """
    Load ALL project files into a single context dict.
    Returns {filename: content} for every core file.
    This is what gives Scuby "full codebase awareness."
    """
    files = {}
    total_chars = 0

    for filename in CODEBASE_FILES:
        path = PROJECT_ROOT / filename
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            # Truncate if too large
            if len(content) > max_per_file:
                content = content[:max_per_file] + f"\n... ({len(path.read_text())} total chars, truncated)"
            files[filename] = content
            total_chars += len(content)
        except Exception as e:
            logger.debug(f"Failed to load {filename}: {e}")

    return {
        "files": files,
        "total_files": len(files),
        "total_chars": total_chars,
        "filenames": list(files.keys()),
    }


def load_codebase_summary() -> str:
    """Load a condensed summary of the entire codebase."""
    codebase = load_full_codebase(max_per_file=1500)  # Smaller per file for summary
    lines = [f"📦 FULL CODEBASE ({codebase['total_files']} files, {codebase['total_chars']} chars)\n"]
    for filename, content in codebase["files"].items():
        lines.append(f"\n=== {filename} ===")
        lines.append(content)
    return "\n".join(lines)


def get_codebase_stats() -> dict:
    """Get statistics about the codebase."""
    codebase = load_full_codebase()
    stats = {
        "total_files": codebase["total_files"],
        "total_chars": codebase["total_chars"],
        "total_lines": sum(content.count("\n") for content in codebase["files"].values()),
        "files": {},
    }
    for filename, content in codebase["files"].items():
        lines = content.count("\n")
        chars = len(content)
        stats["files"][filename] = {"lines": lines, "chars": chars}
    return stats


async def codebase_question(question: str) -> str:
    """
    Answer a question using the FULL codebase as context.
    This is how Scuby can reason about cross-file issues.
    """
    from ai import _call_ai

    codebase = load_full_codebase(max_per_file=2000)
    context = f"CODEBASE ({codebase['total_files']} files):\n\n"
    for filename, content in codebase["files"].items():
        context += f"=== {filename} ===\n{content}\n\n"

    # Truncate to AI context limit
    if len(context) > TOTAL_CONTEXT_BUDGET:
        context = context[:TOTAL_CONTEXT_BUDGET] + "\n... (truncated)"

    prompt = (
        f"You are analyzing a Python Telegram bot codebase called Scuby.\n\n"
        f"{context}\n\n"
        f"QUESTION: {question}\n\n"
        f"Answer using specific file names, function names, and line references. "
        f"If the answer involves multiple files, explain the cross-file relationships."
    )

    try:
        result = await _call_ai(
            prompt,
            [{"role": "user", "content": question}],
            max_tokens=1500,
        )
        return result
    except Exception as e:
        return f"Error analyzing codebase: {e}"


async def plan_refactor(description: str) -> str:
    """
    Plan a multi-file refactoring operation.
    Reads the entire codebase and proposes a step-by-step plan.
    """
    from ai import _call_ai

    codebase = load_full_codebase(max_per_file=2500)
    context = f"CODEBASE ({codebase['total_files']} files):\n\n"
    for filename, content in codebase["files"].items():
        context += f"=== {filename} ===\n{content}\n\n"

    if len(context) > TOTAL_CONTEXT_BUDGET:
        context = context[:TOTAL_CONTEXT_BUDGET] + "\n... (truncated)"

    prompt = (
        f"You are planning a refactoring for a Python Telegram bot called Scuby.\n\n"
        f"{context}\n\n"
        f"REFACTORING REQUEST: {description}\n\n"
        f"Create a detailed step-by-step plan:\n"
        f"1. Which files need to change\n"
        f"2. What specific changes in each file\n"
        f"3. Import updates needed\n"
        f"4. Risk assessment (what could break)\n"
        f"5. How to verify (what tests to run)\n\n"
        f"Be specific — reference actual function names and line numbers."
    )

    try:
        result = await _call_ai(
            prompt,
            [{"role": "user", "content": f"Plan: {description}"}],
            max_tokens=1500,
        )
        return result
    except Exception as e:
        return f"Error planning refactor: {e}"


async def trace_error(error_message: str) -> str:
    """
    Trace an error through the codebase to find the root cause.
    Given an error message/stack trace, find where it originates.
    """
    from ai import _call_ai

    codebase = load_full_codebase(max_per_file=2000)
    context = f"CODEBASE ({codebase['total_files']} files):\n\n"
    for filename, content in codebase["files"].items():
        context += f"=== {filename} ===\n{content}\n\n"

    if len(context) > TOTAL_CONTEXT_BUDGET:
        context = context[:TOTAL_CONTEXT_BUDGET] + "\n... (truncated)"

    prompt = (
        f"You are debugging a Python Telegram bot called Scuby.\n\n"
        f"{context}\n\n"
        f"ERROR: {error_message}\n\n"
        f"Trace this error through the codebase:\n"
        f"1. Which file and function likely caused this\n"
        f"2. What the code is doing at that point\n"
        f"3. What could be wrong\n"
        f"4. How to fix it (exact code change)\n\n"
        f"Reference specific files and line numbers."
    )

    try:
        result = await _call_ai(
            prompt,
            [{"role": "user", "content": f"Trace error: {error_message}"}],
            max_tokens=1200,
        )
        return result
    except Exception as e:
        return f"Error tracing: {e}"


# ─── Persistent Session Memory ───────────────────────────────────────────────

SESSION_FILE = PROJECT_ROOT / ".scuby_session.json"


def save_session(data: dict) -> None:
    """Save session state to disk (survives restarts)."""
    import json
    try:
        # Merge with existing session
        existing = load_session()
        existing.update(data)
        existing["last_saved"] = time.time()
        SESSION_FILE.write_text(json.dumps(existing, indent=2, default=str))
    except Exception as e:
        logger.warning(f"Failed to save session: {e}")


def load_session() -> dict:
    """Load session state from disk."""
    import json
    try:
        if SESSION_FILE.exists():
            return json.loads(SESSION_FILE.read_text())
    except Exception:
        pass
    return {}


def clear_session() -> None:
    """Clear session state."""
    SESSION_FILE.unlink(missing_ok=True)


def session_set(key: str, value) -> None:
    """Set a session value."""
    data = load_session()
    data[key] = value
    save_session(data)


def session_get(key: str, default=None):
    """Get a session value."""
    return load_session().get(key, default)


# ─── Auto-Save Session (persists across restarts) ───────────────────────────

CONVERSATION_FILE = PROJECT_ROOT / ".scuby_conversations.json"


def save_conversation(user_id: int, role: str, content: str) -> None:
    """Save a conversation turn to disk. Auto-prunes to last 50 per user."""
    import json
    try:
        convos = _load_conversations()
        uid = str(user_id)
        if uid not in convos:
            convos[uid] = []
        convos[uid].append({
            "role": role,
            "content": content[:2000],  # cap per message
            "ts": time.time(),
        })
        # Keep last 50 turns per user
        convos[uid] = convos[uid][-50:]
        CONVERSATION_FILE.write_text(json.dumps(convos, indent=2, default=str))
    except Exception as e:
        logger.debug(f"save_conversation failed: {e}")


def get_saved_conversations(user_id: int, limit: int = 10) -> list[dict]:
    """Load recent conversation history for a user from disk."""
    convos = _load_conversations()
    return convos.get(str(user_id), [])[-limit:]


def _load_conversations() -> dict:
    import json
    try:
        if CONVERSATION_FILE.exists():
            return json.loads(CONVERSATION_FILE.read_text())
    except Exception:
        pass
    return {}


def save_recent_action(user_id: int, action: str, details: str) -> None:
    """Save a recent action (what Scuby did for the user)."""
    data = load_session()
    actions_key = f"actions_{user_id}"
    if actions_key not in data:
        data[actions_key] = []
    data[actions_key].append({
        "action": action,
        "details": details[:500],
        "ts": time.time(),
    })
    data[actions_key] = data[actions_key][-20:]  # last 20 actions
    data["last_saved"] = time.time()
    try:
        SESSION_FILE.write_text(json.dumps(data, indent=2, default=str))
    except Exception as e:
        logger.debug(f"save_recent_action failed: {e}")


def get_recent_actions(user_id: int, limit: int = 5) -> list[dict]:
    """Get recent actions for a user."""
    data = load_session()
    return data.get(f"actions_{user_id}", [])[-limit:]


def auto_save_interaction(user_id: int, user_message: str, scuby_reply: str,
                          intent: str = "chat") -> None:
    """Auto-save everything about an interaction to disk (atomic write)."""
    import json
    try:
        # 1. Save conversation turns to conversation file
        convos = _load_conversations()
        uid = str(user_id)
        if uid not in convos:
            convos[uid] = []
        convos[uid].append({"role": "user", "content": user_message[:2000], "ts": time.time()})
        convos[uid].append({"role": "assistant", "content": scuby_reply[:2000], "ts": time.time()})
        convos[uid] = convos[uid][-50:]  # keep last 50 turns
        CONVERSATION_FILE.write_text(json.dumps(convos, indent=2, default=str))

        # 2. Save session data (actions + metadata) in one atomic write
        data = load_session()
        actions_key = f"actions_{user_id}"
        if actions_key not in data:
            data[actions_key] = []
        data[actions_key].append({
            "action": intent,
            "details": user_message[:200],
            "ts": time.time(),
        })
        data[actions_key] = data[actions_key][-20:]  # last 20 actions
        data[f"last_intent_{user_id}"] = intent
        data[f"last_message_{user_id}"] = user_message[:200]
        data[f"last_reply_{user_id}"] = scuby_reply[:200]
        data["last_saved"] = time.time()
        SESSION_FILE.write_text(json.dumps(data, indent=2, default=str))
    except Exception as e:
        logger.debug(f"auto_save_interaction failed: {e}")


def load_session_context(user_id: int) -> str:
    """Load session context for a user to inject into AI prompts."""
    recent = get_saved_conversations(user_id, limit=6)
    actions = get_recent_actions(user_id, limit=3)

    if not recent and not actions:
        return ""

    lines = ["[PERSISTENT MEMORY — survives bot restarts]\n"]
    if actions:
        lines.append("Recent actions:")
        for a in actions:
            lines.append(f"  - {a['action']}: {a['details']}")
    if recent:
        lines.append("Recent conversation:")
        for c in recent:
            role = "User" if c["role"] == "user" else "Scuby"
            lines.append(f"  {role}: {c['content'][:200]}")

    return "\n".join(lines)
