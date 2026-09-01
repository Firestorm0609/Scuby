"""
self_improve.py — Scuby's self-awareness and self-healing module.

Lets Scuby:
  - Read its own source files
  - Run its test suite
  - Diagnose and fix bugs in its own code
  - Preview changes before applying
  - Track what's been modified

Safety:
  - Only touches files in the project directory
  - Creates backups before any modification
  - Runs tests before confirming a fix
  - Shows diffs before applying
"""

import asyncio
import difflib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Project root (where this file lives)
PROJECT_ROOT = Path(__file__).parent.resolve()

# Core Scuby files (what Scuby knows about itself)
CORE_FILES = {
    "main.py":         "Entry point — wires handlers, jobs, lifecycle",
    "ai.py":           "AI brain — providers, chat, intent, code gen, web search",
    "handlers.py":     "Telegram command handlers and callbacks",
    "handlers_ai_addition.py": "AI commands — /ask, /code, /analyze, /smartwatch",
    "utils.py":        "Shared helpers, persistence, API calls, formatters",
    "gemscore.py":     "GemScore token scoring engine",
    "memory.py":       "Long-term learning — user memory, token perf, patterns",
    "feeds.py":        "Momentum feed scanner",
    "smart_filters.py": "Natural-language launch filters",
    "proactive.py":    "Rug watchdog, whale alerts",
    "jobs.py":         "Background jobs — alerts, monitors, watches",
    "wallet_tracker.py": "Whale wallet tracker",
    "portfolio.py":    "Portfolio tracker and daily briefings",
    "reminders.py":    "Reminder system",
    "solscan.py":      "On-chain token creation time lookup",
    "pair_cache.py":   "Shared DexScreener pair pool cache",
    "rate_limiter.py": "Shared DexScreener rate limiter",
    "self_improve.py": "This file — self-awareness module",
}

# Backup directory
BACKUP_DIR = PROJECT_ROOT / ".backups"


# ─── File Operations ─────────────────────────────────────────────────────────

def read_file(filepath: str) -> str:
    """Read a file in the project directory. Returns content or error string."""
    try:
        path = PROJECT_ROOT / filepath
        if not path.exists():
            return f"Error: {filepath} not found"
        if not path.resolve().is_relative_to(PROJECT_ROOT):
            return "Error: cannot read files outside project directory"
        return path.read_text(encoding="utf-8")
    except Exception as e:
        return f"Error reading {filepath}: {e}"


def read_file_lines(filepath: str, start: int = 1, end: int | None = None) -> str:
    """Read specific line range from a file."""
    content = read_file(filepath)
    if content.startswith("Error"):
        return content
    lines = content.splitlines()
    if end is None:
        end = min(start + 100, len(lines))
    selected = lines[max(0, start-1):end]
    numbered = [f"{i+start:4d} | {line}" for i, line in enumerate(selected)]
    return "\n".join(numbered)


def write_file(filepath: str, content: str, backup: bool = True) -> str:
    """
    Write content to a file in the project directory.
    Creates a backup first if backup=True.
    Returns diff preview or error string.
    """
    try:
        path = PROJECT_ROOT / filepath
        if not path.resolve().is_relative_to(PROJECT_ROOT):
            return "Error: cannot write files outside project directory"

        old_content = path.read_text(encoding="utf-8") if path.exists() else ""

        # Create backup
        if backup and path.exists():
            _create_backup(filepath, old_content)

        # Write new content
        path.write_text(content, encoding="utf-8")

        # Generate diff
        diff = _make_diff(old_content, content, filepath)
        return diff
    except Exception as e:
        return f"Error writing {filepath}: {e}"


def replace_in_file(filepath: str, old: str, new: str, backup: bool = True) -> str:
    """Replace a string in a file. Returns diff preview."""
    content = read_file(filepath)
    if content.startswith("Error"):
        return content
    if old not in content:
        return f"Error: pattern not found in {filepath}"
    new_content = content.replace(old, new, 1)
    return write_file(filepath, new_content, backup=backup)


# ─── Backup System ───────────────────────────────────────────────────────────

def _create_backup(filepath: str, content: str) -> None:
    """Create a timestamped backup of a file."""
    BACKUP_DIR.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = filepath.replace("/", "__").replace("\\", "__")
    backup_path = BACKUP_DIR / f"{ts}_{safe_name}"
    backup_path.write_text(content, encoding="utf-8")

    # Prune old backups (keep last 20 per file)
    backups = sorted(BACKUP_DIR.glob(f"*_{safe_name}"))
    for old in backups[:-20]:
        old.unlink(missing_ok=True)


def list_backups(filepath: str | None = None) -> list[str]:
    """List available backups."""
    if not BACKUP_DIR.exists():
        return []
    if filepath:
        safe_name = filepath.replace("/", "__").replace("\\", "__")
        backups = sorted(BACKUP_DIR.glob(f"*_{safe_name}"))
    else:
        backups = sorted(BACKUP_DIR.glob("*"))
    return [b.name for b in backups[-20:]]


# ─── Diff Engine ─────────────────────────────────────────────────────────────

def _make_diff(old: str, new: str, filename: str) -> str:
    """Generate a unified diff between old and new content."""
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)
    diff = list(difflib.unified_diff(
        old_lines, new_lines,
        fromfile=f"a/{filename}",
        tofile=f"b/{filename}",
        lineterm="",
    ))
    if not diff:
        return "No changes detected."
    return "".join(diff)


def preview_diff(filepath: str, new_content: str) -> str:
    """Preview what a write would change without actually writing."""
    old = read_file(filepath)
    if old.startswith("Error"):
        return old
    return _make_diff(old, new_content, filepath)


# ─── Test Runner ─────────────────────────────────────────────────────────────

async def run_tests(test_file: str = "test_scuby.py") -> dict:
    """
    Run pytest and return results.
    Returns {success: bool, output: str, passed: int, failed: int, errors: str}
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "python3", "-m", "pytest", test_file, "-q", "--tb=short",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(PROJECT_ROOT),
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        output = stdout.decode("utf-8", errors="replace")
        errors = stderr.decode("utf-8", errors="replace")

        # Parse results
        passed = 0
        failed = 0
        for line in output.splitlines():
            if "passed" in line:
                parts = line.split()
                for i, p in enumerate(parts):
                    if p == "passed":
                        passed = int(parts[i-1])
            if "failed" in line:
                parts = line.split()
                for i, p in enumerate(parts):
                    if p == "failed":
                        failed = int(parts[i-1])

        return {
            "success": proc.returncode == 0,
            "output": output.strip(),
            "passed": passed,
            "failed": failed,
            "errors": errors.strip() if errors.strip() else "",
        }
    except asyncio.TimeoutError:
        return {"success": False, "output": "Tests timed out after 60s", "passed": 0, "failed": 0, "errors": "timeout"}
    except Exception as e:
        return {"success": False, "output": str(e), "passed": 0, "failed": 0, "errors": str(e)}


# ─── File Status ─────────────────────────────────────────────────────────────

def get_project_status() -> dict:
    """Get status of all core Scuby files."""
    status = {}
    for filename, description in CORE_FILES.items():
        path = PROJECT_ROOT / filename
        if path.exists():
            stat = path.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            status[filename] = {
                "exists": True,
                "size": stat.st_size,
                "modified": mtime.strftime("%Y-%m-%d %H:%M UTC"),
                "description": description,
            }
        else:
            status[filename] = {
                "exists": False,
                "size": 0,
                "modified": None,
                "description": description,
            }
    return status


def get_recent_changes(days: int = 1) -> list[dict]:
    """Get files modified in the last N days."""
    cutoff = time.time() - (days * 86400)
    changes = []
    for filename in CORE_FILES:
        path = PROJECT_ROOT / filename
        if path.exists() and path.stat().st_mtime > cutoff:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            changes.append({
                "file": filename,
                "modified": mtime.strftime("%H:%M UTC"),
                "size": path.stat().st_size,
            })
    return sorted(changes, key=lambda x: x["modified"], reverse=True)


# ─── Codebase Search ─────────────────────────────────────────────────────────

def search_code(pattern: str, files: list[str] | None = None) -> list[dict]:
    """Search for a pattern across project files."""
    import re
    results = []
    targets = files or list(CORE_FILES.keys())

    for filename in targets:
        content = read_file(filename)
        if content.startswith("Error"):
            continue
        for i, line in enumerate(content.splitlines(), 1):
            if re.search(pattern, line, re.IGNORECASE):
                results.append({
                    "file": filename,
                    "line": i,
                    "content": line.strip()[:120],
                })
    return results[:50]


# ─── Health Monitoring ────────────────────────────────────────────────────────

async def check_health() -> dict:
    """Check if the bot process is running and healthy."""
    import subprocess

    # Check if main.py is running
    result = await asyncio.create_subprocess_exec(
        "ps", "aux",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await result.communicate()
    processes = stdout.decode()

    bot_running = "python" in processes and "main.py" in processes

    # Check disk space
    disk = await asyncio.create_subprocess_exec(
        "df", "-h", ".",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    disk_out, _ = await disk.communicate()
    disk_lines = disk_out.decode().splitlines()
    disk_info = disk_lines[1] if len(disk_lines) > 1 else "unknown"

    # Check Python packages
    pip_check = await asyncio.create_subprocess_exec(
        "python3", "-c", "import telegram; import httpx; print('OK')",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    pip_out, pip_err = await pip_check.communicate()
    deps_ok = pip_out.decode().strip() == "OK"

    # Check .env
    env_exists = (PROJECT_ROOT / ".env").exists()

    # Check test suite
    test_result = await run_tests()

    return {
        "bot_running": bot_running,
        "disk": disk_info,
        "deps_ok": deps_ok,
        "env_configured": env_exists,
        "tests_passing": test_result["success"],
        "tests_passed": test_result["passed"],
        "tests_failed": test_result["failed"],
    }


# ─── Performance Profiling ───────────────────────────────────────────────────

_profile_data: dict[str, list[float]] = {}


def profile_start(label: str) -> None:
    """Start timing a labeled operation."""
    import time
    _profile_data.setdefault(label, []).append(time.monotonic())


def profile_end(label: str) -> float | None:
    """End timing and return elapsed seconds."""
    import time
    starts = _profile_data.get(label, [])
    if not starts:
        return None
    start = starts.pop()
    elapsed = time.monotonic() - start
    return elapsed


def get_profile_report() -> dict[str, dict]:
    """Get profiling statistics."""
    report = {}
    for label, times in _profile_data.items():
        if times:
            # These are incomplete (start without end)
            report[label] = {"pending": len(times)}
    return report


async def benchmark_command(command: str, runs: int = 3) -> dict:
    """Benchmark a command by running it multiple times."""
    import time
    times = []
    for _ in range(runs):
        start = time.monotonic()
        result = await run_command(command, timeout=30)
        elapsed = time.monotonic() - start
        times.append(elapsed)

    avg = sum(times) / len(times) if times else 0
    return {
        "command": command,
        "runs": runs,
        "avg_seconds": round(avg, 3),
        "min_seconds": round(min(times), 3) if times else 0,
        "max_seconds": round(max(times), 3) if times else 0,
        "success": result["success"],
    }
