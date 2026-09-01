"""
git_ops.py — Git integration for Scuby.

Lets Scuby:
  - Show working tree status
  - View diffs (staged and unstaged)
  - View commit log
  - Create commits
  - List/create/switch branches
  - Show file blame
  - Check remote status

Safety:
  - Never force-pushes
  - Never resets hard
  - Shows diff before committing
  - Requires confirmation for commits
"""

import asyncio
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.resolve()


async def _run_git(*args: str) -> tuple[int, str, str]:
    """Run a git command and return (returncode, stdout, stderr)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(PROJECT_ROOT),
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        return (
            proc.returncode,
            stdout.decode("utf-8", errors="replace").strip(),
            stderr.decode("utf-8", errors="replace").strip(),
        )
    except asyncio.TimeoutError:
        return -1, "", "Git command timed out"
    except Exception as e:
        return -1, "", str(e)


async def git_status() -> dict:
    """Get working tree status."""
    code, out, err = await _run_git("status", "--porcelain")
    if code != 0:
        return {"error": err or "Not a git repository"}

    files = []
    for line in out.splitlines():
        if line.strip():
            status_code = line[:2].strip()
            filepath = line[3:].strip()
            files.append({"status": status_code, "file": filepath})

    # Get branch info
    _, branch, _ = await _run_git("branch", "--show-current")
    _, ahead_behind, _ = await _run_git("rev-list", "--left-right", "--count", f"HEAD...@{{u}}")

    return {
        "branch": branch or "detached",
        "files": files,
        "total": len(files),
        "ahead_behind": ahead_behind.strip() if ahead_behind else "",
    }


async def git_diff(staged: bool = False) -> str:
    """Get diff of changes."""
    args = ["diff", "--staged"] if staged else ["diff"]
    code, out, err = await _run_git(*args)
    if code != 0:
        return f"Error: {err}"
    return out or "No changes."


async def git_diff_file(filepath: str) -> str:
    """Get diff for a specific file."""
    code, out, err = await _run_git("diff", "--", filepath)
    if code != 0:
        return f"Error: {err}"
    return out or "No changes."


async def git_log(count: int = 10) -> list[dict]:
    """Get recent commit log."""
    code, out, err = await _run_git(
        "log", f"-{count}", "--pretty=format:%H|%an|%ai|%s"
    )
    if code != 0:
        return [{"error": err}]

    commits = []
    for line in out.splitlines():
        parts = line.split("|", 3)
        if len(parts) == 4:
            commits.append({
                "hash": parts[0][:8],
                "author": parts[1],
                "date": parts[2],
                "message": parts[3],
            })
    return commits


async def git_commit(message: str, files: list[str] | None = None) -> dict:
    """Create a commit. If files is None, commits all staged changes."""
    # Stage files if specified
    if files:
        for f in files:
            code, _, err = await _run_git("add", f)
            if code != 0:
                return {"success": False, "error": f"Failed to stage {f}: {err}"}

    # Check if there's anything to commit
    code, out, _ = await _run_git("diff", "--cached", "--quiet")
    if code == 0 and not files:
        return {"success": False, "error": "Nothing staged to commit"}

    # Create commit
    code, out, err = await _run_git("commit", "-m", message)
    if code != 0:
        return {"success": False, "error": err or "Commit failed"}

    # Get the new commit hash
    _, hash_out, _ = await _run_git("rev-parse", "--short", "HEAD")
    return {"success": True, "hash": hash_out.strip(), "message": message}


async def git_add(files: list[str]) -> dict:
    """Stage files."""
    added = []
    errors = []
    for f in files:
        code, _, err = await _run_git("add", f)
        if code == 0:
            added.append(f)
        else:
            errors.append(f"{f}: {err}")
    return {"added": added, "errors": errors}


async def git_branches() -> dict:
    """List branches."""
    code, out, err = await _run_git("branch", "-a")
    if code != 0:
        return {"error": err}

    current = []
    local = []
    remote = []
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("* "):
            current.append(line[2:])
        elif line.startswith("remotes/"):
            remote.append(line[8:])
        elif line:
            local.append(line)

    return {"current": current, "local": local, "remote": remote}


async def git_create_branch(name: str) -> dict:
    """Create and switch to a new branch."""
    code, _, err = await _run_git("checkout", "-b", name)
    if code != 0:
        return {"success": False, "error": err}
    return {"success": True, "branch": name}


async def git_switch_branch(name: str) -> dict:
    """Switch to an existing branch."""
    code, _, err = await _run_git("checkout", name)
    if code != 0:
        return {"success": False, "error": err}
    return {"success": True, "branch": name}


async def git_stash() -> dict:
    """Stash current changes."""
    code, out, err = await _run_git("stash")
    if code != 0:
        return {"success": False, "error": err}
    return {"success": True, "message": out}


async def git_stash_pop() -> dict:
    """Pop the latest stash."""
    code, out, err = await _run_git("stash", "pop")
    if code != 0:
        return {"success": False, "error": err}
    return {"success": True, "message": out}


async def git_blame(filepath: str) -> str:
    """Get blame for a file."""
    code, out, err = await _run_git("blame", "--line-porcelain", filepath)
    if code != 0:
        return f"Error: {err}"
    return out[:3000]  # Cap output


async def git_changelog(count: int = 20) -> str:
    """Generate a changelog from recent commits."""
    code, out, err = await _run_git(
        "log", f"-{count}", "--pretty=format:%H|%an|%ai|%s"
    )
    if code != 0:
        return f"Error: {err}"

    # Categorize commits
    features = []
    fixes = []
    other = []

    for line in out.splitlines():
        parts = line.split("|", 3)
        if len(parts) != 4:
            continue
        hash_, author, date, message = parts
        entry = f"- {message.strip()} _({hash_[:8]})_"
        msg_lower = message.lower()

        if any(w in msg_lower for w in ["add", "feat", "new", "implement", "+"]):
            features.append(entry)
        elif any(w in msg_lower for w in ["fix", "bug", "patch", "resolve", "correct"]):
            fixes.append(entry)
        else:
            other.append(entry)

    sections = []
    if features:
        sections.append("✨ *New Features:*\n" + "\n".join(features))
    if fixes:
        sections.append("🐛 *Bug Fixes:*\n" + "\n".join(fixes))
    if other:
        sections.append("📝 *Other Changes:*\n" + "\n".join(other))

    return "\n\n".join(sections) if sections else "No recent changes."
