"""
repo.py — Repository cloning and multi-repo support for Scuby.

Lets Scuby:
  - Clone GitHub/GitLab repos
  - Switch between repos
  - List cloned repos
  - Read files from any cloned repo
  - Get repo overview (structure, languages, stats)

Safety:
  - Clones into a dedicated .repos/ directory
  - Size limit: 50MB per repo
  - Skips .git directory when reading
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.resolve()
REPOS_DIR = PROJECT_ROOT / ".repos"
ACTIVE_REPO_FILE = PROJECT_ROOT / ".active_repo"

# Max repo size in MB
MAX_REPO_SIZE_MB = 50


def _ensure_repos_dir() -> None:
    REPOS_DIR.mkdir(exist_ok=True)


def get_active_repo() -> str | None:
    """Get the currently active repo name."""
    try:
        return ACTIVE_REPO_FILE.read_text().strip() or None
    except FileNotFoundError:
        return None


def set_active_repo(name: str) -> None:
    """Set the active repo."""
    ACTIVE_REPO_FILE.write_text(name)


def get_repo_path(name: str) -> Path:
    """Get the path to a cloned repo."""
    return REPOS_DIR / name


def list_repos() -> list[dict]:
    """List all cloned repos."""
    _ensure_repos_dir()
    repos = []
    active = get_active_repo()
    for item in sorted(REPOS_DIR.iterdir()):
        if item.is_dir() and (item / ".git").exists():
            # Count files
            file_count = sum(1 for _ in item.rglob("*") if _.is_file() and ".git" not in str(_))
            # Get description if available
            desc = ""
            readme = item / "README.md"
            if readme.exists():
                try:
                    desc = readme.read_text(encoding="utf-8")[:200].strip().split("\n")[0]
                except Exception:
                    pass
            repos.append({
                "name": item.name,
                "path": str(item),
                "files": file_count,
                "active": item.name == active,
                "description": desc,
            })
    return repos


async def clone_repo(url: str, name: str | None = None) -> dict:
    """
    Clone a git repository.
    URL formats supported:
      - https://github.com/user/repo
      - git@github.com:user/repo.git
      - user/repo (shorthand for GitHub)
    """
    _ensure_repos_dir()

    # Normalize URL
    if not url.startswith(("http", "git@")):
        # Shorthand: user/repo → https://github.com/user/repo
        url = f"https://github.com/{url}"

    # Extract name from URL if not provided
    if not name:
        name = url.rstrip("/").split("/")[-1].replace(".git", "")

    # Sanitize name
    name = re.sub(r'[^a-zA-Z0-9._-]', '_', name)

    target = REPOS_DIR / name
    if target.exists():
        return {"success": False, "error": f"Repo '{name}' already exists. Use /repo switch {name}"}

    logger.info(f"Cloning {url} → {target}")

    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "clone", "--depth=1", url, str(target),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)

        if proc.returncode != 0:
            error = stderr.decode("utf-8", errors="replace").strip()
            return {"success": False, "error": error}

        # Check size
        size_result = await asyncio.create_subprocess_exec(
            "du", "-sm", str(target),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        size_out, _ = await size_result.communicate()
        size_mb = int(size_out.decode().split()[0]) if size_out else 0

        if size_mb > MAX_REPO_SIZE_MB:
            # Too big, remove it
            import shutil
            shutil.rmtree(target)
            return {"success": False, "error": f"Repo too large ({size_mb}MB, max {MAX_REPO_SIZE_MB}MB)"}

        # Count files
        file_count = sum(1 for _ in target.rglob("*") if _.is_file() and ".git" not in str(_))

        return {
            "success": True,
            "name": name,
            "path": str(target),
            "size_mb": size_mb,
            "files": file_count,
        }
    except asyncio.TimeoutError:
        return {"success": False, "error": "Clone timed out after 2 minutes"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def delete_repo(name: str) -> dict:
    """Delete a cloned repo."""
    import shutil
    target = REPOS_DIR / name
    if not target.exists():
        return {"success": False, "error": f"Repo '{name}' not found"}
    if not (target / ".git").exists():
        return {"success": False, "error": f"'{name}' is not a git repo"}

    shutil.rmtree(target)
    if get_active_repo() == name:
        ACTIVE_REPO_FILE.unlink(missing_ok=True)
    return {"success": True, "name": name}


async def repo_overview(name: str | None = None) -> dict:
    """Get overview of a repo: structure, languages, stats."""
    repo_name = name or get_active_repo()
    if not repo_name:
        return {"error": "No repo specified and no active repo. Use /repo list first."}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {"error": f"Repo '{repo_name}' not found"}

    # File structure (top 2 levels)
    structure = []
    for item in sorted(repo_path.iterdir()):
        if item.name.startswith(".") or item.name == "__pycache__":
            continue
        if item.is_dir():
            sub_items = [s.name for s in sorted(item.iterdir())[:5] if not s.name.startswith(".")]
            structure.append(f"📁 {item.name}/ ({', '.join(sub_items)}{'...' if len(list(item.iterdir())) > 5 else ''})")
        else:
            structure.append(f"📄 {item.name}")

    # Language detection
    extensions = {}
    for f in repo_path.rglob("*"):
        if f.is_file() and ".git" not in str(f):
            ext = f.suffix.lower()
            if ext:
                extensions[ext] = extensions.get(ext, 0) + 1

    lang_map = {
        ".py": "Python", ".js": "JavaScript", ".ts": "TypeScript",
        ".rs": "Rust", ".go": "Go", ".sol": "Solidity",
        ".html": "HTML", ".css": "CSS", ".json": "JSON",
        ".md": "Markdown", ".yaml": "YAML", ".yml": "YAML",
        ".toml": "TOML", ".sh": "Shell", ".sql": "SQL",
    }
    languages = []
    for ext, count in sorted(extensions.items(), key=lambda x: -x[1])[:5]:
        lang = lang_map.get(ext, ext)
        languages.append(f"{lang} ({count})")

    # Git info
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", str(repo_path), "log", "--oneline", "-5",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        recent_commits = stdout.decode().strip().splitlines()
    except Exception:
        recent_commits = []

    # Total size
    try:
        proc = await asyncio.create_subprocess_exec(
            "du", "-sh", str(repo_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        size = stdout.decode().split()[0]
    except Exception:
        size = "?"

    return {
        "name": repo_name,
        "structure": structure[:15],
        "languages": languages,
        "recent_commits": recent_commits[:5],
        "size": size,
    }


def read_repo_file(repo_name: str, filepath: str) -> str:
    """Read a file from a cloned repo."""
    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return f"Error: Repo '{repo_name}' not found"

    file_path = repo_path / filepath
    if not file_path.exists():
        return f"Error: {filepath} not found in {repo_name}"
    if not file_path.resolve().is_relative_to(repo_path):
        return "Error: cannot read outside repo"

    try:
        return file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return f"Error: {e}"
