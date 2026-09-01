"""
terminal.py — Sandboxed terminal access for Scuby.

Lets Scuby:
  - Run shell commands safely
  - Install Python packages
  - Check system status
  - Run build/test commands
  - View running processes

Safety:
  - Commands run in project directory
  - Timeout on all commands (30s default)
  - Blocks dangerous commands (rm -rf /, sudo, etc.)
  - Output capped to prevent spam
"""

import asyncio
import logging
import os
import re
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.resolve()

# Blocked command patterns (safety)
_BLOCKED_PATTERNS = [
    r'\brm\s+-rf\s+/',       # rm -rf /
    r'\bsudo\b',              # sudo
    r'\bchmod\s+777',         # chmod 777
    r'\bmkfs\b',              # format disk
    r'\bdd\b.*of=/dev/',      # dd to device
    r'\bsystemctl\b',         # systemd
    r'\bservice\b.*stop',     # stop services
    r'\bkill\s+-9\s+1\b',    # kill init
    r'\bshutdown\b',          # shutdown
    r'\breboot\b',            # reboot
    r'>\s*/dev/sd',           # write to disk
]

_BLOCKED_RE = re.compile('|'.join(_BLOCKED_PATTERNS), re.IGNORECASE)

# Dangerous file operations
_DANGEROUS_FILE_OPS = re.compile(
    r'\brm\s+(-[rf]+\s+)?(\.|\.\.|/~|/root|/home|/etc|/var|/usr)',
    re.IGNORECASE
)


def is_command_safe(command: str) -> tuple[bool, str]:
    """Check if a command is safe to run."""
    if _BLOCKED_RE.search(command):
        return False, "Command blocked for safety"
    if _DANGEROUS_FILE_OPS.search(command):
        return False, "Dangerous file operation blocked"
    return True, ""


async def run_command(
    command: str,
    timeout: int = 30,
    max_output: int = 4000,
) -> dict:
    """
    Run a shell command in the project directory.
    Returns {success, output, error, exit_code}.
    """
    safe, reason = is_command_safe(command)
    if not safe:
        return {"success": False, "output": "", "error": reason, "exit_code": -1}

    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)

        output = stdout.decode("utf-8", errors="replace").strip()
        error = stderr.decode("utf-8", errors="replace").strip()

        # Cap output
        if len(output) > max_output:
            output = output[:max_output] + f"\n... ({len(output) - max_output} chars truncated)"
        if len(error) > max_output:
            error = error[:max_output] + f"\n... (truncated)"

        return {
            "success": proc.returncode == 0,
            "output": output,
            "error": error,
            "exit_code": proc.returncode,
        }
    except asyncio.TimeoutError:
        return {"success": False, "output": "", "error": f"Command timed out after {timeout}s", "exit_code": -1}
    except Exception as e:
        return {"success": False, "output": "", "error": str(e), "exit_code": -1}


async def install_package(package: str) -> dict:
    """Install a Python package."""
    safe, reason = is_command_safe(f"pip install {package}")
    if not safe:
        return {"success": False, "error": reason}
    return await run_command(f"pip install {package}", timeout=60)


async def check_python_version() -> str:
    """Get Python version."""
    result = await run_command("python3 --version")
    return result["output"] or result["error"]


async def check_disk_usage() -> str:
    """Get disk usage."""
    result = await run_command("df -h . | tail -1")
    return result["output"]


async def check_running_processes() -> str:
    """Check if main.py is running."""
    result = await run_command("ps aux | grep 'python.*main.py' | grep -v grep")
    return result["output"] or "Not running"


async def pip_list() -> str:
    """List installed packages."""
    result = await run_command("pip list --format=columns 2>/dev/null | head -30")
    return result["output"]


async def pip_outdated() -> str:
    """Check for outdated packages."""
    result = await run_command("pip list --outdated --format=columns 2>/dev/null | head -20")
    return result["output"] or "All packages up to date"


async def test_coverage() -> dict:
    """Run tests with coverage report."""
    # First check if pytest-cov is installed
    check = await run_command("python3 -c 'import pytest_cov' 2>/dev/null && echo OK")
    if "OK" not in check["output"]:
        # Install it
        await run_command("pip install pytest-cov -q", timeout=30)

    result = await run_command(
        "python3 -m pytest test_scuby.py --cov=. --cov-report=term-missing -q 2>&1 | tail -30",
        timeout=60,
    )
    return {
        "success": result["success"],
        "output": result["output"],
        "error": result["error"],
    }


async def pip_audit() -> str:
    """Check for known security vulnerabilities in packages."""
    result = await run_command(
        "pip install pip-audit -q 2>/dev/null && pip-audit 2>/dev/null | head -30",
        timeout=60,
    )
    return result["output"] or result["error"] or "No vulnerabilities found"


async def sort_imports(filepath: str) -> str:
    """Sort imports in a Python file using isort."""
    result = await run_command(f"pip install isort -q 2>/dev/null && isort --profile black {filepath} --diff", timeout=30)
    return result["output"] or "No import changes needed"


# ─── Mypy Type Checking ──────────────────────────────────────────────────────

async def run_mypy(filepath: str = ".", strict: bool = False) -> dict:
    """Run mypy type checking on a file or the whole project.
    Returns {success, output, error, exit_code}.
    """
    # Ensure mypy is installed
    check = await run_command("python3 -c 'import mypy' 2>/dev/null && echo OK")
    if "OK" not in check["output"]:
        await run_command("pip install mypy -q", timeout=30)

    flags = "--strict" if strict else "--ignore-missing-imports --no-error-summary"
    result = await run_command(
        f"python3 -m mypy {flags} --no-color-output {filepath} 2>&1 | head -50",
        timeout=60,
    )
    return {
        "success": result["success"],
        "output": result["output"],
        "error": result["error"],
        "exit_code": result["exit_code"],
    }


# ─── Pyflakes Linting ────────────────────────────────────────────────────────

async def run_pyflakes(filepath: str = ".") -> str:
    """Run pyflakes linting for quick error detection."""
    check = await run_command("python3 -c 'import pyflakes' 2>/dev/null && echo OK")
    if "OK" not in check["output"]:
        await run_command("pip install pyflakes -q", timeout=30)

    result = await run_command(f"python3 -m pyflakes {filepath} 2>&1 | head -30", timeout=30)
    return result["output"] or "No issues found"


# ─── Full Type Check Pipeline ────────────────────────────────────────────────

async def full_type_check(filepath: str = ".") -> dict:
    """Run mypy + pyflakes and combine results."""
    mypy_result = await run_mypy(filepath)
    pyflakes_result = await run_pyflakes(filepath)

    issues = []
    if mypy_result["output"]:
        issues.append(f"🔍 Mypy:\n{mypy_result['output']}")
    if pyflakes_result and pyflakes_result != "No issues found":
        issues.append(f"🔍 Pyflakes:\n{pyflakes_result}")

    return {
        "success": mypy_result["success"] and "Error" not in pyflakes_result,
        "mypy": mypy_result,
        "pyflakes": pyflakes_result,
        "summary": "\n\n".join(issues) if issues else "✅ No type errors found!",
    }
