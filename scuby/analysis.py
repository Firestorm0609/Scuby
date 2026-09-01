"""
analysis.py — Code analysis engine for Scuby.

Lets Scuby:
  - Map import dependencies between files
  - Find circular dependencies
  - Detect dead/unused code
  - Audit environment variables
  - Validate configuration
  - Profile function performance
"""

import ast
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.resolve()


# ─── Import / Dependency Analysis ─────────────────────────────────────────────

def analyze_imports(filename: str) -> dict:
    """Analyze imports in a file. Returns what it imports and what imports it."""
    from self_improve import read_file, CORE_FILES

    content = read_file(filename)
    if content.startswith("Error"):
        return {"error": content}

    imports = []
    local_imports = []

    try:
        tree = ast.parse(content)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                imports.append(module)
                # Check if it's a local import
                if module in CORE_FILES or module.replace(".py", "") in CORE_FILES:
                    local_imports.append(module)
    except SyntaxError:
        # Fallback: regex-based import detection
        for line in content.splitlines():
            m = re.match(r'^(?:from|import)\s+(\S+)', line.strip())
            if m:
                imports.append(m.group(1))

    # Find who imports this file
    imported_by = []
    for other_file in CORE_FILES:
        if other_file == filename:
            continue
        other_content = read_file(other_file)
        if other_content.startswith("Error"):
            continue
        module_name = filename.replace(".py", "")
        if re.search(rf'\bfrom\s+{re.escape(module_name)}\b|\bimport\s+{re.escape(module_name)}\b', other_content):
            imported_by.append(other_file)

    return {
        "file": filename,
        "imports": imports,
        "local_imports": local_imports,
        "imported_by": imported_by,
    }


def find_circular_deps() -> list[list[str]]:
    """Find circular dependency chains."""
    from self_improve import CORE_FILES

    # Build dependency graph
    graph: dict[str, set[str]] = {}
    for filename in CORE_FILES:
        info = analyze_imports(filename)
        deps = set()
        for imp in info.get("local_imports", []):
            clean = imp.replace(".py", "")
            if clean in CORE_FILES:
                deps.add(clean)
        graph[filename.replace(".py", "")] = deps

    # DFS to find cycles
    cycles = []
    visited = set()
    path = []

    def dfs(node):
        if node in path:
            cycle = path[path.index(node):] + [node]
            cycles.append(cycle)
            return
        if node in visited:
            return
        visited.add(node)
        path.append(node)
        for neighbor in graph.get(node, set()):
            dfs(neighbor)
        path.pop()

    for node in graph:
        dfs(node)

    return cycles


def get_import_graph() -> str:
    """Get a text representation of the dependency graph."""
    from self_improve import CORE_FILES

    lines = ["📦 Import Graph:\n"]
    for filename in sorted(CORE_FILES.keys()):
        info = analyze_imports(filename)
        local = info.get("local_imports", [])
        if local:
            lines.append(f"  {filename} → {', '.join(local)}")

    cycles = find_circular_deps()
    if cycles:
        lines.append("\n⚠️ Circular dependencies:")
        for cycle in cycles:
            lines.append(f"  {' → '.join(cycle)}")
    else:
        lines.append("\n✅ No circular dependencies found")

    return "\n".join(lines)


# ─── Dead Code Detection ─────────────────────────────────────────────────────

def find_dead_code(filename: str) -> list[dict]:
    """Find functions defined in a file but never called elsewhere."""
    from self_improve import read_file, CORE_FILES

    content = read_file(filename)
    if content.startswith("Error"):
        return [{"error": content}]

    # Extract function definitions
    functions = []
    try:
        tree = ast.parse(content)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = node.name
                if not name.startswith("_") or name.startswith("__"):
                    continue  # Skip private functions (they may be used internally)
                functions.append({
                    "name": name,
                    "line": node.lineno,
                    "is_async": isinstance(node, ast.AsyncFunctionDef),
                })
    except SyntaxError:
        return []

    if not functions:
        return []

    # Check if each function is referenced anywhere
    dead = []
    all_code = ""
    for f in CORE_FILES:
        c = read_file(f)
        if not c.startswith("Error"):
            all_code += c

    for func in functions:
        name = func["name"]
        # Count references (excluding the definition itself)
        refs = len(re.findall(rf'\b{re.escape(name)}\b', all_code))
        if refs <= 1:  # Only the definition itself
            dead.append({
                "name": name,
                "line": func["line"],
                "file": filename,
                "is_async": func["is_async"],
            })

    return dead


# ─── Environment Variable Audit ──────────────────────────────────────────────

def audit_env_vars() -> dict:
    """Scan all files for environment variable usage and check which are set."""
    from self_improve import read_file, CORE_FILES

    env_vars_used = set()
    env_vars_pattern = re.compile(
        r'os\.environ\.get\(["\'](\w+)["\']|'
        r'os\.environ\[(["\'])(\w+)\]|'
        r'os\.getenv\(["\'](\w+)["\']'
    )

    for filename in CORE_FILES:
        content = read_file(filename)
        if content.startswith("Error"):
            continue
        for match in env_vars_pattern.finditer(content):
            var_name = match.group(1) or match.group(3) or match.group(4)
            if var_name:
                env_vars_used.add(var_name)

    # Check which are set
    env_status = {}
    for var in sorted(env_vars_used):
        value = os.environ.get(var)
        if value:
            # Mask the value for security
            masked = value[:4] + "..." + value[-4:] if len(value) > 8 else "***"
            env_status[var] = {"set": True, "masked": masked}
        else:
            env_status[var] = {"set": False, "masked": ""}

    return {
        "total": len(env_vars_used),
        "set": sum(1 for v in env_status.values() if v["set"]),
        "missing": sum(1 for v in env_status.values() if not v["set"]),
        "vars": env_status,
    }


# ─── Config Validator ────────────────────────────────────────────────────────

def validate_config() -> list[dict]:
    """Validate project configuration files."""
    issues = []

    # Check .env
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        issues.append({"file": ".env", "issue": "Missing", "severity": "error"})
    else:
        required = ["TELEGRAM_BOT_TOKEN"]
        optional = ["GROQ_API_KEY", "CEREBRAS_API_KEY", "GEMINI_API_KEY", "OPENROUTER_API_KEY"]
        content = env_path.read_text()
        for var in required:
            if var not in content:
                issues.append({"file": ".env", "issue": f"Missing required: {var}", "severity": "error"})
        for var in optional:
            if var not in content:
                issues.append({"file": ".env", "issue": f"Missing optional: {var}", "severity": "warning"})

    # Check requirements.txt
    req_path = PROJECT_ROOT / "requirements.txt"
    if not req_path.exists():
        issues.append({"file": "requirements.txt", "issue": "Missing", "severity": "error"})
    else:
        content = req_path.read_text()
        required_pkgs = ["python-telegram-bot", "httpx"]
        for pkg in required_pkgs:
            if pkg not in content:
                issues.append({"file": "requirements.txt", "issue": f"Missing required: {pkg}", "severity": "error"})

    # Check .gitignore
    gitignore = PROJECT_ROOT / ".gitignore"
    if not gitignore.exists():
        issues.append({"file": ".gitignore", "issue": "Missing", "severity": "warning"})
    else:
        content = gitignore.read_text()
        if ".env" not in content:
            issues.append({"file": ".gitignore", "issue": ".env not ignored (security risk!)", "severity": "error"})

    # Check if test file exists
    test_path = PROJECT_ROOT / "test_scuby.py"
    if not test_path.exists():
        issues.append({"file": "test_scuby.py", "issue": "Missing test file", "severity": "warning"})

    return issues


# ─── Performance Profiling ───────────────────────────────────────────────────

async def profile_function(func_name: str, runs: int = 5) -> dict:
    """Profile a specific function by running it multiple times."""
    import time

    # Find and import the function
    from self_improve import CORE_FILES
    for filename in CORE_FILES:
        try:
            module_name = filename.replace(".py", "")
            mod = __import__(module_name)
            func = getattr(mod, func_name, None)
            if func and callable(func):
                times = []
                for _ in range(runs):
                    start = time.monotonic()
                    try:
                        if asyncio.iscoroutinefunction(func):
                            await func()
                        else:
                            func()
                    except Exception:
                        pass
                    elapsed = time.monotonic() - start
                    times.append(elapsed)

                return {
                    "function": func_name,
                    "module": filename,
                    "runs": runs,
                    "avg_ms": round(sum(times) / len(times) * 1000, 2),
                    "min_ms": round(min(times) * 1000, 2),
                    "max_ms": round(max(times) * 1000, 2),
                }
        except Exception:
            continue

    return {"error": f"Function '{func_name}' not found"}
