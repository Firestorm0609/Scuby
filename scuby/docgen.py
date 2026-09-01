"""
docgen.py — Documentation generator for Scuby.

Lets Scuby:
  - Generate docstrings for Python files
  - Create README files
  - Generate API documentation
  - Create inline comments
  - Build a project overview

Uses AI to generate human-readable docs from source code.
"""

import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.resolve()


async def generate_file_docs(filename: str) -> str:
    """Generate documentation for a Python file using AI."""
    from self_improve import read_file, CORE_FILES

    if filename not in CORE_FILES:
        return f"Error: {filename} is not a known Scuby file"

    content = read_file(filename)
    if content.startswith("Error"):
        return content

    from ai import _call_ai

    prompt = (
        f"You are documenting a Python file for a Telegram crypto bot called Scuby.\n\n"
        f"FILE: {filename}\n"
        f"CODE:\n{content[:6000]}\n\n"
        f"Generate documentation in this format:\n"
        f"1. Module overview (2-3 sentences)\n"
        f"2. Key functions/classes with descriptions\n"
        f"3. Usage examples\n"
        f"4. Dependencies\n\n"
        f"Keep it concise. Use markdown."
    )

    try:
        result = await _call_ai(
            prompt,
            [{"role": "user", "content": f"Document {filename}"}],
            max_tokens=1000,
        )
        return result
    except Exception as e:
        return f"Error generating docs: {e}"


async def generate_readme() -> str:
    """Generate a README.md for the project."""
    from self_improve import CORE_FILES, read_file

    # Gather file summaries
    summaries = []
    for filename, description in CORE_FILES.items():
        content = read_file(filename)
        lines_count = len(content.splitlines()) if not content.startswith("Error") else 0
        summaries.append(f"- **{filename}** ({lines_count} lines) — {description}")

    file_list = "\n".join(summaries)

    from ai import _call_ai

    prompt = (
        f"You are writing a README.md for a Telegram crypto bot called Scuby.\n\n"
        f"PROJECT FILES:\n{file_list}\n\n"
        f"Write a README with:\n"
        f"1. Project name and tagline\n"
        f"2. Features list\n"
        f"3. File structure\n"
        f"4. Setup instructions\n"
        f"5. Commands list\n\n"
        f"Keep it clean and professional."
    )

    try:
        result = await _call_ai(
            prompt,
            [{"role": "user", "content": "Generate README"}],
            max_tokens=1500,
        )
        return result
    except Exception as e:
        return f"Error generating README: {e}"


async def generate_all_docs() -> dict:
    """Generate docs for all core files."""
    from self_improve import CORE_FILES
    results = {}
    for filename in CORE_FILES:
        docs = await generate_file_docs(filename)
        results[filename] = docs[:500]  # Cap per file
    return results


def get_function_list(filename: str) -> list[str]:
    """Extract function/class names from a Python file."""
    from self_improve import read_file

    content = read_file(filename)
    if content.startswith("Error"):
        return []

    functions = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("def ") or stripped.startswith("async def "):
            name = stripped.split("(")[0].replace("def ", "").replace("async ", "")
            functions.append(name)
        elif stripped.startswith("class "):
            name = stripped.split("(")[0].split(":")[0].replace("class ", "")
            functions.append(f"class {name}")

    return functions
