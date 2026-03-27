"""
rename_to_scuby.py — Renames Scooby → Scuby across all Scooby bot files.

Run from your project root:
    python rename_to_scuby.py

Replacements (order matters — longest strings first):
    Scooby-Dooby-Doo  →  Scuby-Duby-Doo
    Scooby-Doo        →  Scuby-Doo
    Scooby            →  Scuby
    scooby            →  scuby
    SCOOBY            →  SCUBY

Backs up every modified file as <file>.bak before changing it.
"""

import os
import shutil

ROOT = os.path.dirname(os.path.abspath(__file__))

# Files to patch (add any new .py files you create later)
TARGET_FILES = [
    "ai.py",
    "feeds.py",
    "gemscore.py",
    "handlers.py",
    "handlers_ai_addition.py",
    "jobs.py",
    "main.py",
    "memory.py",
    "portfolio.py",
    "proactive.py",
    "rate_limiter.py",
    "reminder.py",
    "reminders.py",
    "smart_filters.py",
    "solana_seed_data.py",
    "utils.py",
    "wallet_tracker.py",
    "fixes.py",
    "patch_rate_limits.py",
    "install_scooby_upgrades.sh",
    "README.md",
]

# Order matters: longest / most specific strings first
REPLACEMENTS = [
    # Catchphrases with the name embedded
    ("Scooby-Dooby-Doo",  "Scuby-Duby-Doo"),
    ("Scooby-Doo",        "Scuby-Doo"),
    ("scooby-dooby-doo",  "scuby-duby-doo"),
    ("scooby-doo",        "scuby-doo"),
    ("SCOOBY-DOO",        "SCUBY-DOO"),

    # Plain name variants
    ("Scooby",            "Scuby"),
    ("scooby",            "scuby"),
    ("SCOOBY",            "SCUBY"),

    # README / shell script reference
    ("Scooby OG Finder",  "Scuby OG Finder"),
    ("Scooby Upgrade",    "Scuby Upgrade"),
    ("Scooby diagnostic", "Scuby diagnostic"),
    ("Scooby v2",         "Scuby v2"),
]


def patch_file(filepath: str) -> bool:
    """Apply all replacements to a file. Returns True if any change was made."""
    if not os.path.exists(filepath):
        print(f"  ⚠️  {os.path.basename(filepath)} not found — skipping")
        return False

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        original = f.read()

    content = original
    for old, new in REPLACEMENTS:
        content = content.replace(old, new)

    if content == original:
        print(f"  ℹ️  {os.path.basename(filepath)}: nothing to change")
        return False

    # Back up before modifying
    bak = filepath + ".bak"
    if not os.path.exists(bak):
        shutil.copy2(filepath, bak)
        print(f"  📦 Backed up → {os.path.basename(bak)}")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    changed = sum(
        original.count(old) for old, _ in REPLACEMENTS
        if old in original
    )
    print(f"  ✅ {os.path.basename(filepath)}: {changed} replacement(s) applied")
    return True


def main():
    print()
    print("🐾 Scooby → Scuby rename tool")
    print("=" * 50)
    print()

    modified = 0
    for filename in TARGET_FILES:
        full_path = os.path.join(ROOT, filename)
        if patch_file(full_path):
            modified += 1

    print()
    print("=" * 50)
    print(f"🐾 Done! {modified} file(s) updated.")
    print()
    print("Name changes applied:")
    for old, new in REPLACEMENTS[:5]:   # show the key ones
        print(f"  {old!r:30s}  →  {new!r}")
    print()
    print("Backup files (.bak) created for every modified file.")
    print("Restart with:  python main.py")
    print()


if __name__ == "__main__":
    main()
