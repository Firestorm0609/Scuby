import os
from pathlib import Path

# Load .env file if it exists
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

# API Configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
MAX_AGENT_STEPS = int(os.environ.get("MAX_AGENT_STEPS", "15"))

# Paths
WORKSPACE_DIR = Path(os.environ.get("WORKSPACE_DIR", Path(__file__).parent / "workspace"))
WORKSPACE_DIR.mkdir(exist_ok=True)
