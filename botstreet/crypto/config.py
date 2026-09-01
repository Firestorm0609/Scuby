"""
Crypto Trading Agent Configuration
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path("/root/gemini-agent/.env"))

# Mistral — read individual keys
MISTRAL_API_KEYS = []
key = os.getenv("MISTRAL_API_KEY", "")
if key:
    MISTRAL_API_KEYS.append(key)
for i in range(1, 20):
    key = os.getenv(f"MISTRAL_API_KEY_{i}", "")
    if key:
        MISTRAL_API_KEYS.append(key)

MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Trading rules
STARTING_BALANCE = 100000.0
MAX_POSITION_PCT = 0.5
