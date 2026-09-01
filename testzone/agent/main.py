#!/usr/bin/env python3
"""
🤖 AI Memecoin Trading Agent
Zero-cost autonomous trading on Solana (pump.fun + Jupiter)

Usage:
    python -m agent.main
    
Prerequisites:
    1. Install dependencies: pip install -r requirements.txt
    2. Install Ollama: curl -fsSL https://ollama.com/install.sh | sh
    3. Pull the model: ollama pull llama3
    4. Start Ollama: ollama serve
    5. Configure config.yaml
    6. Run: python -m agent.main
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agent.core.loop import AgentLoop


def setup_logging(config: dict):
    """Configure logging from config."""
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = log_cfg.get("file", "./logs/agent.log")

    # Create log directory
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Configure root logger
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_config(config_path: str = "agent/config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def main():
    """Main entry point."""
    # Load config
    config_path = os.getenv("AGENT_CONFIG", "agent/config.yaml")
    
    if not os.path.exists(config_path):
        print(f"❌ Config not found: {config_path}")
        print("   Copy config.yaml and customize it.")
        sys.exit(1)

    config = load_config(config_path)
    
    # Setup logging
    setup_logging(config)
    
    # Create and start the agent
    agent = AgentLoop(config)
    await agent.start()


if __name__ == "__main__":
    asyncio.run(main())
