"""Agent persistent state management."""

import json
from .database import Database


class AgentState:
    """Persistent key-value store for agent state."""

    def __init__(self, db: Database):
        self.db = db

    def get(self, key: str, default=None):
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT value FROM agent_state WHERE key = ?",
                (key,)
            ).fetchone()
            if row is None:
                return default
            try:
                return json.loads(row["value"])
            except (json.JSONDecodeError, TypeError):
                return row["value"]

    def set(self, key: str, value):
        with self.db.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO agent_state (key, value)
                   VALUES (?, ?)""",
                (key, json.dumps(value) if not isinstance(value, str) else value)
            )

    def delete(self, key: str):
        with self.db.connect() as conn:
            conn.execute("DELETE FROM agent_state WHERE key = ?", (key,))

    def get_all(self) -> dict:
        with self.db.connect() as conn:
            rows = conn.execute("SELECT key, value FROM agent_state").fetchall()
            result = {}
            for row in rows:
                try:
                    result[row["key"]] = json.loads(row["value"])
                except (json.JSONDecodeError, TypeError):
                    result[row["key"]] = row["value"]
            return result

    def init_defaults(self, starting_balance: float = 0.5):
        """Initialize state with defaults if not already set."""
        if self.get("balance") is None:
            self.set("balance", starting_balance)
        if self.get("total_pnl") is None:
            self.set("total_pnl", 0.0)
        if self.get("total_trades") is None:
            self.set("total_trades", 0)
        if self.get("daily_trades") is None:
            self.set("daily_trades", 0)
        if self.get("daily_loss") is None:
            self.set("daily_loss", 0.0)
        if self.get("daily_reset_date") is None:
            from datetime import datetime
            self.set("daily_reset_date", datetime.utcnow().date().isoformat())
