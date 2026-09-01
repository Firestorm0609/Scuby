"""SQLite database connection and schema."""

import sqlite3
import os
from contextlib import contextmanager


class Database:
    def __init__(self, db_path: str = "./data.db"):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self):
        with self.connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_addr TEXT NOT NULL,
                    token_name TEXT,
                    action TEXT NOT NULL CHECK(action IN ('buy', 'sell')),
                    amount_sol REAL NOT NULL,
                    price_usd REAL,
                    tx_hash TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    llm_reasoning TEXT,
                    pnl_sol REAL,
                    pnl_pct REAL,
                    status TEXT DEFAULT 'open' CHECK(status IN ('open', 'closed')),
                    close_reason TEXT,
                    close_timestamp DATETIME
                );

                CREATE TABLE IF NOT EXISTS tokens_analyzed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_addr TEXT NOT NULL,
                    name TEXT,
                    profile_json TEXT,
                    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    decision TEXT
                );

                CREATE TABLE IF NOT EXISTS blacklist (
                    token_addr TEXT PRIMARY KEY,
                    reason TEXT,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS agent_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_addr);
                CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
                CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
                CREATE INDEX IF NOT EXISTS idx_tokens_addr ON tokens_analyzed(token_addr);
            """)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
