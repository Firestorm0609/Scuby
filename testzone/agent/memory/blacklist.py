"""Token blacklist management."""

from .database import Database


class Blacklist:
    def __init__(self, db: Database):
        self.db = db

    def add(self, token_addr: str, reason: str = "manual"):
        with self.db.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO blacklist (token_addr, reason)
                   VALUES (?, ?)""",
                (token_addr, reason)
            )

    def remove(self, token_addr: str):
        with self.db.connect() as conn:
            conn.execute(
                "DELETE FROM blacklist WHERE token_addr = ?",
                (token_addr,)
            )

    def is_blacklisted(self, token_addr: str) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as count FROM blacklist WHERE token_addr = ?",
                (token_addr,)
            ).fetchone()
            return row["count"] > 0

    def get_all(self) -> list:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM blacklist ORDER BY added_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
