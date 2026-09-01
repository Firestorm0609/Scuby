"""Trade history storage and queries."""

from datetime import datetime, timedelta
from .database import Database


class TradeStore:
    def __init__(self, db: Database):
        self.db = db

    def record_buy(self, token_addr: str, token_name: str, amount_sol: float,
                   price_usd: float, tx_hash: str, reasoning: str) -> int:
        with self.db.connect() as conn:
            cursor = conn.execute(
                """INSERT INTO trades 
                   (token_addr, token_name, action, amount_sol, price_usd, 
                    tx_hash, llm_reasoning, status)
                   VALUES (?, ?, 'buy', ?, ?, ?, ?, 'open')""",
                (token_addr, token_name, amount_sol, price_usd, tx_hash, reasoning)
            )
            return cursor.lastrowid

    def record_sell(self, token_addr: str, amount_sol: float, price_usd: float,
                    tx_hash: str, pnl_sol: float, pnl_pct: float, 
                    close_reason: str) -> int:
        with self.db.connect() as conn:
            # Close the open buy trade
            conn.execute(
                """UPDATE trades 
                   SET status = 'closed', close_reason = ?, 
                       close_timestamp = ?, pnl_sol = ?, pnl_pct = ?
                   WHERE token_addr = ? AND status = 'open'""",
                (close_reason, datetime.utcnow().isoformat(), 
                 pnl_sol, pnl_pct, token_addr)
            )
            # Record the sell
            cursor = conn.execute(
                """INSERT INTO trades 
                   (token_addr, action, amount_sol, price_usd, tx_hash, 
                    status, pnl_sol, pnl_pct, close_reason)
                   VALUES (?, 'sell', ?, ?, ?, 'closed', ?, ?, ?)""",
                (token_addr, amount_sol, price_usd, tx_hash, 
                 pnl_sol, pnl_pct, close_reason)
            )
            return cursor.lastrowid

    def get_open_positions(self) -> list:
        with self.db.connect() as conn:
            rows = conn.execute(
                """SELECT * FROM trades 
                   WHERE action = 'buy' AND status = 'open'
                   ORDER BY timestamp DESC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_open_position(self, token_addr: str) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT * FROM trades 
                   WHERE token_addr = ? AND action = 'buy' AND status = 'open'""",
                (token_addr,)
            ).fetchone()
            return dict(row) if row else None

    def get_recent_trades(self, limit: int = 10) -> list:
        with self.db.connect() as conn:
            rows = conn.execute(
                """SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_daily_trades(self) -> int:
        today = datetime.utcnow().date().isoformat()
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT COUNT(*) as count FROM trades 
                   WHERE timestamp >= ?""",
                (today,)
            ).fetchone()
            return row["count"]

    def get_daily_pnl(self) -> float:
        today = datetime.utcnow().date().isoformat()
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(pnl_sol), 0) as total FROM trades 
                   WHERE timestamp >= ? AND status = 'closed'""",
                (today,)
            ).fetchone()
            return row["total"]

    def get_total_pnl(self) -> float:
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(pnl_sol), 0) as total FROM trades 
                   WHERE status = 'closed'"""
            ).fetchone()
            return row["total"]

    def get_win_rate(self) -> float:
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN pnl_sol > 0 THEN 1 ELSE 0 END) as wins
                   FROM trades WHERE status = 'closed'"""
            ).fetchone()
            if row["total"] == 0:
                return 0.0
            return row["wins"] / row["total"]

    def get_total_trades(self) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as count FROM trades WHERE status = 'closed'"
            ).fetchone()
            return row["count"]

    def was_analyzed_recently(self, token_addr: str, minutes: int = 30) -> bool:
        cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT COUNT(*) as count FROM tokens_analyzed 
                   WHERE token_addr = ? AND analyzed_at > ?""",
                (token_addr, cutoff)
            ).fetchone()
            return row["count"] > 0

    def record_analysis(self, token_addr: str, name: str, 
                        profile_json: str, decision: str):
        with self.db.connect() as conn:
            # Upsert — update if exists, insert if not
            existing = conn.execute(
                "SELECT id FROM tokens_analyzed WHERE token_addr = ?",
                (token_addr,)
            ).fetchone()
            
            if existing:
                conn.execute(
                    """UPDATE tokens_analyzed 
                       SET last_seen = ?, analyzed_at = ?, 
                           profile_json = ?, decision = ?, name = ?
                       WHERE token_addr = ?""",
                    (datetime.utcnow().isoformat(), datetime.utcnow().isoformat(),
                     profile_json, decision, name, token_addr)
                )
            else:
                conn.execute(
                    """INSERT INTO tokens_analyzed 
                       (token_addr, name, profile_json, decision)
                       VALUES (?, ?, ?, ?)""",
                    (token_addr, name, profile_json, decision)
                )
