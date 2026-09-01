"""Reset all bet history (parlays) while keeping user accounts intact.

Usage: python3 reset_bets.py
"""
import sqlite3
import sys

DB_PATH = "parlay_bot.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Count before
    c.execute("SELECT COUNT(*) FROM parlays")
    parlay_count = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM users")
    user_count = c.fetchone()[0]

    print(f"Before reset: {parlay_count} parlays, {user_count} users")

    if parlay_count == 0:
        print("Nothing to reset.")
        conn.close()
        return

    confirm = input(f"Delete ALL {parlay_count} parlays? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Cancelled.")
        conn.close()
        return

    # Delete all parlays
    c.execute("DELETE FROM parlays")
    print(f"✅ Deleted {parlay_count} parlays.")

    # Reset user stats to clean state
    c.execute("""
        UPDATE users SET
            bankroll = 100.0,
            profit_protection = 0.0,
            current_streak = 0,
            best_win_streak = 0,
            best_loss_streak = 0,
            bankroll_history = '[]'
    """)
    print(f"✅ Reset {user_count} user balances to $100 and cleared streaks.")

    # Verify users still exist
    c.execute("SELECT id, tg_id, username FROM users")
    users = c.fetchall()
    print(f"\nUsers preserved ({len(users)}):")
    for uid, tg_id, username in users:
        print(f"  #{uid} — @{username or 'N/A'} (tg:{tg_id})")

    conn.commit()
    conn.close()
    print("\n✅ Done. Restart the bot to pick up changes.")


if __name__ == "__main__":
    main()
