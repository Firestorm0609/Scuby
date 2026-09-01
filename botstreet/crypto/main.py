#!/usr/bin/env python3
"""
Crypto Trading Arena — Main Entry Point

Usage:
  python main.py run <agent1> <agent2> <agent3>   Run all agents
  python main.py dashboard [port]                  Start web dashboard
  python main.py status                            Show all portfolios
  python main.py reset                             Reset all portfolios
"""
import sys
import json
from datetime import datetime
from pathlib import Path

from config import DATA_DIR, STARTING_BALANCE
from agent import TradingAgent
from portfolio import get_portfolio, get_total_value, save_portfolio
from dashboard import start_dashboard


def run_agents(names: list):
    """Run daily process for all agents."""
    results = []
    for name in names:
        print(f"\n{'#'*60}")
        print(f"# Running agent: {name}")
        print(f"{'#'*60}\n")
        
        agent = TradingAgent(name)
        result = agent.run_daily()
        results.append(result)
    
    # Print leaderboard
    print("\n" + "="*60)
    print("LEADERBOARD")
    print("="*60 + "\n")
    
    results.sort(key=lambda x: x["portfolio_value"], reverse=True)
    for i, r in enumerate(results, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"#{i}"
        pnl = r["portfolio_value"] - STARTING_BALANCE
        pnl_pct = (pnl / STARTING_BALANCE) * 100
        print(f"{medal} {r['agent']}: ${r['portfolio_value']:,.2f} ({'+' if pnl >= 0 else ''}{pnl_pct:.1f}%)")


def show_status():
    """Show all portfolios."""
    print("\n" + "="*60)
    print("PORTFOLIO STATUS")
    print("="*60 + "\n")
    
    for f in sorted(DATA_DIR.glob("*_portfolio.json")):
        name = f.stem.replace("_portfolio", "")
        portfolio = json.loads(f.read_text())
        total = get_total_value(portfolio)
        pnl = total - STARTING_BALANCE
        pnl_pct = (pnl / STARTING_BALANCE) * 100
        
        print(f"{name}:")
        print(f"  Total: ${total:,.2f} ({'+' if pnl >= 0 else ''}{pnl_pct:.1f}%)")
        print(f"  Cash: ${portfolio['cash']:,.2f}")
        print(f"  Holdings: {len(portfolio.get('holdings', {}))} tokens")
        print(f"  Trades: {portfolio.get('total_trades', 0)}")
        print()


def reset_portfolios():
    """Reset all portfolios to starting balance."""
    for f in DATA_DIR.glob("*_portfolio.json"):
        name = f.stem.replace("_portfolio", "")
        portfolio = {
            "agent": name,
            "cash": STARTING_BALANCE,
            "holdings": {},
            "total_trades": 0,
            "created_at": datetime.now().isoformat(),
        }
        save_portfolio(name, portfolio)
        print(f"Reset {name}")
    
    # Also clear diaries and snapshots
    for f in DATA_DIR.glob("*_diary.md"):
        f.unlink()
        print(f"Cleared {f.name}")
    
    for f in DATA_DIR.glob("*_snapshots.jsonl"):
        f.unlink()
        print(f"Cleared {f.name}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "run":
        names = sys.argv[2:] if len(sys.argv) > 2 else ["mistral_alpha", "mistral_beta", "mistral_gamma"]
        run_agents(names)
    
    elif cmd == "dashboard":
        port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
        start_dashboard(port)
    
    elif cmd == "status":
        show_status()
    
    elif cmd == "reset":
        confirm = input("Reset all portfolios? (yes/no): ")
        if confirm.lower() == "yes":
            reset_portfolios()
    
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
