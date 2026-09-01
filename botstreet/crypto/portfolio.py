"""
Portfolio Module — Track holdings, cash, and performance
"""
import json
from pathlib import Path
from datetime import datetime

from config import DATA_DIR, STARTING_BALANCE


def portfolio_file(agent_name: str) -> Path:
    return DATA_DIR / f"{agent_name}_portfolio.json"


def get_portfolio(agent_name: str) -> dict:
    """Get current portfolio state."""
    f = portfolio_file(agent_name)
    if f.exists():
        return json.loads(f.read_text())
    # Initialize new portfolio
    portfolio = {
        "agent": agent_name,
        "cash": STARTING_BALANCE,
        "holdings": {},
        "total_trades": 0,
        "created_at": datetime.now().isoformat(),
    }
    save_portfolio(agent_name, portfolio)
    return portfolio


def save_portfolio(agent_name: str, portfolio: dict):
    """Save portfolio to disk."""
    portfolio_file(agent_name).write_text(json.dumps(portfolio, indent=2))


def update_prices(agent_name: str, price_getter) -> dict:
    """Update all holding prices using the provided price getter function."""
    portfolio = get_portfolio(agent_name)
    for token, holding in portfolio["holdings"].items():
        try:
            price_data = price_getter(token, holding.get("chain", "solana"))
            if "price" in price_data:
                holding["current_price"] = price_data["price"]
                holding["value"] = holding["amount"] * price_data["price"]
        except Exception:
            pass
    save_portfolio(agent_name, portfolio)
    return portfolio


def execute_buy(agent_name: str, token: str, amount_usd: float, price: float, chain: str = "solana") -> dict:
    """Execute a buy trade."""
    portfolio = get_portfolio(agent_name)
    
    if amount_usd > portfolio["cash"]:
        return {"error": f"Insufficient cash. Have ${portfolio['cash']:.2f}, need ${amount_usd:.2f}"}
    
    # Check max position
    total_value = get_total_value(portfolio)
    new_position_value = amount_usd
    if token in portfolio["holdings"]:
        new_position_value += portfolio["holdings"][token].get("value", 0)
    
    if total_value > 0 and new_position_value / total_value > 0.5:
        return {"error": f"Max 50% position limit. Would be {new_position_value/total_value*100:.1f}%"}
    
    # Execute trade
    portfolio["cash"] -= amount_usd
    amount_tokens = amount_usd / price
    
    if token in portfolio["holdings"]:
        h = portfolio["holdings"][token]
        old_value = h["amount"] * h["avg_price"]
        h["amount"] += amount_tokens
        h["avg_price"] = (old_value + amount_usd) / h["amount"]
        h["current_price"] = price
        h["value"] = h["amount"] * price
    else:
        portfolio["holdings"][token] = {
            "amount": amount_tokens,
            "avg_price": price,
            "current_price": price,
            "value": amount_tokens * price,
            "chain": chain,
        }
    
    portfolio["total_trades"] += 1
    save_portfolio(agent_name, portfolio)
    
    return {
        "status": "executed",
        "token": token,
        "action": "BUY",
        "amount_usd": amount_usd,
        "amount_tokens": amount_tokens,
        "price": price,
        "total_value": get_total_value(portfolio),
    }


def execute_sell(agent_name: str, token: str, amount_usd: float, price: float) -> dict:
    """Execute a sell trade."""
    portfolio = get_portfolio(agent_name)
    
    if token not in portfolio["holdings"]:
        return {"error": f"No {token} position to sell"}
    
    holding = portfolio["holdings"][token]
    max_sell_value = holding["amount"] * price
    
    if amount_usd > max_sell_value:
        amount_usd = max_sell_value  # Sell all
    
    amount_tokens = amount_usd / price
    portfolio["cash"] += amount_usd
    holding["amount"] -= amount_tokens
    
    if holding["amount"] <= 0.0001:  # Dust threshold
        del portfolio["holdings"][token]
    else:
        holding["current_price"] = price
        holding["value"] = holding["amount"] * price
    
    portfolio["total_trades"] += 1
    save_portfolio(agent_name, portfolio)
    
    return {
        "status": "executed",
        "token": token,
        "action": "SELL",
        "amount_usd": amount_usd,
        "amount_tokens": amount_tokens,
        "price": price,
        "total_value": get_total_value(portfolio),
    }


def get_total_value(portfolio: dict) -> float:
    """Calculate total portfolio value."""
    holdings_value = sum(h.get("value", 0) for h in portfolio["holdings"].values())
    return portfolio["cash"] + holdings_value


def get_portfolio_summary(agent_name: str) -> str:
    """Human-readable portfolio summary."""
    portfolio = get_portfolio(agent_name)
    total = get_total_value(portfolio)
    pnl = total - STARTING_BALANCE
    pnl_pct = (pnl / STARTING_BALANCE) * 100
    
    lines = [
        f"Portfolio: {agent_name}",
        f"Total: ${total:,.2f} (PnL: {'+' if pnl >= 0 else ''}{pnl:.2f} / {'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%)",
        f"Cash: ${portfolio['cash']:,.2f} ({portfolio['cash']/total*100:.1f}%)" if total > 0 else "",
        f"Trades: {portfolio['total_trades']}",
    ]
    
    if portfolio["holdings"]:
        lines.append("\nHoldings:")
        for token, h in sorted(portfolio["holdings"].items(), key=lambda x: x[1].get("value", 0), reverse=True):
            pct = (h.get("value", 0) / total * 100) if total > 0 else 0
            lines.append(f"  {token}: {h['amount']:.4f} x ${h.get('current_price', 0):.6f} = ${h.get('value', 0):.2f} ({pct:.1f}%)")
    
    return "\n".join(lines)
