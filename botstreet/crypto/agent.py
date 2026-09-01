"""
Trading Agent — Mistral-powered portfolio manager
"""
import json
import sys
from datetime import datetime
from pathlib import Path
from openai import OpenAI

from config import MISTRAL_API_KEYS, MISTRAL_MODEL, DATA_DIR
from prices import get_current_price, search_token, get_trending
from portfolio import (
    get_portfolio, update_prices, execute_buy, execute_sell,
    get_total_value, get_portfolio_summary, save_portfolio,
)

# Popular tokens with addresses
TOKEN_DB = {
    "SOL": {"address": "So11111111111111111111111111111111111111112", "chain": "solana"},
    "WIF": {"address": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "chain": "solana"},
    "BONK": {"address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "chain": "solana"},
    "POPCAT": {"address": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "chain": "solana"},
    "FARTCOIN": {"address": "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump", "chain": "solana"},
    "TRUMP": {"address": "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN", "chain": "solana"},
}


class TradingAgent:
    def __init__(self, name: str):
        self.name = name
        self.client = OpenAI(
            api_key=MISTRAL_API_KEYS[0] if MISTRAL_API_KEYS else "",
            base_url="https://api.mistral.ai/v1",
            timeout=15.0,
        )
    
    def _call_llm(self, messages):
        try:
            response = self.client.chat.completions.create(
                model=MISTRAL_MODEL,
                messages=messages,
                max_tokens=2000,
                temperature=0.7,
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            return f"LLM error: {str(e)}"
    
    def _get_market_data(self):
        """Get real market data for all tokens."""
        data = []
        for symbol, info in TOKEN_DB.items():
            try:
                price_data = get_current_price(info["address"], info["chain"])
                if "price" in price_data:
                    data.append({
                        "symbol": symbol,
                        "price": price_data["price"],
                        "change_24h": price_data.get("change_24h", 0),
                        "volume_24h": price_data.get("volume_24h", 0),
                        "liquidity": price_data.get("liquidity", 0),
                    })
            except Exception:
                pass
        return data
    
    def run_daily(self):
        """Execute the daily trading process."""
        print(f"\n{'='*60}")
        print(f"Agent: {self.name} | Date: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"{'='*60}\n")
        
        # Step 1: Update prices
        print("Step 1: Updating prices...")
        portfolio = update_prices(self.name, get_current_price)
        print(f"  Portfolio value: ${get_total_value(portfolio):,.2f}")
        
        # Step 2: Review portfolio
        print("\nStep 2: Reviewing portfolio...")
        summary = get_portfolio_summary(self.name)
        print(summary)
        
        # Step 3: Research with real data
        print("\nStep 3: Researching markets...")
        market_data = self._get_market_data()
        
        market_text = "Available tokens (Solana):\n"
        for t in market_data:
            change = t.get("change_24h", 0)
            market_text += f"  {t['symbol']}: ${t['price']:.6f} | 24h: {change:+.1f}% | Vol: ${t['volume_24h']:,.0f} | Liq: ${t['liquidity']:,.0f}\n"
        print(market_text)
        
        # Step 4: Make trading decision
        print("\nStep 4: Making trading decisions...")
        
        holdings_text = "  Cash only\n" if not portfolio["holdings"] else ""
        for token, h in portfolio["holdings"].items():
            holdings_text += f"  {token}: {h['amount']:.4f} tokens @ ${h.get('current_price', 0):.6f} = ${h.get('value', 0):.2f}\n"
        
        prompt = f"""You are a crypto portfolio manager competing against other AI agents. Your goal is to MAXIMIZE portfolio value.

Starting balance: $100,000

Current Portfolio:
  Cash: ${portfolio['cash']:,.2f}
  Holdings:
{holdings_text}
  Total Value: ${get_total_value(portfolio):,.2f}

Market Data (Solana tokens):
{market_text}

RULES:
1. You MUST make trades. Sitting in cash loses to competitors.
2. Max 50% in single position
3. Buy tokens with good liquidity (>$100k) and volume
4. Consider 24h change momentum
5. Diversify across 3-5 tokens
6. Think like a hedge fund manager

AVAILABLE TOKENS TO TRADE (use these symbols):
- SOL, WIF, BONK, POPCAT, FARTCOIN, TRUMP

RESPOND WITH JSON ONLY:
{{"trades": [{{"token": "SOL", "action": "buy", "amount_usd": 20000}}]}}

Make 3-5 trades to deploy your capital. Example allocation:
- 30% SOL (blue chip)
- 20% WIF (meme leader)
- 15% BONK (community)
- 15% POPCAT (trending)
- 20% FARTCOIN or TRUMP (high risk)"""

        messages = [
            {"role": "system", "content": f"You are {self.name}, an aggressive crypto trading agent. You ALWAYS trade. You NEVER hold cash."},
            {"role": "user", "content": prompt},
        ]
        
        response = self._call_llm(messages)
        print(f"  LLM Response: {response[:300]}...")
        
        # Parse trades
        try:
            start = response.find("{")
            end = response.rfind("}") + 1
            if start >= 0 and end > start:
                trade_data = json.loads(response[start:end])
                trades = trade_data.get("trades", [])
            else:
                trades = []
        except json.JSONDecodeError:
            print("  Could not parse trade decisions")
            trades = []
        
        # Execute trades
        executed = []
        for trade in trades:
            token = trade.get("token", "").upper()
            action = trade.get("action", "").lower()
            amount = float(trade.get("amount_usd", 0))
            
            if not token or not action or amount <= 0:
                continue
            
            if token not in TOKEN_DB:
                print(f"  Unknown token: {token}")
                continue
            
            # Get current price
            price_data = get_current_price(TOKEN_DB[token]["address"], TOKEN_DB[token]["chain"])
            if "price" not in price_data:
                print(f"  Could not get price for {token}")
                continue
            
            price = price_data["price"]
            
            if action == "buy":
                result = execute_buy(self.name, token, amount, price, TOKEN_DB[token]["chain"])
            elif action == "sell":
                result = execute_sell(self.name, token, amount, price)
            else:
                continue
            
            if "error" in result:
                print(f"  Trade failed: {result['error']}")
            else:
                print(f"  ✅ {action.upper()} ${amount:.2f} of {token} @ ${price:.6f}")
                executed.append(trade)
        
        # Step 5: Write diary
        print("\nStep 5: Writing diary...")
        self._write_diary(portfolio, executed, market_data)
        
        # Step 6: Save snapshot
        print("\nStep 6: Saving snapshot...")
        self._save_snapshot(portfolio)
        
        print(f"\n{'='*60}")
        print(f"Day complete for {self.name}")
        print(f"{'='*60}\n")
        
        return {
            "agent": self.name,
            "portfolio_value": get_total_value(portfolio),
            "trades": executed,
            "summary": summary,
        }
    
    def _write_diary(self, portfolio, trades, market_data):
        diary_file = DATA_DIR / f"{self.name}_diary.md"
        
        trending = sorted(market_data, key=lambda x: x.get("change_24h", 0), reverse=True)[:3]
        
        entry = f"""## Day {portfolio['total_trades']} — {datetime.now().strftime('%Y-%m-%d')}

### Market Overview
Top movers: {', '.join([f"{t['symbol']} ({t['change_24h']:+.1f}%)" for t in trending])}

### Portfolio
Total: ${get_total_value(portfolio):,.2f} | Cash: {portfolio['cash']/max(get_total_value(portfolio),1)*100:.1f}%

### Trades
{chr(10).join([f"- {t['action'].upper()} ${t['amount_usd']:.2f} of {t['token']}" for t in trades]) if trades else "- No trades today"}

---

"""
        
        if diary_file.exists():
            existing = diary_file.read_text()
            diary_file.write_text(entry + existing)
        else:
            diary_file.write_text(entry)
    
    def _save_snapshot(self, portfolio):
        snapshot_file = DATA_DIR / f"{self.name}_snapshots.jsonl"
        snapshot = {
            "date": datetime.now().isoformat(),
            "total_value": get_total_value(portfolio),
            "cash": portfolio["cash"],
            "holdings": portfolio["holdings"],
            "trades": portfolio["total_trades"],
        }
        with open(snapshot_file, "a") as f:
            f.write(json.dumps(snapshot) + "\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python agent.py <agent_name>")
        sys.exit(1)
    
    name = sys.argv[1]
    agent = TradingAgent(name)
    result = agent.run_daily()
    print(f"\nFinal Portfolio:")
    print(result["summary"])


if __name__ == "__main__":
    main()
