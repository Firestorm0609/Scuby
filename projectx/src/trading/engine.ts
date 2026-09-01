import { TokenWithScore } from "../types/index";
import { Autopsy } from "./autopsy";
import { StrategyRewriter, StrategyConfig } from "./rewriter";
import { DexScreenerClient } from "../scanner/dexscreener";
import { logger } from "../utils/logger";

export interface Position {
  mint: string;
  symbol: string;
  entryPrice: number;
  entryTime: Date;
  amount: number;
  status: "open" | "closed";
  exitPrice?: number;
  exitTime?: Date;
  exitReason?: string;
  pnl?: number;
  pnlPercent?: number;
  entryScore?: number;
  entryReasons?: string[];
  entryMcap?: number;
}

export interface TradeSignal {
  action: "BUY" | "SELL" | "HOLD";
  confidence: number;
  reasons: string[];
}

export class TradingEngine {
  private positions: Map<string, Position> = new Map();
  private autopsy: Autopsy;
  private rewriter: StrategyRewriter;
  private dexScreener: DexScreenerClient;
  private strategy: StrategyConfig;

  constructor(dexScreener: DexScreenerClient) {
    this.dexScreener = dexScreener;
    this.autopsy = new Autopsy(dexScreener);
    this.rewriter = new StrategyRewriter(this.autopsy);
    this.strategy = this.rewriter.getStrategy();
  }

  evaluateBuy(token: TokenWithScore): TradeSignal | null {
    // Check if we already have a position
    if (this.positions.has(token.mint)) {
      return null;
    }

    // Check max positions
    if (this.positions.size >= this.strategy.maxPositions) {
      return null;
    }

    const reasons: string[] = [];
    let confidence = 0;

    // Score threshold (dynamic from strategy)
    if (token.score >= this.strategy.minScore + 10) {
      reasons.push(`High score: ${token.score}/100 (threshold: ${this.strategy.minScore})`);
      confidence += 30;
    } else if (token.score >= this.strategy.minScore) {
      reasons.push(`Good score: ${token.score}/100`);
      confidence += 20;
    } else {
      return null;
    }

    // Freshness
    const createdAtMs = token.createdAt instanceof Date ? token.createdAt.getTime() : new Date(token.createdAt).getTime();
    const ageSeconds = (Date.now() - createdAtMs) / 1000;
    if (ageSeconds < 60) {
      reasons.push("Ultra fresh entry");
      confidence += 25;
    } else if (ageSeconds < 180) {
      reasons.push("Fresh entry");
      confidence += 15;
    }

    // Liquidity check (dynamic)
    if (token.liquidity >= this.strategy.minLiquidity * 2) {
      reasons.push(`Strong liquidity: $${token.liquidity.toFixed(0)}`);
      confidence += 15;
    } else if (token.liquidity >= this.strategy.minLiquidity) {
      reasons.push(`Adequate liquidity: $${token.liquidity.toFixed(0)}`);
      confidence += 10;
    }

    // Volume activity
    const volume = token.volume24h || 0;
    if (volume > 0 && token.liquidity > 0) {
      const ratio = volume / token.liquidity;
      if (ratio > 0.3) {
        reasons.push(`High volume activity: ${(ratio * 100).toFixed(0)}%`);
        confidence += 15;
      }
    }

    // Buy pressure (dynamic)
    const buys = token.buys24h || 0;
    const sells = token.sells24h || 0;
    if (buys + sells > 0) {
      const buyRatio = buys / (buys + sells);
      if (buyRatio >= this.strategy.minBuyRatio) {
        reasons.push(`Strong buy pressure: ${(buyRatio * 100).toFixed(0)}% (threshold: ${(this.strategy.minBuyRatio * 100).toFixed(0)}%)`);
        confidence += 10;
      }
    }

    // Social signals
    if (token.twitter || token.telegram || token.website) {
      reasons.push("Has socials");
      confidence += 5;
    }

    confidence = Math.min(100, confidence);

    if (confidence < 55) {
      return null;
    }

    return {
      action: "BUY",
      confidence,
      reasons,
    };
  }

  evaluateSell(position: Position, currentPrice: number): TradeSignal | null {
    if (position.status !== "open") return null;

    // Use market cap for PnL if available (avoids bonding curve vs DexScreener scale mismatch)
    const entryRef = (position as any).entryMcap || position.entryPrice;
    const currentRef = currentPrice; // Will be mcap from monitor
    const pnlPercent = entryRef > 0 ? ((currentRef - entryRef) / entryRef) * 100 : 0;
    const reasons: string[] = [];

    // Stop loss (dynamic)
    if (pnlPercent <= this.strategy.stopLossPercent) {
      reasons.push(`Stop loss triggered: ${pnlPercent.toFixed(1)}% (threshold: ${this.strategy.stopLossPercent}%)`);
      return { action: "SELL", confidence: 100, reasons };
    }

    // Take profit (dynamic)
    if (pnlPercent >= this.strategy.takeProfitPercent) {
      reasons.push(`Take profit triggered: ${pnlPercent.toFixed(1)}% (threshold: ${this.strategy.takeProfitPercent}%)`);
      return { action: "SELL", confidence: 100, reasons };
    }

    // Time-based exit (dynamic)
    const holdTime = Date.now() - position.entryTime.getTime();
    if (holdTime > this.strategy.maxHoldTimeSeconds * 1000 && pnlPercent < 20) {
      reasons.push(`Time exit: held ${Math.floor(holdTime / 1000)}s with ${pnlPercent.toFixed(1)}% (max: ${this.strategy.maxHoldTimeSeconds}s)`);
      return { action: "SELL", confidence: 80, reasons };
    }

    return null;
  }

  openPosition(token: TokenWithScore, price: number, mcap?: number): Position {
    const position: Position = {
      mint: token.mint,
      symbol: token.symbol,
      entryPrice: price,
      entryTime: new Date(),
      amount: this.strategy.positionSize,
      status: "open",
      entryScore: token.score,
      entryReasons: token.reasons,
      entryMcap: mcap || token.marketCap,
    };

    this.positions.set(token.mint, position);
    const priceStr = price < 0.0001 ? `$${price.toExponential(2)}` : `$${price.toFixed(8)}`;
    logger.trade(`📈 OPENED: ${token.symbol} at ${priceStr} (${this.strategy.positionSize} SOL) | Score: ${token.score}/100`);
    return position;
  }

  async closePosition(mint: string, price: number, reason: string): Promise<Position | null> {
    const position = this.positions.get(mint);
    if (!position || position.status !== "open") return null;

    const entryRef = (position as any).entryMcap || position.entryPrice;
    const pnlPercent = entryRef > 0 ? ((price - entryRef) / entryRef) * 100 : 0;
    const pnl = (pnlPercent / 100) * position.amount;

    position.exitPrice = price;
    position.exitTime = new Date();
    position.exitReason = reason;
    position.pnl = pnl;
    position.pnlPercent = pnlPercent;
    position.status = "closed";

    const emoji = pnlPercent >= 0 ? "💰" : "💸";
    logger.trade(
      `${emoji} CLOSED: ${position.symbol} at ${price < 0.0001 ? `$${price.toExponential(2)}` : `$${price.toFixed(8)}`} | PnL: ${pnlPercent >= 0 ? "+" : ""}${pnlPercent.toFixed(1)}% (${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} SOL) | Reason: ${reason}`
    );

    // Run autopsy on the closed trade
    await this.autopsy.analyze(position);

    // Try to rewrite strategy if we have enough data
    const autopsies = this.autopsy.getAutopsies();
    if (autopsies.length >= 5 && autopsies.length % 5 === 0) {
      const { changes } = await this.rewriter.rewrite();
      if (changes.length > 0) {
        this.strategy = this.rewriter.getStrategy();
      }
    }

    return position;
  }

  getOpenPositions(): Position[] {
    return Array.from(this.positions.values()).filter(p => p.status === "open");
  }

  getClosedPositions(): Position[] {
    return Array.from(this.positions.values()).filter(p => p.status === "closed");
  }

  getAutopsy() {
    return this.autopsy;
  }

  getRewriter() {
    return this.rewriter;
  }

  getStrategy(): StrategyConfig {
    return { ...this.strategy };
  }

  getStats() {
    const all = Array.from(this.positions.values());
    const open = all.filter(p => p.status === "open");
    const closed = all.filter(p => p.status === "closed");
    const wins = closed.filter(p => (p.pnl || 0) > 0);
    const losses = closed.filter(p => (p.pnl || 0) < 0);
    const totalPnl = closed.reduce((sum, p) => sum + (p.pnl || 0), 0);

    return {
      total: all.length,
      open: open.length,
      closed: closed.length,
      wins: wins.length,
      losses: losses.length,
      winRate: closed.length > 0 ? (wins.length / closed.length * 100) : 0,
      totalPnl,
      avgPnl: closed.length > 0 ? totalPnl / closed.length : 0,
      strategyVersion: this.strategy.version,
    };
  }
}
