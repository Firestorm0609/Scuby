import { Autopsy, PatternAnalysis } from "./autopsy";
import { FilterConfig } from "../types/index";
import { logger } from "../utils/logger";

export interface StrategyConfig {
  // Entry criteria
  minScore: number;
  minLiquidity: number;
  minBuyRatio: number;
  maxAgeSeconds: number;
  
  // Position sizing
  positionSize: number;        // SOL per trade
  maxPositions: number;
  
  // Exit criteria
  stopLossPercent: number;
  takeProfitPercent: number;
  maxHoldTimeSeconds: number;
  
  // Metadata
  version: number;
  lastUpdated: Date;
  performance: {
    winRate: number;
    avgPnl: number;
    totalTrades: number;
  };
}

const DEFAULT_STRATEGY: StrategyConfig = {
  minScore: 70,
  minLiquidity: 1000,
  minBuyRatio: 0.6,
  maxAgeSeconds: 3600,
  positionSize: 0.1,
  maxPositions: 5,
  stopLossPercent: -30,
  takeProfitPercent: 100,
  maxHoldTimeSeconds: 300,
  version: 1,
  lastUpdated: new Date(),
  performance: { winRate: 0, avgPnl: 0, totalTrades: 0 },
};

export class StrategyRewriter {
  private autopsy: Autopsy;
  private currentStrategy: StrategyConfig;
  private history: StrategyConfig[] = [];

  constructor(autopsy: Autopsy) {
    this.autopsy = autopsy;
    this.currentStrategy = { ...DEFAULT_STRATEGY };
  }

  async rewrite(): Promise<{ strategy: StrategyConfig; changes: string[] }> {
    const patterns = this.autopsy.analyzePatterns();
    const autopsies = this.autopsy.getAutopsies();
    
    if (autopsies.length < 5) {
      logger.info("Not enough data for rewrite — need at least 5 trades");
      return { strategy: this.currentStrategy, changes: [] };
    }

    const oldStrategy = { ...this.currentStrategy };
    const changes: string[] = [];

    // Save current version to history
    this.history.push({ ...this.currentStrategy });

    // Update performance metrics
    const stats = this.autopsy.getStats();
    this.currentStrategy.performance = {
      winRate: stats.winRate,
      avgPnl: stats.avgPnl,
      totalTrades: stats.total,
    };

    // === REWRITE BASED ON PATTERNS ===

    // 1. Adjust stop loss based on loss severity
    const severeLossRatio = autopsies.filter(a => a.verdict === "loss" && a.severity === "severe").length / autopsies.length;
    if (severeLossRatio > 0.2) {
      const newStopLoss = Math.max(this.currentStrategy.stopLossPercent + 5, -25);
      if (newStopLoss !== this.currentStrategy.stopLossPercent) {
        changes.push(`Stop loss: ${this.currentStrategy.stopLossPercent}% → ${newStopLoss}% (too many severe losses)`);
        this.currentStrategy.stopLossPercent = newStopLoss;
      }
    }

    // 2. Adjust take profit based on win patterns
    if (patterns.commonWinFactors.some(f => f.includes("take profit"))) {
      const newTP = Math.min(this.currentStrategy.takeProfitPercent + 10, 150);
      if (newTP !== this.currentStrategy.takeProfitPercent) {
        changes.push(`Take profit: ${this.currentStrategy.takeProfitPercent}% → ${newTP}% (TP is working)`);
        this.currentStrategy.takeProfitPercent = newTP;
      }
    }

    // 3. Adjust liquidity threshold
    if (patterns.avgLiquidity.losses < patterns.avgLiquidity.wins * 0.7 && patterns.avgLiquidity.losses > 0) {
      const newLiq = Math.max(this.currentStrategy.minLiquidity, patterns.avgLiquidity.losses * 1.2);
      if (newLiq > this.currentStrategy.minLiquidity) {
        changes.push(`Min liquidity: $${this.currentStrategy.minLiquidity} → $${newLiq.toFixed(0)} (low liq = losses)`);
        this.currentStrategy.minLiquidity = newLiq;
      }
    }

    // 4. Adjust buy ratio requirement
    if (patterns.avgBuyRatio.losses < 0.5 && patterns.avgBuyRatio.losses > 0) {
      const newBuyRatio = Math.min(this.currentStrategy.minBuyRatio + 0.05, 0.75);
      if (newBuyRatio > this.currentStrategy.minBuyRatio) {
        changes.push(`Min buy ratio: ${(this.currentStrategy.minBuyRatio * 100).toFixed(0)}% → ${(newBuyRatio * 100).toFixed(0)}% (sell-heavy = losses)`);
        this.currentStrategy.minBuyRatio = newBuyRatio;
      }
    }

    // 5. Adjust hold time
    if (patterns.avgHoldTime.losses > patterns.avgHoldTime.wins * 1.5 && patterns.avgHoldTime.losses > 0) {
      const newMaxHold = Math.max(this.currentStrategy.maxHoldTimeSeconds - 60, 120);
      if (newMaxHold < this.currentStrategy.maxHoldTimeSeconds) {
        changes.push(`Max hold: ${this.currentStrategy.maxHoldTimeSeconds}s → ${newMaxHold}s (holding too long = losses)`);
        this.currentStrategy.maxHoldTimeSeconds = newMaxHold;
      }
    }

    // 6. Adjust position sizing based on win rate
    if (stats.winRate > 60 && stats.totalTrades >= 10) {
      const newSize = Math.min(this.currentStrategy.positionSize + 0.02, 0.2);
      if (newSize > this.currentStrategy.positionSize) {
        changes.push(`Position size: ${this.currentStrategy.positionSize} SOL → ${newSize} SOL (winning streak)`);
        this.currentStrategy.positionSize = newSize;
      }
    } else if (stats.winRate < 40 && stats.totalTrades >= 10) {
      const newSize = Math.max(this.currentStrategy.positionSize - 0.02, 0.05);
      if (newSize < this.currentStrategy.positionSize) {
        changes.push(`Position size: ${this.currentStrategy.positionSize} SOL → ${newSize} SOL (losing streak)`);
        this.currentStrategy.positionSize = newSize;
      }
    }

    // 7. Adjust score threshold
    if (stats.winRate < 35 && stats.totalTrades >= 10) {
      const newScore = Math.min(this.currentStrategy.minScore + 5, 85);
      if (newScore > this.currentStrategy.minScore) {
        changes.push(`Min score: ${this.currentStrategy.minScore} → ${newScore} (too many losses)`);
        this.currentStrategy.minScore = newScore;
      }
    } else if (stats.winRate > 65 && stats.totalTrades >= 10) {
      const newScore = Math.max(this.currentStrategy.minScore - 5, 60);
      if (newScore < this.currentStrategy.minScore) {
        changes.push(`Min score: ${this.currentStrategy.minScore} → ${newScore} (performing well)`);
        this.currentStrategy.minScore = newScore;
      }
    }

    // Update metadata
    this.currentStrategy.version++;
    this.currentStrategy.lastUpdated = new Date();

    // Log changes
    if (changes.length > 0) {
      logger.divider();
      logger.success(`🧠 STRATEGY REWRITTEN — v${this.currentStrategy.version}`);
      changes.forEach(c => logger.info(`  → ${c}`));
      logger.info(`  Performance: ${stats.winRate.toFixed(0)}% win rate | ${stats.avgPnl.toFixed(1)}% avg PnL`);
      logger.divider();
    } else {
      logger.info("🧠 No strategy changes needed — performing within parameters");
    }

    return { strategy: this.currentStrategy, changes };
  }

  getStrategy(): StrategyConfig {
    return { ...this.currentStrategy };
  }

  getHistory(): StrategyConfig[] {
    return [...this.history];
  }

  getFilterConfig(): FilterConfig {
    return {
      minLiquidityUsd: this.currentStrategy.minLiquidity,
      maxTokenAgeSeconds: this.currentStrategy.maxAgeSeconds,
      minVolume24h: 0,
      minHolders: 0,
      requireSocials: false,
      requireWebsite: false,
      minBuySellRatio: this.currentStrategy.minBuyRatio,
    };
  }

  logStrategy() {
    const s = this.currentStrategy;
    logger.divider();
    logger.info(`📋 CURRENT STRATEGY (v${s.version})`);
    logger.info(`  Entry: Score≥${s.minScore} | Liq≥$${s.minLiquidity} | BuyRatio≥${(s.minBuyRatio * 100).toFixed(0)}%`);
    logger.info(`  Exit: SL=${s.stopLossPercent}% | TP=${s.takeProfitPercent}% | MaxHold=${s.maxHoldTimeSeconds}s`);
    logger.info(`  sizing: ${s.positionSize} SOL | Max ${s.maxPositions} positions`);
    logger.info(`  Performance: ${s.performance.winRate.toFixed(0)}% win | ${s.performance.avgPnl.toFixed(1)}% avg PnL | ${s.performance.totalTrades} trades`);
    logger.divider();
  }
}
