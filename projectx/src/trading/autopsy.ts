import { Position } from "./engine";
import { DexScreenerClient } from "../scanner/dexscreener";
import { logger } from "../utils/logger";

export interface TradeAutopsy {
  mint: string;
  symbol: string;
  entryPrice: number;
  exitPrice: number;
  pnlPercent: number;
  holdTimeSeconds: number;
  exitReason: string;
  
  // Market conditions at entry
  entryLiquidity: number;
  entryVolume: number;
  entryBuyRatio: number;
  entryAgeSeconds: number;
  
  // What went wrong (or right)
  diagnosis: string[];
  factors: {
    liquidityScore: number;    // 0-10: was liquidity sufficient?
    volumeScore: number;       // 0-10: was there real activity?
    timingScore: number;       // 0-10: was entry timing good?
    exitTimingScore: number;   // 0-10: was exit timing good?
  };
  
  // Verdict
  verdict: "win" | "loss" | "breakeven";
  severity: "minor" | "moderate" | "severe";
  timestamp: Date;
}

export interface PatternAnalysis {
  commonLossFactors: string[];
  commonWinFactors: string[];
  avgHoldTime: { wins: number; losses: number };
  avgLiquidity: { wins: number; losses: number };
  avgBuyRatio: { wins: number; losses: number };
  recommendations: string[];
}

export class Autopsy {
  private dexScreener: DexScreenerClient;
  private autopsies: TradeAutopsy[] = [];

  constructor(dexScreener: DexScreenerClient) {
    this.dexScreener = dexScreener;
  }

  async analyze(position: Position): Promise<TradeAutopsy> {
    const pnlPercent = position.pnlPercent || 0;
    const holdTime = position.exitTime && position.entryTime
      ? (position.exitTime.getTime() - position.entryTime.getTime()) / 1000
      : 0;

    // Get market data at time of trade (from cache or estimate)
    const marketData = await this.dexScreener.getTokenData(position.mint);
    
    const diagnosis: string[] = [];
    const factors = {
      liquidityScore: 5,
      volumeScore: 5,
      timingScore: 5,
      exitTimingScore: 5,
    };

    // Analyze exit reason
    if (position.exitReason?.includes("Stop loss")) {
      diagnosis.push("Hit stop loss — price dropped too fast");
      factors.exitTimingScore = 2;
      
      if (holdTime < 60) {
        diagnosis.push("Entered too early — price hadn't stabilized");
        factors.timingScore = 2;
      }
      
      if (holdTime > 300) {
        diagnosis.push("Held too long — should have cut sooner");
        factors.exitTimingScore = 3;
      }
    }

    if (position.exitReason?.includes("Take profit")) {
      diagnosis.push("Hit take profit — good exit");
      factors.exitTimingScore = 8;
    }

    if (position.exitReason?.includes("Time exit")) {
      diagnosis.push("Time-based exit — didn't move enough");
      factors.timingScore = 3;
    }

    // Analyze liquidity
    if (marketData?.liquidity) {
      if (marketData.liquidity < 2000) {
        diagnosis.push("Low liquidity — high slippage risk");
        factors.liquidityScore = 2;
      } else if (marketData.liquidity < 5000) {
        factors.liquidityScore = 4;
      } else if (marketData.liquidity > 20000) {
        factors.liquidityScore = 8;
      }
    }

    // Analyze volume activity
    if (marketData?.volume24h && marketData?.liquidity) {
      const volLiqRatio = marketData.volume24h / marketData.liquidity;
      if (volLiqRatio < 0.1) {
        diagnosis.push("Low volume relative to liquidity — no interest");
        factors.volumeScore = 2;
      } else if (volLiqRatio > 0.5) {
        factors.volumeScore = 8;
      }
    }

    // Analyze buy pressure
    const buys = marketData?.buys24h || 0;
    const sells = marketData?.sells24h || 0;
    if (buys + sells > 0) {
      const buyRatio = buys / (buys + sells);
      if (buyRatio < 0.4) {
        diagnosis.push("Sell-heavy — bears in control");
        factors.volumeScore = Math.min(factors.volumeScore, 3);
      } else if (buyRatio > 0.7) {
        diagnosis.push("Strong buy pressure — good entry");
        factors.volumeScore = Math.max(factors.volumeScore, 7);
      }
    }

    // Entry age analysis
    const ageSeconds = position.entryTime 
      ? (Date.now() - position.entryTime.getTime()) / 1000 
      : 0;
    
    if (ageSeconds < 30) {
      factors.timingScore = Math.max(factors.timingScore, 7);
      diagnosis.push("Ultra-fast entry — caught the pump");
    }

    // Determine verdict
    let verdict: "win" | "loss" | "breakeven" = "breakeven";
    let severity: "minor" | "moderate" | "severe" = "minor";

    if (pnlPercent > 10) {
      verdict = "win";
    } else if (pnlPercent < -10) {
      verdict = "loss";
      severity = pnlPercent < -30 ? "severe" : "moderate";
    } else if (pnlPercent < -5) {
      verdict = "loss";
      severity = "minor";
    }

    if (diagnosis.length === 0) {
      diagnosis.push(verdict === "win" ? "Clean win — strategy worked" : "Minor loss — normal variance");
    }

    const autopsy: TradeAutopsy = {
      mint: position.mint,
      symbol: position.symbol,
      entryPrice: position.entryPrice,
      exitPrice: position.exitPrice || 0,
      pnlPercent,
      holdTimeSeconds: holdTime,
      exitReason: position.exitReason || "unknown",
      entryLiquidity: marketData?.liquidity || 0,
      entryVolume: marketData?.volume24h || 0,
      entryBuyRatio: buys + sells > 0 ? buys / (buys + sells) : 0.5,
      entryAgeSeconds: ageSeconds,
      diagnosis,
      factors,
      verdict,
      severity,
      timestamp: new Date(),
    };

    this.autopsies.push(autopsy);
    this.logAutopsy(autopsy);

    return autopsy;
  }

  private logAutopsy(a: TradeAutopsy) {
    const emoji = a.verdict === "win" ? "💰" : a.verdict === "loss" ? "💸" : "➡️";
    const severityEmoji = a.severity === "severe" ? "🔴" : a.severity === "moderate" ? "🟡" : "🟢";
    
    logger.divider();
    logger.info(`${emoji} AUTOPSY: ${a.symbol} ${severityEmoji}`);
    logger.info(`  PnL: ${a.pnlPercent >= 0 ? "+" : ""}${a.pnlPercent.toFixed(1)}%`);
    logger.info(`  Hold: ${a.holdTimeSeconds.toFixed(0)}s | Exit: ${a.exitReason}`);
    logger.info(`  Factors: Liq=${a.factors.liquidityScore}/10 Vol=${a.factors.volumeScore}/10 Entry=${a.factors.timingScore}/10 Exit=${a.factors.exitTimingScore}/10`);
    logger.info(`  Diagnosis: ${a.diagnosis.join("; ")}`);
    logger.divider();
  }

  analyzePatterns(): PatternAnalysis {
    if (this.autopsies.length < 3) {
      return {
        commonLossFactors: [],
        commonWinFactors: [],
        avgHoldTime: { wins: 0, losses: 0 },
        avgLiquidity: { wins: 0, losses: 0 },
        avgBuyRatio: { wins: 0, losses: 0 },
        recommendations: ["Need more data — at least 3 trades"],
      };
    }

    const wins = this.autopsies.filter(a => a.verdict === "win");
    const losses = this.autopsies.filter(a => a.verdict === "loss");

    // Common factors in losses
    const lossFactors: Record<string, number> = {};
    losses.forEach(a => {
      a.diagnosis.forEach(d => {
        lossFactors[d] = (lossFactors[d] || 0) + 1;
      });
    });
    const commonLossFactors = Object.entries(lossFactors)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([factor]) => factor);

    // Common factors in wins
    const winFactors: Record<string, number> = {};
    wins.forEach(a => {
      a.diagnosis.forEach(d => {
        winFactors[d] = (winFactors[d] || 0) + 1;
      });
    });
    const commonWinFactors = Object.entries(winFactors)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([factor]) => factor);

    // Averages
    const avgHoldTime = {
      wins: wins.length > 0 ? wins.reduce((s, a) => s + a.holdTimeSeconds, 0) / wins.length : 0,
      losses: losses.length > 0 ? losses.reduce((s, a) => s + a.holdTimeSeconds, 0) / losses.length : 0,
    };

    const avgLiquidity = {
      wins: wins.length > 0 ? wins.reduce((s, a) => s + a.entryLiquidity, 0) / wins.length : 0,
      losses: losses.length > 0 ? losses.reduce((s, a) => s + a.entryLiquidity, 0) / losses.length : 0,
    };

    const avgBuyRatio = {
      wins: wins.length > 0 ? wins.reduce((s, a) => s + a.entryBuyRatio, 0) / wins.length : 0,
      losses: losses.length > 0 ? losses.reduce((s, a) => s + a.entryBuyRatio, 0) / losses.length : 0,
    };

    // Generate recommendations
    const recommendations: string[] = [];

    if (avgLiquidity.losses < avgLiquidity.wins * 0.7) {
      recommendations.push("Increase minimum liquidity threshold — low liq correlates with losses");
    }
    if (avgBuyRatio.losses < 0.5) {
      recommendations.push("Require higher buy ratio (>60%) — sell-heavy entries lose more");
    }
    if (avgHoldTime.losses > avgHoldTime.wins * 2) {
      recommendations.push("Reduce hold time on losing trades — cutting losses faster helps");
    }

    const severeLosses = losses.filter(a => a.severity === "severe").length;
    if (severeLosses > losses.length * 0.3) {
      recommendations.push("Tighten stop loss — too many severe losses");
    }

    if (recommendations.length === 0) {
      recommendations.push("Strategy performing well — no major adjustments needed");
    }

    return {
      commonLossFactors,
      commonWinFactors,
      avgHoldTime,
      avgLiquidity,
      avgBuyRatio,
      recommendations,
    };
  }

  getAutopsies(): TradeAutopsy[] {
    return [...this.autopsies];
  }

  getStats() {
    const all = this.autopsies;
    const wins = all.filter(a => a.verdict === "win");
    const losses = all.filter(a => a.verdict === "loss");

    return {
      total: all.length,
      wins: wins.length,
      losses: losses.length,
      winRate: all.length > 0 ? (wins.length / all.length * 100) : 0,
      avgPnl: all.length > 0 ? all.reduce((s, a) => s + a.pnlPercent, 0) / all.length : 0,
      avgWinPnl: wins.length > 0 ? wins.reduce((s, a) => s + a.pnlPercent, 0) / wins.length : 0,
      avgLossPnl: losses.length > 0 ? losses.reduce((s, a) => s + a.pnlPercent, 0) / losses.length : 0,
    };
  }
}
