import { Token, TokenWithScore, FilterConfig } from "../types/index";
import { config } from "../config";

export class TokenFilter {
  private config: FilterConfig;

  constructor(filterConfig?: Partial<FilterConfig>) {
    this.config = { ...config.filters, ...filterConfig };
  }

  filter(token: Token): TokenWithScore | null {
    const reasons: string[] = [];
    let score = 0;

    // Age check
    const createdAtMs = token.createdAt instanceof Date ? token.createdAt.getTime() : new Date(token.createdAt).getTime();
    const ageSeconds = (Date.now() - createdAtMs) / 1000;
    if (ageSeconds > this.config.maxTokenAgeSeconds) {
      return null;
    }

    // Liquidity check (hard filter — pump.fun bonding curve minimum)
    const liquidity = token.liquidity || 0;
    if (liquidity < this.config.minLiquidityUsd) {
      return null;
    }

    const buys = token.buys24h || 0;
    const sells = token.sells24h || 0;
    const totalTxns = buys + sells;
    const volume = token.volume24h || 0;

    // === SCORING ===

    // 1. LIQUIDITY (0-25)
    if (liquidity >= 20000) {
      reasons.push(`Liq: $${this.formatNum(liquidity)} 💪`);
      score += 25;
    } else if (liquidity >= 10000) {
      reasons.push(`Liq: $${this.formatNum(liquidity)}`);
      score += 20;
    } else if (liquidity >= 5000) {
      reasons.push(`Liq: $${this.formatNum(liquidity)}`);
      score += 15;
    } else {
      reasons.push(`Liq: $${this.formatNum(liquidity)}`);
      score += 8;
    }

    // 2. FRESHNESS (0-15)
    if (ageSeconds < 30) {
      reasons.push("🔥 Ultra fresh (<30s)");
      score += 15;
    } else if (ageSeconds < 120) {
      reasons.push("⚡ Fresh (<2m)");
      score += 10;
    } else if (ageSeconds < 300) {
      reasons.push("Fresh (<5m)");
      score += 5;
    }

    // 3. BUY PRESSURE (0-25)
    if (totalTxns >= 10) {
      const buyRatio = buys / totalTxns;
      if (buyRatio >= 0.8) {
        reasons.push(`Buy ratio: ${(buyRatio * 100).toFixed(0)}% 🟢🟢`);
        score += 25;
      } else if (buyRatio >= 0.65) {
        reasons.push(`Buy ratio: ${(buyRatio * 100).toFixed(0)}% 🟢`);
        score += 18;
      } else if (buyRatio >= 0.55) {
        reasons.push(`Buy ratio: ${(buyRatio * 100).toFixed(0)}%`);
        score += 10;
      } else {
        reasons.push(`Sell heavy: ${(buyRatio * 100).toFixed(0)}% 🔴`);
        score -= 15;
      }
    } else if (totalTxns > 0) {
      reasons.push(`${totalTxns} txns`);
    }

    // 4. VOLUME (0-15)
    if (volume > 0 && liquidity > 0) {
      const ratio = volume / liquidity;
      if (ratio > 1.0) {
        reasons.push(`Vol/Liq: ${(ratio * 100).toFixed(0)}% 🔥`);
        score += 15;
      } else if (ratio > 0.3) {
        reasons.push(`Vol/Liq: ${(ratio * 100).toFixed(0)}%`);
        score += 10;
      } else if (ratio > 0.1) {
        reasons.push(`Vol/Liq: ${(ratio * 100).toFixed(0)}%`);
        score += 5;
      }
    }

    // 5. MARKET CAP (0-10)
    const marketCap = token.marketCap || 0;
    if (marketCap > 50000) {
      reasons.push(`MC: $${this.formatNum(marketCap)}`);
      score += 10;
    } else if (marketCap > 10000) {
      reasons.push(`MC: $${this.formatNum(marketCap)}`);
      score += 5;
    } else if (marketCap > 3000) {
      reasons.push(`MC: $${this.formatNum(marketCap)}`);
      score += 2;
    }

    // 6. SOCIALS (0-10)
    let socialScore = 0;
    if (token.twitter) { reasons.push("Twitter ✓"); socialScore += 5; }
    if (token.telegram) { reasons.push("TG ✓"); socialScore += 3; }
    if (token.website) { reasons.push("Web ✓"); socialScore += 2; }
    score += Math.min(socialScore, 10);

    // 7. TXN ACTIVITY (0-5)
    if (totalTxns > 100) {
      reasons.push(`${totalTxns} txns 📊`);
      score += 5;
    } else if (totalTxns > 30) {
      reasons.push(`${totalTxns} txns`);
      score += 3;
    }

    reasons.unshift(`Age: ${this.formatAge(ageSeconds)}`);
    score = Math.max(0, Math.min(100, score));

    // Low bar to track — the buy decision happens in the monitor
    if (score < 30) return null;

    return {
      ...token,
      score,
      reasons,
      tracked: false,
    };
  }

  filterBatch(tokens: Token[]): TokenWithScore[] {
    const results: TokenWithScore[] = [];
    for (const token of tokens) {
      const scored = this.filter(token);
      if (scored) {
        results.push(scored);
      }
    }
    results.sort((a, b) => b.score - a.score);
    return results;
  }

  private formatAge(seconds: number): string {
    if (seconds < 60) return `${Math.floor(seconds)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
    return `${Math.floor(seconds / 3600)}h`;
  }

  private formatNum(n: number): string {
    if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
    if (n >= 1000) return `${(n / 1000).toFixed(1)}K`;
    return n.toFixed(0);
  }
}
