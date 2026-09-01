/* =========================================================
   ProjectX — Token Filter & Scorer
========================================================= */

import { Token, TokenWithScore, FilterConfig } from "../types/index.js";
import { config } from "../config.js";
import { logger } from "../utils/logger.js";

export class TokenFilter {
  private config: FilterConfig;

  constructor(config?: Partial<FilterConfig>) {
    this.config = { ...config.filters, ...config };
  }

  filter(token: Token): TokenWithScore | null {
    const reasons: string[] = [];
    let score = 0;

    // Age check — must be fresh
    const ageSeconds = (Date.now() - token.createdAt.getTime()) / 1000;
    if (ageSeconds > this.config.maxTokenAgeSeconds) {
      logger.filter(`SKIP ${token.symbol} — too old (${Math.floor(ageSeconds)}s)`);
      return null;
    }
    reasons.push(`Fresh (${Math.floor(ageSeconds)}s old)`);
    score += 20;

    // Liquidity check
    if (token.liquidity < this.config.minLiquidityUsd) {
      logger.filter(`SKIP ${token.symbol} — low liquidity ($${token.liquidity.toFixed(0)})`);
      return null;
    }
    reasons.push(`Liquidity $${token.liquidity.toFixed(0)}`);
    score += 15;

    // Volume check
    if (token.volume24h < this.config.minVolume24h) {
      logger.filter(`SKIP ${token.symbol} — low volume ($${token.volume24h.toFixed(0)})`);
      return null;
    }
    reasons.push(`Volume $${token.volume24h.toFixed(0)}`);
    score += 15;

    // Holders check
    if (token.holders < this.config.minHolders) {
      logger.filter(`SKIP ${token.symbol} — too few holders (${token.holders})`);
      return null;
    }
    reasons.push(`${token.holders} holders`);
    score += 10;

    // Buy/Sell ratio — bullish signal
    const buyRatio = token.buys24h / (token.buys24h + token.sells24h + 1);
    if (buyRatio >= this.config.minBuySellRatio) {
      reasons.push(`Strong buy ratio (${(buyRatio * 100).toFixed(0)}%)`);
      score += 20;
    } else {
      reasons.push(`Weak buy ratio (${(buyRatio * 100).toFixed(0)}%)`);
      score -= 10;
    }

    // Social presence bonus
    if (token.twitter) {
      reasons.push("Has Twitter");
      score += 10;
    }
    if (token.telegram) {
      reasons.push("Has Telegram");
      score += 5;
    }
    if (token.website) {
      reasons.push("Has Website");
      score += 5;
    }

    // Creator trust — if they've launched before
    // (would need historical data for this)

    // Market cap momentum
    if (token.marketCap > 10000) {
      reasons.push(`MC $${token.marketCap.toFixed(0)}`);
      score += 10;
    }

    // Normalize score to 0-100
    score = Math.max(0, Math.min(100, score));

    logger.success(
      `SCORE ${token.symbol}: ${score}/100 — ${reasons.join(", ")}`
    );

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
      if (scored && scored.score >= 40) {
        results.push(scored);
      }
    }

    // Sort by score descending
    results.sort((a, b) => b.score - a.score);

    return results;
  }
}
