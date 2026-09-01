/* =========================================================
   ProjectX — Token Monitor
========================================================= */

import { PumpFunClient } from "./pumpfun.js";
import { TokenFilter } from "../filters/token-filter.js";
import { Database } from "../storage/database.js";
import { config } from "../config.js";
import { logger } from "../utils/logger.js";
import { Token, TokenWithScore } from "../types/index.js";

export class TokenMonitor {
  private client: PumpFunClient;
  private filter: TokenFilter;
  private db: Database;
  private interval: NodeJS.Timeout | null = null;
  private isRunning = false;

  // Callbacks
  onNewToken?: (token: TokenWithScore) => void;
  onHighScore?: (token: TokenWithScore) => void;

  constructor() {
    this.client = new PumpFunClient();
    this.filter = new TokenFilter();
    this.db = new Database(config.databasePath);
  }

  async start() {
    if (this.isRunning) return;
    
    this.isRunning = true;
    logger.divider();
    logger.success("🚀 ProjectX Scanner Starting...");
    logger.divider();
    logger.info(`RPC: ${config.solanaRpcUrl}`);
    logger.info(`Scan interval: ${config.scanIntervalMs}ms`);
    logger.info(`Min liquidity: $${config.filters.minLiquidityUsd}`);
    logger.info(`Max age: ${config.filters.maxTokenAgeSeconds}s`);
    logger.divider();

    const stats = this.db.getStats();
    logger.info(
      `Database: ${stats.total} tokens tracked (${stats.watching} watching)`
    );

    // Initial scan
    await this.scan();

    // Set up interval
    this.interval = setInterval(() => this.scan(), config.scanIntervalMs);
  }

  stop() {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.isRunning = false;
    logger.info("⏹  Scanner stopped");
  }

  private async scan() {
    try {
      // Fetch latest tokens
      const tokens = await this.client.fetchLatestTokens(50);
      
      if (tokens.length === 0) {
        return; // Silently retry on next interval
      }

      // Find new tokens we haven't seen
      const newTokens = this.client.getNewTokens(tokens);
      
      if (newTokens.length === 0) {
        return; // No new tokens
      }

      logger.info(`🔍 Found ${newTokens.length} new tokens`);

      // Filter and score
      const scored = this.filter.filterBatch(newTokens);

      if (scored.length === 0) {
        logger.info("No tokens passed filters this round");
        return;
      }

      logger.success(`✨ ${scored.length} tokens passed filters`);

      // Process each scored token
      for (const token of scored) {
        // Check if already in database
        if (this.db.getToken(token.mint)) {
          continue;
        }

        // Add to database
        const isNew = this.db.addToken(token);
        if (!isNew) continue;

        // Log it
        logger.token(
          `${token.symbol} | Score: ${token.score}/100 | $${token.liquidity.toFixed(0)} liq | ${token.holders} holders`
        );
        logger.info(`  Reasons: ${token.reasons.join(", ")}`);

        // Fire callbacks
        this.onNewToken?.(token);
        if (token.score >= 70) {
          this.onHighScore?.(token);
        }
      }

      // Print stats periodically
      const stats = this.db.getStats();
      logger.divider();
      logger.info(
        `📊 Tracked: ${stats.total} | Watching: ${stats.watching} | Bought: ${stats.bought}`
      );
    } catch (error) {
      logger.error("Scan error:", error);
    }
  }

  getStats() {
    return this.db.getStats();
  }

  getWatching() {
    return this.db.getWatching();
  }
}
