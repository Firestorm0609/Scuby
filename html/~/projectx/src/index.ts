/* =========================================================
   ProjectX — Self-Improving Memecoin Trader
   Scanner Module Entry Point
========================================================= */

import { TokenMonitor } from "./scanner/monitor.js";
import { logger } from "./utils/logger.js";

async function main() {
  logger.divider();
  logger.info(" ProjectX — Self-Improving Memecoin Trader");
  logger.info(" Phase 1: Token Scanner");
  logger.divider();

  const monitor = new TokenMonitor();

  // Set up callbacks
  monitor.onNewToken = (token) => {
    logger.divider();
    logger.token(
      `🆕 NEW: ${token.symbol} (${token.name})`
    );
    logger.info(`   Mint: ${token.mint}`);
    logger.info(`   Score: ${token.score}/100`);
    logger.info(`   Liquidity: $${token.liquidity.toFixed(0)}`);
    logger.info(`   Holders: ${token.holders}`);
    logger.info(`   Market Cap: $${token.marketCap.toFixed(0)}`);
    logger.info(`   Age: ${Math.floor((Date.now() - token.createdAt.getTime()) / 1000)}s`);
  };

  monitor.onHighScore = (token) => {
    logger.divider();
    logger.trade(
      `🔥 HIGH SCORE: ${token.symbol} — ${token.score}/100`
    );
    logger.trade(`   This token looks promising!`);
    logger.trade(`   Watch for entry signals...`);
    logger.divider();
  };

  // Handle graceful shutdown
  process.on("SIGINT", () => {
    logger.divider();
    logger.info("Shutting down...");
    monitor.stop();
    const stats = monitor.getStats();
    logger.info(`Final stats: ${stats.total} tracked, ${stats.watching} watching`);
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    monitor.stop();
    process.exit(0);
  });

  // Start the monitor
  await monitor.start();
}

main().catch((error) => {
  logger.error("Fatal error:", error);
  process.exit(1);
});
