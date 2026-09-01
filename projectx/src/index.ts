import { TokenMonitor } from "./scanner/monitor";
import { logger } from "./utils/logger";

async function main() {
  logger.divider();
  logger.info(" ProjectX — Self-Improving Memecoin Trader");
  logger.info(" Phase 2: Autopsy + Strategy Rewriting + Telegram Bot");
  logger.divider();

  const monitor = new TokenMonitor();

  monitor.onNewToken = (token) => {
    const buys = token.buys24h || 0;
    const sells = token.sells24h || 0;
    logger.divider();
    logger.token(`NEW: ${token.symbol} (${token.name})`);
    logger.info(`   Score: ${token.score}/100 | Price: $${token.price?.toFixed(8) || "N/A"}`);
    logger.info(`   Liq: $${token.liquidity.toFixed(0)} | Vol: $${token.volume24h.toFixed(0)} | MC: $${token.marketCap.toFixed(0)}`);
    const createdAtMs = token.createdAt instanceof Date ? token.createdAt.getTime() : new Date(token.createdAt).getTime();
    logger.info(`   Txns: ${buys}B/${sells}S | Age: ${Math.floor((Date.now() - createdAtMs) / 1000)}s`);
  };

  monitor.onHighScore = (token) => {
    logger.divider();
    logger.trade(`HIGH SCORE: ${token.symbol} — ${token.score}/100`);
    logger.divider();
  };

  monitor.onTrade = (action, details) => {
    if (action === "BUY") {
      logger.trade(`🟢 BUY SIGNAL: ${details}`);
    } else {
      logger.trade(`🔴 SELL SIGNAL: ${details}`);
    }
  };

  process.on("SIGINT", () => {
    logger.divider();
    logger.info("Shutting down...");
    monitor.stop();
    process.exit(0);
  });

  await monitor.start();
}

main().catch((error) => {
  logger.error("Fatal error:", error);
  process.exit(1);
});
