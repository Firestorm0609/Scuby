import { PumpFunClient } from "./pumpfun";
import { DexScreenerClient } from "./dexscreener";
import { TokenFilter } from "../filters/token-filter";
import { Database } from "../storage/database";
import { TelegramNotifier } from "../notifications/telegram";
import { TradingEngine } from "../trading/engine";
import { Wallet } from "../wallet";
import { config } from "../config";
import { logger } from "../utils/logger";
import { TokenWithScore } from "../types/index";

export class TokenMonitor {
  private client: PumpFunClient;
  private dexScreener: DexScreenerClient;
  private filter: TokenFilter;
  private db: Database;
  private telegram: TelegramNotifier;
  private trading: TradingEngine;
  private wallet: Wallet | null;
  private interval: NodeJS.Timeout | null = null;
  private isRunning = false;
  private scanCount = 0;
  private network: "mainnet" | "devnet" = "devnet";

  onNewToken?: (token: TokenWithScore) => void;
  onHighScore?: (token: TokenWithScore) => void;
  onTrade?: (action: string, details: string) => void;

  constructor() {
    this.client = new PumpFunClient();
    this.dexScreener = new DexScreenerClient();
    this.filter = new TokenFilter();
    this.db = new Database(config.databasePath);
    this.telegram = new TelegramNotifier();
    this.trading = new TradingEngine(this.dexScreener);
    this.wallet = Wallet.fromEnv();
    
    // Set default network from env
    this.network = (process.env.SOLANA_NETWORK as "mainnet" | "devnet") || "devnet";
  }

  async start() {
    if (this.isRunning) return;
    
    this.isRunning = true;
    const strategy = this.trading.getStrategy();
    
    logger.divider();
    logger.success("ProjectX Scanner Starting...");
    logger.divider();
    logger.info(`Scan interval: ${config.scanIntervalMs}ms`);
    logger.info(`Network: ${this.network.toUpperCase()}`);
    logger.info(`Wallet: ${this.wallet ? `✅ ${this.wallet.address.slice(0, 8)}...` : "❌ Not connected"}`);
    logger.info(`Telegram: ${this.telegram.isEnabled() ? "✅ ACTIVE" : "⚠️ Disabled"}`);
    logger.info(`Trading: ${this.wallet ? "LIVE" : "SIMULATION"}`);
    logger.divider();
    
    this.trading.getRewriter().logStrategy();

    const stats = this.db.getStats();
    logger.info(`Database: ${stats.total} tokens tracked`);

    // Setup Telegram data providers
    if (this.telegram.isEnabled()) {
      this.telegram.setDataProviders(
        () => this.trading.getOpenPositions().concat(this.trading.getClosedPositions()),
        () => this.trading.getStrategy(),
        () => this.getStats(),
        () => this.wallet,
        async (network) => await this.switchNetwork(network),
        async (privateKey: string) => {
          try {
            const rpcEndpoints: Record<string, string> = {
              mainnet: process.env.SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com",
              devnet: "https://api.devnet.solana.com",
            };
            this.wallet = new Wallet({
              privateKey,
              rpcEndpoint: rpcEndpoints[this.network],
              network: this.network,
            });
            logger.success(`Wallet imported via Telegram: ${this.wallet.address.slice(0, 8)}...`);
            return true;
          } catch (error) {
            logger.error("Wallet import failed:", error);
            return false;
          }
        },
        (updates: Partial<any>) => {
          const strategy = this.trading.getRewriter().getStrategy();
          Object.assign(strategy, updates);
          logger.info(`Strategy updated via Telegram`);
        }
      );
      this.telegram.startPolling();
    }

    await this.scan();
    this.interval = setInterval(() => this.scan(), config.scanIntervalMs);
  }

  async switchNetwork(network: "mainnet" | "devnet") {
    this.network = network;
    logger.info(`Switched to ${network.toUpperCase()}`);
    
    // Reinitialize wallet for new network
    if (this.wallet) {
      try {
        const rpcEndpoints: Record<string, string> = {
          mainnet: process.env.SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com",
          devnet: "https://api.devnet.solana.com",
        };
        
        const privateKey = process.env.SOLANA_PRIVATE_KEY;
        if (privateKey) {
          this.wallet = new Wallet({
            privateKey,
            rpcEndpoint: rpcEndpoints[network],
            network,
          });
          logger.success(`Wallet reconnected to ${network}`);
        }
      } catch (error) {
        logger.error("Failed to switch network:", error);
      }
    }
  }

  stop() {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.isRunning = false;
    this.printFinalStats();
    logger.info("Scanner stopped");
  }

  private async scan() {
    try {
      this.scanCount++;
      
      const tokens = await this.client.fetchLatestTokens(50);
      if (tokens.length === 0) return;

      const newTokens = this.client.getNewTokens(tokens);
      if (newTokens.length === 0) return;

      logger.info(`Found ${newTokens.length} new tokens`);

      const quickScored = this.filter.filterBatch(newTokens);
      if (quickScored.length === 0) {
        logger.info("No tokens passed filters this round");
        return;
      }

      const topCandidates = quickScored.slice(0, 10);
      const enriched = await this.dexScreener.enrichTokens(topCandidates);

      const finalScored = this.filter.filterBatch(enriched);
      logger.success(`${finalScored.length} tokens passed final filters`);

      for (const token of finalScored) {
        if (this.db.getToken(token.mint)) continue;
        const isNew = this.db.addToken(token);
        if (!isNew) continue;

        const buys = token.buys24h || 0;
        const sells = token.sells24h || 0;
        const volLiq = token.liquidity > 0 ? ((token.volume24h / token.liquidity) * 100).toFixed(0) : "0";
        
        const priceStr = token.price > 0 ? (token.price < 0.0001 ? `$${token.price.toExponential(2)}` : `$${token.price.toFixed(8)}`) : "N/A";
        logger.token(
          `${token.symbol} | Score: ${token.score}/100 | ${priceStr} | Liq: $${token.liquidity.toFixed(0)} | MC: $${(token.marketCap || 0).toFixed(0)} | ${buys}B/${sells}S`
        );
        
        this.onNewToken?.(token);
        
        if (token.score >= this.trading.getStrategy().minScore) {
          this.onHighScore?.(token);
          // Notify about high-score token but don't buy yet — wait for DexScreener data
          await this.telegram.notifyHighScore(token);
        } else if (token.score >= 60) {
          await this.telegram.notifyNewToken(token);
        }
      }

      await this.checkForBuyOpportunities();
      await this.checkPositions();
      this.printStats();

    } catch (error) {
      logger.error("Scan error:", error);
    }
  }

  // Check recently tracked tokens (30s-5min old) that now have DexScreener data
  private async checkForBuyOpportunities() {
    const strategy = this.trading.getStrategy();
    
    // Re-fetch latest tokens to get updated bonding curve data
    const tokens = await this.client.fetchLatestTokens(50);
    
    for (const token of tokens) {
      // Skip if already have a position
      if (this.trading.getOpenPositions().some(p => p.mint === token.mint)) continue;
      if (this.trading.getClosedPositions().some(p => p.mint === token.mint)) continue;
      const dbToken = this.db.getToken(token.mint);
      if (dbToken && dbToken.status === 'bought') continue;
      
      // Re-score with fresh bonding curve data
      const scored = this.filter.filter(token);
      if (!scored || scored.score < 50) continue;
      
      // Require meaningful buy activity from bonding curve
      const totalTxns = (token.buys24h || 0) + (token.sells24h || 0);
      if (totalTxns < 3) continue;
      
      // Try to buy
      const signal = this.trading.evaluateBuy(scored);
      if (!signal) continue;
      
      // Track this token if not already tracked
      if (!this.db.getToken(token.mint)) {
        this.db.addToken(scored);
      }
      
      const mcap = token.marketCap || 0;
      const position = this.trading.openPosition(scored, token.price, mcap);
      this.db.updateTokenStatus(scored.mint, "bought");
      const buyPriceStr = token.price < 0.0001 ? `${token.price.toExponential(2)}` : `${token.price.toFixed(8)}`;
      logger.trade(`🟢 BUY: ${scored.symbol} at ${buyPriceStr} | MCap: ${mcap.toFixed(0)} | Score: ${scored.score}/100 | Confidence: ${signal.confidence}%`);
      await this.telegram.notifyTrade("BUY", `${scored.symbol} at ${buyPriceStr} | MCap: ${mcap.toFixed(0)} | Score: ${scored.score}/100 | Confidence: ${signal.confidence}%`);
      this.onTrade?.("BUY", `${scored.symbol} at ${buyPriceStr} | Confidence: ${signal.confidence}%`);
      
      // Rate limit — one buy per scan cycle
      break;
    }
  }

  private async checkPositions() {
    const openPositions = this.trading.getOpenPositions();
    
    for (const position of openPositions) {
      // Try DexScreener first, fall back to pump.fun
      let currentPrice = 0;
      let currentMcap = 0;
      
      const dexData = await this.dexScreener.getTokenData(position.mint);
      if (dexData && dexData.price && dexData.price > 0) {
        currentPrice = dexData.price;
        currentMcap = dexData.marketCap || 0;
      } else {
        const tokens = await this.client.fetchLatestTokens(50);
        const pfToken = tokens.find(t => t.mint === position.mint);
        if (pfToken) {
          currentPrice = pfToken.price;
          currentMcap = pfToken.marketCap;
        }
      }
      
      if (currentPrice <= 0) continue;

      // Use market cap for PnL (consistent scale)
      const entryMcap = (position as any).entryMcap || position.entryPrice;
      const refPrice = currentMcap > 0 ? currentMcap : currentPrice;
      const pnlPercent = entryMcap > 0 ? ((refPrice - entryMcap) / entryMcap) * 100 : 0;
      const holdTime = Math.floor((Date.now() - position.entryTime.getTime()) / 1000);
      const mcStr = currentMcap > 0 ? "$" + currentMcap.toFixed(0) : "N/A";
      logger.info("  " + position.symbol + ": MC=" + mcStr + " PnL=" + pnlPercent.toFixed(1) + "% hold=" + holdTime + "s");

      const signal = this.trading.evaluateSell(position, refPrice);
      if (signal) {
        await this.trading.closePosition(position.mint, refPrice, signal.reasons[0]);
        this.onTrade("SELL", position.symbol + " | " + signal.reasons[0]);
        const priceStr = currentPrice < 0.0001 ? "$" + currentPrice.toExponential(2) : "$" + currentPrice.toFixed(8);
        await this.telegram.notifyTrade("SELL", position.symbol + " | Price: " + priceStr + " | MCap: $" + currentMcap.toFixed(0) + " | " + signal.reasons[0]);
      }
    }
  }

  
  private printStats() {
    const dbStats = this.db.getStats();
    const tradeStats = this.trading.getStats();
    
    logger.divider();
    logger.info(
      `📊 Scan #${this.scanCount} | Tokens: ${dbStats.total} | Positions: ${tradeStats.open} open / ${tradeStats.closed} closed`
    );
    
    if (tradeStats.closed > 0) {
      logger.info(
        `   Win Rate: ${tradeStats.winRate.toFixed(0)}% | PnL: ${tradeStats.totalPnl >= 0 ? "+" : ""}${tradeStats.totalPnl.toFixed(4)} SOL | Strategy v${tradeStats.strategyVersion}`
      );
    }
  }

  private printFinalStats() {
    const tradeStats = this.trading.getStats();
    const patterns = this.trading.getAutopsy().analyzePatterns();
    
    logger.divider();
    logger.info("FINAL STATS:");
    logger.info(`  Total trades: ${tradeStats.total}`);
    logger.info(`  Wins: ${tradeStats.wins} | Losses: ${tradeStats.losses}`);
    logger.info(`  Win rate: ${tradeStats.winRate.toFixed(1)}%`);
    logger.info(`  Total PnL: ${tradeStats.totalPnl >= 0 ? "+" : ""}${tradeStats.totalPnl.toFixed(4)} SOL`);
    logger.info(`  Strategy version: ${tradeStats.strategyVersion}`);
    
    if (patterns.recommendations.length > 0) {
      logger.divider();
      logger.info("RECOMMENDATIONS:");
      patterns.recommendations.forEach(r => logger.info(`  → ${r}`));
    }
    
    logger.divider();
  }

  getStats() {
    return {
      db: this.db.getStats(),
      trading: this.trading.getStats(),
      autopsy: this.trading.getAutopsy().getStats(),
      strategy: this.trading.getStrategy(),
    };
  }

  getWatching() {
    return this.db.getWatching();
  }
}
