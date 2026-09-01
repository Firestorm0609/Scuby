import { Token } from "../types/index";
import { logger } from "../utils/logger";

interface PumpFunCoin {
  mint: string;
  name: string;
  symbol: string;
  description: string;
  image_uri: string;
  metadata_uri: string;
  creator: string;
  created_timestamp: number;
  complete: boolean;
  virtual_sol_reserves: number;
  virtual_token_reserves: number;
  real_sol_reserves: number;
  real_token_reserves: number;
  total_supply: number;
  market_cap: number;
  usd_market_cap: number;
  ath_market_cap: number;
  last_trade_timestamp: number;
  reply_count: number;
  is_currently_live: boolean;
  nsfw: boolean;
  is_banned: boolean;
  twitter?: string;
  website?: string;
  telegram?: string;
}

// Initial bonding curve state
const INITIAL_VIRTUAL_TOKENS = 1_073_000_000_000_000;

export class PumpFunClient {
  private knownMints: Set<string> = new Set();
  private lastFetchTime = 0;
  private fetchDelay = 2000;

  async fetchLatestTokens(limit: number = 50): Promise<Token[]> {
    try {
      const now = Date.now();
      const timeSinceLastFetch = now - this.lastFetchTime;
      if (timeSinceLastFetch < this.fetchDelay) {
        await new Promise(resolve => setTimeout(resolve, this.fetchDelay - timeSinceLastFetch));
      }

      const url = `https://frontend-api-v3.pump.fun/coins?limit=${limit}&offset=0&sort=created_timestamp&order=DESC`;
      const response = await fetch(url, {
        headers: {
          "Accept": "application/json",
          "User-Agent": "ProjectX/1.0",
        },
      });
      this.lastFetchTime = Date.now();

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json() as PumpFunCoin[];
      
      if (!Array.isArray(data)) return [];

      return data.map(coin => {
        const solPrice = 150;
        const virtualSol = coin.virtual_sol_reserves / 1e9;
        const virtualToken = coin.virtual_token_reserves;
        const price = virtualToken > 0 ? (virtualSol / virtualToken) * solPrice : 0;

        // Calculate bonding curve activity
        const tokenDepletion = Math.max(0, 1 - (coin.virtual_token_reserves / INITIAL_VIRTUAL_TOKENS));
        const solDeposited = Math.max(0, (coin.real_sol_reserves - 1) / 1e9); // Real SOL deposited by traders
        
        // Estimate realistic buy count based on bonding curve depletion
        // Most pump.fun buys are small (0.01-0.1 SOL), so we estimate conservatively
        let estimatedBuys = 0;
        if (tokenDepletion > 0.30) estimatedBuys = Math.floor(100 + tokenDepletion * 200); // Very active
        else if (tokenDepletion > 0.15) estimatedBuys = Math.floor(30 + tokenDepletion * 150);
        else if (tokenDepletion > 0.05) estimatedBuys = Math.floor(5 + tokenDepletion * 100);
        else if (tokenDepletion > 0.01) estimatedBuys = Math.floor(1 + tokenDepletion * 50);
        
        // Buy ratio: on pump.fun, early activity is almost all buys
        // Only estimate ratio if we have meaningful data
        let estimatedBuyRatio = 0;
        if (estimatedBuys > 10) {
          // Higher depletion = more price impact = higher buy pressure
          estimatedBuyRatio = Math.min(0.95, 0.6 + tokenDepletion * 0.5);
        }

        // Volume estimate: real SOL deposited * price
        const volumeEstimate = solDeposited * solPrice;

        // Market cap trend: is it above or below ATH?
        const mcTrend = coin.ath_market_cap > 0 
          ? (coin.usd_market_cap / coin.ath_market_cap) 
          : 1;

        return {
          mint: coin.mint,
          name: coin.name,
          symbol: coin.symbol,
          uri: coin.metadata_uri,
          image: coin.image_uri,
          creator: coin.creator,
          createdAt: new Date(coin.created_timestamp),
          marketCap: coin.usd_market_cap || coin.market_cap || 0,
          price,
          volume24h: volumeEstimate,
          liquidity: virtualSol * solPrice,
          holders: 0,
          txns24h: estimatedBuys,
          buys24h: Math.floor(estimatedBuys * estimatedBuyRatio),
          sells24h: Math.floor(estimatedBuys * (1 - estimatedBuyRatio)),
          twitter: coin.twitter,
          website: coin.website,
          telegram: coin.telegram,
          websiteVerified: false,
          twitterVerified: false,
          telegramVerified: false,
        };
      });
    } catch (error) {
      logger.error("Failed to fetch tokens:", error);
      return [];
    }
  }

  getNewTokens(tokens: Token[]): Token[] {
    const newTokens: Token[] = [];
    for (const token of tokens) {
      if (!this.knownMints.has(token.mint)) {
        this.knownMints.add(token.mint);
        newTokens.push(token);
      }
    }
    return newTokens;
  }

  getTokenCount(): number {
    return this.knownMints.size;
  }
}
