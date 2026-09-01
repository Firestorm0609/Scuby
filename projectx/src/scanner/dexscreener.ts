import { Token } from "../types/index";
import { logger } from "../utils/logger";

interface DexScreenerPair {
  chainId: string;
  dexId: string;
  url: string;
  pairAddress: string;
  baseToken: {
    address: string;
    name: string;
    symbol: string;
  };
  priceUsd: string;
  txns: {
    h24: { buys: number; sells: number };
    h6: { buys: number; sells: number };
    h1: { buys: number; sells: number };
  };
  volume: {
    h24: number;
    h6: number;
    h1: number;
  };
  priceChange: {
    h24: number;
    h6: number;
    h1: number;
  };
  liquidity: {
    usd: number;
  };
  fdv: number;
  marketCap: number;
  pairCreatedAt: number;
  info?: {
    imageUrl?: string;
    websites?: { url: string }[];
    socials?: { type: string; url: string }[];
  };
}

export class DexScreenerClient {
  private lastFetchTime = 0;
  private fetchDelay = 1000;
  private cache: Map<string, { data: Partial<Token>; timestamp: number }> = new Map();
  private cacheTTL = 30000; // 30 seconds

  async getTokenData(mint: string): Promise<Partial<Token> | null> {
    // Check cache first
    const cached = this.cache.get(mint);
    if (cached && Date.now() - cached.timestamp < this.cacheTTL) {
      return cached.data;
    }

    try {
      const now = Date.now();
      const timeSinceLastFetch = now - this.lastFetchTime;
      if (timeSinceLastFetch < this.fetchDelay) {
        await new Promise(resolve => setTimeout(resolve, this.fetchDelay - timeSinceLastFetch));
      }

      const url = `https://api.dexscreener.com/tokens/v1/solana/${mint}`;
      const response = await fetch(url);
      this.lastFetchTime = Date.now();

      if (!response.ok) {
        return null;
      }

      const pairs = await response.json() as DexScreenerPair[];
      if (!pairs || pairs.length === 0) {
        return null;
      }

      // Find the pair with highest liquidity
      const bestPair = pairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];

      const data: Partial<Token> = {
        volume24h: bestPair.volume?.h24 || 0,
        liquidity: bestPair.liquidity?.usd || 0,
        buys24h: bestPair.txns?.h24?.buys || 0,
        sells24h: bestPair.txns?.h24?.sells || 0,
        txns24h: (bestPair.txns?.h24?.buys || 0) + (bestPair.txns?.h24?.sells || 0),
        marketCap: bestPair.marketCap || bestPair.fdv || 0,
        price: parseFloat(bestPair.priceUsd) || 0,
        twitter: bestPair.info?.socials?.find(s => s.type === "twitter")?.url,
        telegram: bestPair.info?.socials?.find(s => s.type === "telegram")?.url,
        website: bestPair.info?.websites?.[0]?.url,
      };

      // Cache the result
      this.cache.set(mint, { data, timestamp: Date.now() });

      return data;
    } catch (error) {
      logger.error(`DexScreener fetch failed for ${mint}:`, error);
      return null;
    }
  }

  async enrichTokens(tokens: Token[]): Promise<Token[]> {
    const enriched: Token[] = [];

    for (const token of tokens) {
      const extra = await this.getTokenData(token.mint);
      if (extra) {
        enriched.push({ ...token, ...extra });
      } else {
        enriched.push(token);
      }
    }

    return enriched;
  }
}
