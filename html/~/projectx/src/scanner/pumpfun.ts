/* =========================================================
   ProjectX — Pump.fun API Client
========================================================= */

import { Token } from "../types/index";
import { config } from "../config";
import { logger } from "../utils/logger";

interface PumpFunCoin {
  mint: string;
  name: string;
  symbol: string;
  uri: string;
  image: string;
  creator: string;
  created_timestamp: number;
  market_cap: number;
  price: number;
  volume_24h: number;
  liquidity: number;
  holders: number;
  txns_24h: number;
  buys_24h: number;
  sells_24h: number;
  website: string | null;
  twitter: string | null;
  telegram: string | null;
  complete: boolean;
}

function mapCoinToToken(coin: PumpFunCoin): Token {
  return {
    mint: coin.mint,
    name: coin.name,
    symbol: coin.symbol,
    uri: coin.uri,
    image: coin.image,
    creator: coin.creator,
    createdAt: new Date(coin.created_timestamp),
    marketCap: coin.market_cap,
    price: coin.price,
    volume24h: coin.volume_24h,
    liquidity: coin.liquidity,
    holders: coin.holders,
    txns24h: coin.txns_24h,
    buys24h: coin.buys_24h,
    sells24h: coin.sells_24h,
    website: coin.website || undefined,
    twitter: coin.twitter || undefined,
    telegram: coin.telegram || undefined,
    websiteVerified: !!coin.website,
    twitterVerified: !!coin.twitter,
    telegramVerified: !!coin.telegram,
  };
}

export class PumpFunClient {
  private baseUrl: string;
  private knownMints: Set<string> = new Set();

  constructor() {
    this.baseUrl = config.pumpfun.baseUrl;
  }

  async fetchLatestTokens(limit: number = 50): Promise<Token[]> {
    try {
      const url = `${this.baseUrl}${config.pumpfun.allTokensEndpoint}?limit=${limit}&offset=0&sort=created_timestamp&order=DESC`;
      
      const response = await fetch(url, {
        headers: {
          "Accept": "application/json",
          "User-Agent": "ProjectX/1.0",
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json() as PumpFunCoin[];
      return data.map(mapCoinToToken);
    } catch (error) {
      logger.error("Failed to fetch tokens from Pump.fun:", error);
      return [];
    }
  }

  async fetchNewTokens(): Promise<Token[]> {
    try {
      const url = `${this.baseUrl}${config.pumpfun.newTokenEndpoint}`;
      
      const response = await fetch(url, {
        headers: {
          "Accept": "application/json",
          "User-Agent": "ProjectX/1.0",
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json() as PumpFunCoin[];
      return data.map(mapCoinToToken);
    } catch (error) {
      logger.error("Failed to fetch new tokens:", error);
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
