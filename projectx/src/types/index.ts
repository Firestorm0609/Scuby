export interface Token {
  mint: string;
  name: string;
  symbol: string;
  uri: string;
  image?: string;
  creator: string;
  createdAt: Date;
  marketCap: number;
  price: number;
  volume24h: number;
  liquidity: number;
  holders: number;
  txns24h: number;
  buys24h: number;
  sells24h: number;
  website?: string;
  twitter?: string;
  telegram?: string;
  websiteVerified: boolean;
  twitterVerified: boolean;
  telegramVerified: boolean;
}

export interface TokenWithScore extends Token {
  score: number;
  reasons: string[];
  tracked: boolean;
  trackedAt?: Date;
}

export interface FilterConfig {
  minLiquidityUsd: number;
  maxTokenAgeSeconds: number;
  minVolume24h: number;
  minHolders: number;
  requireSocials: boolean;
  requireWebsite: boolean;
  minBuySellRatio: number;
}

export interface TradeDecision {
  token: TokenWithScore;
  action: "BUY" | "SKIP" | "WATCH";
  confidence: number;
  reasons: string[];
  timestamp: Date;
}

export interface ScannerEvent {
  type: "NEW_TOKEN" | "UPDATE" | "DELISTED";
  token: Token;
  timestamp: Date;
}
