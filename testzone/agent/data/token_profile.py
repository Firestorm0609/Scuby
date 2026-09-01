"""Token Profile Builder — combines data from all sources into enriched analysis."""

import json
import logging
from typing import Optional
from datetime import datetime

from .pumpfun import PumpFunClient
from .dexscreener import DexScreenerClient
from .solscan import SolscanClient

logger = logging.getLogger(__name__)


class TokenProfileBuilder:
    """Builds enriched token profiles from multiple data sources."""

    def __init__(self, pumpfun: PumpFunClient, dexscreener: DexScreenerClient,
                 solscan: SolscanClient):
        self.pumpfun = pumpfun
        self.dexscreener = dexscreener
        self.solscan = solscan

    async def build_profile(self, token_address: str, 
                            pumpfun_data: Optional[dict] = None) -> dict:
        """Build a complete token profile from all available data.
        
        Args:
            token_address: The token mint address
            pumpfun_data: Optional pre-fetched pump.fun data
            
        Returns:
            Enriched token profile dict
        """
        # Fetch data from all sources concurrently
        if pumpfun_data is None:
            pumpfun_data = await self.pumpfun.get_token(token_address)
        
        dex_data = await self.dexscreener.get_token_price(token_address)
        holder_data = await self.solscan.get_token_holders(token_address)

        # Build the profile
        profile = {
            "address": token_address,
            "name": "",
            "symbol": "",
            "analyzed_at": datetime.utcnow().isoformat(),
        }

        # Pump.fun data
        if pumpfun_data:
            profile["name"] = pumpfun_data.get("name", "")
            profile["symbol"] = pumpfun_data.get("symbol", "")
            profile["description"] = pumpfun_data.get("description", "")
            profile["website"] = pumpfun_data.get("website", "")
            profile["twitter"] = pumpfun_data.get("twitter", "")
            profile["telegram"] = pumpfun_data.get("telegram", "")
            profile["creator"] = pumpfun_data.get("creator", "")
            profile["pumpfun_market_cap_sol"] = pumpfun_data.get("market_cap_sol", 0)
            profile["pumpfun_usd_mcap"] = pumpfun_data.get("usd_market_cap", 0)
            profile["complete"] = pumpfun_data.get("complete", False)  # graduated to Raydium

        # DexScreener data
        if dex_data:
            profile["market_data"] = {
                "price_usd": dex_data.get("price_usd", 0),
                "market_cap_usd": dex_data.get("market_cap_usd", 0),
                "fdv": dex_data.get("fdv", 0),
                "liquidity_usd": dex_data.get("liquidity_usd", 0),
                "volume_24h": dex_data.get("volume_24h", 0),
                "volume_1h": dex_data.get("volume_1h", 0),
                "price_change_5m": dex_data.get("price_change_5m", 0),
                "price_change_1h": dex_data.get("price_change_1h", 0),
                "txns_5m_buys": dex_data.get("txns_5m_buys", 0),
                "txns_5m_sells": dex_data.get("txns_5m_sells", 0),
                "pair_address": dex_data.get("pair_address", ""),
            }
        else:
            profile["market_data"] = {}

        # Holder data
        if holder_data:
            profile["holder_data"] = {
                "total_holders": holder_data.get("total_holders", 0),
                "top_10_concentration": holder_data.get("top_10_concentration", 1.0),
            }
        else:
            profile["holder_data"] = {"total_holders": 0, "top_10_concentration": 1.0}

        # Calculate risk flags
        profile["risk_flags"] = self._calculate_risk_flags(profile)

        # Calculate a simple risk score (0 = safe, 100 = very risky)
        profile["risk_score"] = self._calculate_risk_score(profile)

        return profile

    def _calculate_risk_flags(self, profile: dict) -> list[str]:
        """Identify risk flags based on profile data."""
        flags = []
        holder_data = profile.get("holder_data", {})
        market_data = profile.get("market_data", {})

        # Holder risks
        if holder_data.get("total_holders", 0) < 10:
            flags.append("low_holder_count")
        
        if holder_data.get("top_10_concentration", 1.0) > 0.7:
            flags.append("high_concentration")

        # Market risks
        if market_data.get("liquidity_usd", 0) < 1000:
            flags.append("low_liquidity")
        
        if market_data.get("price_change_5m", 0) > 500:
            flags.append("pump_and_dump_risk")

        # Social risks
        if not profile.get("twitter") and not profile.get("telegram"):
            flags.append("no_social_presence")

        if not profile.get("website"):
            flags.append("no_website")

        # Creator risks
        if not profile.get("creator"):
            flags.append("unknown_creator")

        return flags

    def _calculate_risk_score(self, profile: dict) -> int:
        """Calculate a risk score from 0-100."""
        score = 0
        flags = profile.get("risk_flags", [])
        
        flag_scores = {
            "low_holder_count": 20,
            "high_concentration": 25,
            "low_liquidity": 25,
            "pump_and_dump_risk": 30,
            "no_social_presence": 10,
            "no_website": 5,
            "unknown_creator": 15,
        }
        
        for flag in flags:
            score += flag_scores.get(flag, 10)
        
        return min(score, 100)

    def profile_to_prompt(self, profile: dict) -> str:
        """Convert a profile to a human-readable string for the LLM."""
        market = profile.get("market_data", {})
        holders = profile.get("holder_data", {})
        
        return f"""
TOKEN: {profile.get('name', 'Unknown')} ({profile.get('symbol', '?')})
Address: {profile.get('address', 'unknown')}

MARKET DATA:
- Price: ${market.get('price_usd', 0):.10f}
- Market Cap: ${market.get('market_cap_usd', 0):,.0f}
- Liquidity: ${market.get('liquidity_usd', 0):,.0f}
- Volume (24h): ${market.get('volume_24h', 0):,.0f}
- Price change (5m): {market.get('price_change_5m', 0):+.1f}%
- Price change (1h): {market.get('price_change_1h', 0):+.1f}%
- Buy/Sell ratio (5m): {market.get('txns_5m_buys', 0)} buys / {market.get('txns_5m_sells', 0)} sells

HOLDERS:
- Total holders: {holders.get('total_holders', 0)}
- Top 10 concentration: {holders.get('top_10_concentration', 0):.1%}

SOCIAL:
- Twitter: {profile.get('twitter', 'None')}
- Telegram: {profile.get('telegram', 'None')}
- Website: {profile.get('website', 'None')}

RISK FLAGS: {', '.join(profile.get('risk_flags', [])) or 'None'}
RISK SCORE: {profile.get('risk_score', 0)}/100
"""
