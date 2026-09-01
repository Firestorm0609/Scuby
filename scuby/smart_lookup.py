"""
smart_lookup.py — Dynamic token lookup that searches the web and reads
multiple sources, like a real analyst would.

Not hardcoded to one API. Searches the web, reads Birdeye, GMGN, Solscan,
Twitter/X, and synthesizes everything into a real answer.
"""

import asyncio
import logging
import re

import httpx

logger = logging.getLogger(__name__)


async def smart_token_lookup(
    ca: str,
    symbol: str | None = None,
    http: httpx.AsyncClient | None = None,
) -> dict:
    """
    Dynamic token lookup — searches the web and reads multiple sources.
    Returns a dict with all gathered context for the AI to synthesize.
    """
    results = {
        "ca": ca,
        "symbol": symbol,
        "chain": "evm" if ca.startswith("0x") else "solana",
        "on_chain": {},
        "web_results": [],
        "x_results": [],
        "pages_fetched": [],
    }

    # Build search queries based on what we know
    search_queries = []
    if symbol:
        search_queries.append(f"{symbol} crypto token")
        search_queries.append(f"${symbol} solana" if results["chain"] == "solana" else f"${symbol} crypto")
    search_queries.append(f"token {ca[:12]}... crypto")

    # 1. Fetch on-chain data from DexScreener
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            if resp.status_code == 200:
                data = resp.json()
                pairs = data.get("pairs", [])
                if pairs:
                    p = pairs[0]
                    results["on_chain"] = {
                        "symbol": p.get("baseToken", {}).get("symbol", ""),
                        "name": p.get("baseToken", {}).get("name", ""),
                        "price": p.get("priceUsd", "?"),
                        "mcap": p.get("marketCap") or p.get("fdv", 0),
                        "liquidity": (p.get("liquidity") or {}).get("usd", 0),
                        "volume_24h": (p.get("volume") or {}).get("h24", 0),
                        "volume_1h": (p.get("volume") or {}).get("h1", 0),
                        "change_1h": (p.get("priceChange") or {}).get("h1", 0),
                        "change_24h": (p.get("priceChange") or {}).get("h24", 0),
                        "chain": p.get("chainId", "?"),
                        "dex": p.get("dexId", "?"),
                        "pair_address": p.get("pairAddress", ""),
                        "age_hours": _calc_age(p.get("pairCreatedAt")),
                    }
                    # Update symbol if we got it from on-chain
                    if not results["symbol"]:
                        results["symbol"] = results["on_chain"].get("symbol")
                        search_queries.insert(0, f"{results['symbol']} crypto token")
    except Exception as e:
        logger.warning(f"DexScreener lookup failed: {e}")

    # 2. Search the web
    try:
        from ai import web_search, x_search, fetch_url

        # Run web search and X search in parallel
        web_task = asyncio.gather(*[
            web_search(q, max_results=3) for q in search_queries[:2]
        ], return_exceptions=True)
        x_task = x_search(results["symbol"] or ca[:8], max_results=3)

        web_results_raw, x_results_raw = await asyncio.gather(web_task, x_task)

        # Flatten web results
        seen_urls = set()
        for wr in web_results_raw:
            if isinstance(wr, list):
                for r in wr:
                    url = r.get("href", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        results["web_results"].append({
                            "title": r.get("title", ""),
                            "body": r.get("body", "")[:300],
                            "url": url,
                        })

        results["x_results"] = [
            {"title": r.get("title", ""), "body": r.get("body", "")[:300], "url": r.get("href", "")}
            for r in (x_results_raw if isinstance(x_results_raw, list) else [])
        ]

        # 3. Fetch top 2 web pages for deeper context
        fetch_tasks = []
        for r in results["web_results"][:2]:
            url = r["url"]
            if url and not any(skip in url for skip in ["x.com", "twitter.com", "reddit.com"]):
                fetch_tasks.append(fetch_url(url, max_chars=2000))
            else:
                fetch_tasks.append(asyncio.sleep(0))

        fetched = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        results["pages_fetched"] = [f if isinstance(f, str) else "" for f in fetched]

    except Exception as e:
        logger.warning(f"Web search failed: {e}")

    return results


def _calc_age(created_at_ms: int | None) -> float:
    """Calculate age in hours from pair creation timestamp."""
    if not created_at_ms:
        return 0.0
    import time
    return (time.time() * 1000 - created_at_ms) / 3_600_000


def format_lookup_context(results: dict) -> str:
    """
    Format gathered context into a string the AI can use to give a real answer.
    This is what gets injected into the AI prompt.
    """
    lines = []

    # On-chain data
    oc = results.get("on_chain", {})
    if oc:
        lines.append(f"ON-CHAIN DATA for {oc.get('symbol', '?')} on {oc.get('chain', '?')}:")
        lines.append(f"- Price: ${oc.get('price', '?')}")
        lines.append(f"- MCap: ${oc.get('mcap', 0):,.0f}")
        lines.append(f"- Liquidity: ${oc.get('liquidity', 0):,.0f}")
        lines.append(f"- Volume 24h: ${oc.get('volume_24h', 0):,.0f} | 1h: ${oc.get('volume_1h', 0):,.0f}")
        lines.append(f"- 1h change: {oc.get('change_1h', 0):+.1f}% | 24h: {oc.get('change_24h', 0):+.1f}%")
        lines.append(f"- Age: {oc.get('age_hours', 0):.1f}h")
        lines.append(f"- DEX: {oc.get('dex', '?')}")
        lines.append("")

    # Web results
    if results.get("web_results"):
        lines.append("WEB SEARCH RESULTS:")
        for r in results["web_results"][:3]:
            lines.append(f"- {r['title']}")
            lines.append(f"  {r['body']}")
            lines.append(f"  {r['url']}")
        lines.append("")

    # X/Twitter
    if results.get("x_results"):
        lines.append("X/TWITTER POSTS:")
        for r in results["x_results"][:3]:
            lines.append(f"- {r['title']}")
            lines.append(f"  {r['body']}")
        lines.append("")

    # Fetched page content
    for i, page in enumerate(results.get("pages_fetched", [])):
        if page:
            lines.append(f"ARTICLE {i+1} CONTENT:")
            lines.append(page[:1500])
            lines.append("")

    return "\n".join(lines)
