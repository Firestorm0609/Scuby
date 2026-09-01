"""
socials.py — Free social media APIs for Scuba.

All APIs used here are FREE with no credit card required:
  - Reddit: official API (100 req/min, generous)
  - YouTube: Data API v3 (10K units/day free)
  - GitHub: REST API (5K req/hr authenticated)
  - CoinGecko: free tier (10-30 req/min)
  - X/Twitter: DuckDuckGo site:x.com queries (improved)
  - Any URL: fetch + read (built-in)
"""

import asyncio
import logging
import os
import re
import time

import httpx

logger = logging.getLogger(__name__)


# ─── Reddit (official free API) ─────────────────────────────────────────────

_REDDIT_HEADERS = {
    "User-Agent": "ScubaBot/1.0 (crypto research bot)",
}


async def reddit_search(query: str, subreddit: str = "all", limit: int = 5) -> list[dict]:
    """
    Search Reddit for posts. Falls back to DuckDuckGo if API blocked.
    Returns list of {title, body, url, score, comments, subreddit, author}.
    """
    # Try official API first
    try:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "limit": limit, "sort": "new", "t": "week"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params, headers=_REDDIT_HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                posts = []
                for child in data.get("data", {}).get("children", []):
                    d = child.get("data", {})
                    posts.append({
                        "title": d.get("title", ""),
                        "body": d.get("selftext", "")[:500],
                        "url": f"https://reddit.com{d.get('permalink', '')}",
                        "score": d.get("score", 0),
                        "comments": d.get("num_comments", 0),
                        "subreddit": d.get("subreddit", ""),
                        "author": d.get("author", ""),
                    })
                return posts
    except Exception:
        pass

    # Fallback: DuckDuckGo site:reddit.com
    try:
        from ai import web_search
        results = await web_search(f"site:reddit.com {query}", max_results=limit)
        posts = []
        for r in results:
            url = r.get("href", "")
            if "reddit.com" in url:
                posts.append({
                    "title": r.get("title", ""),
                    "body": r.get("body", "")[:300],
                    "url": url,
                    "score": 0,
                    "comments": 0,
                    "subreddit": "",
                    "author": "",
                })
        return posts
    except Exception as e:
        logger.warning(f"reddit_search failed: {e}")
        return []


async def reddit_user_posts(username: str, limit: int = 5) -> list[dict]:
    """Get recent posts from a Reddit user. Free."""
    try:
        url = f"https://www.reddit.com/user/{username}/submitted.json"
        params = {"limit": limit, "sort": "new"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params, headers=_REDDIT_HEADERS)
            if resp.status_code != 200:
                return []
            data = resp.json()
            posts = []
            for child in data.get("data", {}).get("children", []):
                d = child.get("data", {})
                posts.append({
                    "title": d.get("title", ""),
                    "body": d.get("selftext", "")[:500],
                    "url": f"https://reddit.com{d.get('permalink', '')}",
                    "score": d.get("score", 0),
                    "subreddit": d.get("subreddit", ""),
                })
            return posts
    except Exception as e:
        logger.warning(f"reddit_user_posts failed: {e}")
        return []


# ─── YouTube (free Data API v3) ─────────────────────────────────────────────

async def youtube_search(query: str, max_results: int = 5) -> list[dict]:
    """
    Search YouTube for videos. Free (10K units/day).
    Returns list of {title, channel, url, views, published, description}.
    """
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        # Fallback: use DuckDuckGo
        return await _youtube_search_fallback(query, max_results)

    try:
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": max_results,
            "order": "date",
            "key": api_key,
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                logger.warning(f"YouTube search failed: {resp.status_code}")
                return []
            data = resp.json()
            videos = []
            for item in data.get("items", []):
                snippet = item.get("snippet", {})
                video_id = item.get("id", {}).get("videoId", "")
                videos.append({
                    "title": snippet.get("title", ""),
                    "channel": snippet.get("channelTitle", ""),
                    "url": f"https://youtube.com/watch?v={video_id}",
                    "published": snippet.get("publishedAt", ""),
                    "description": snippet.get("description", "")[:300],
                })
            return videos
    except Exception as e:
        logger.warning(f"youtube_search failed: {e}")
        return []


async def _youtube_search_fallback(query: str, max_results: int = 5) -> list[dict]:
    """Fallback YouTube search via DuckDuckGo."""
    try:
        from ai import web_search
        results = await web_search(f"site:youtube.com {query}", max_results=max_results)
        videos = []
        for r in results:
            url = r.get("href", "")
            if "youtube.com/watch" in url or "youtu.be/" in url:
                videos.append({
                    "title": r.get("title", ""),
                    "channel": "",
                    "url": url,
                    "published": "",
                    "description": r.get("body", "")[:300],
                })
        return videos
    except Exception as e:
        logger.warning(f"youtube_search_fallback failed: {e}")
        return []


# ─── GitHub (free REST API) ─────────────────────────────────────────────────

async def github_search_repos(query: str, limit: int = 5) -> list[dict]:
    """
    Search GitHub repos. Free (60 req/hr unauthenticated, 5K authenticated).
    Returns list of {name, description, url, stars, language, updated}.
    """
    token = os.environ.get("GITHUB_TOKEN", "")
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        url = "https://api.github.com/search/repositories"
        params = {"q": query, "sort": "stars", "order": "desc", "per_page": limit}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code != 200:
                logger.warning(f"GitHub search failed: {resp.status_code}")
                return []
            data = resp.json()
            repos = []
            for item in data.get("items", [])[:limit]:
                repos.append({
                    "name": item.get("full_name", ""),
                    "description": item.get("description", "")[:200],
                    "url": item.get("html_url", ""),
                    "stars": item.get("stargazers_count", 0),
                    "language": item.get("language", ""),
                    "updated": item.get("updated_at", ""),
                })
            return repos
    except Exception as e:
        logger.warning(f"github_search_repos failed: {e}")
        return []


async def github_user_repos(username: str, limit: int = 5) -> list[dict]:
    """Get repos for a GitHub user. Free."""
    token = os.environ.get("GITHUB_TOKEN", "")
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        url = f"https://api.github.com/users/{username}/repos"
        params = {"sort": "updated", "per_page": limit}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code != 200:
                return []
            repos = []
            for item in resp.json()[:limit]:
                repos.append({
                    "name": item.get("full_name", ""),
                    "description": item.get("description", "")[:200],
                    "url": item.get("html_url", ""),
                    "stars": item.get("stargazers_count", 0),
                    "language": item.get("language", ""),
                })
            return repos
    except Exception as e:
        logger.warning(f"github_user_repos failed: {e}")
        return []


# ─── CoinGecko (free crypto data) ──────────────────────────────────────────

async def coingecko_search(query: str) -> list[dict]:
    """
    Search CoinGecko for coins. Free (10-30 req/min).
    Returns list of {id, name, symbol, market_cap_rank}.
    """
    try:
        url = "https://api.coingecko.com/api/v3/search"
        params = {"query": query}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                logger.warning(f"CoinGecko search failed: {resp.status_code}")
                return []
            data = resp.json()
            coins = []
            for coin in data.get("coins", [])[:5]:
                coins.append({
                    "id": coin.get("id", ""),
                    "name": coin.get("name", ""),
                    "symbol": coin.get("symbol", ""),
                    "market_cap_rank": coin.get("market_cap_rank"),
                })
            return coins
    except Exception as e:
        logger.warning(f"coingecko_search failed: {e}")
        return []


async def coingecko_price(coin_id: str) -> dict:
    """
    Get current price and market data for a coin. Free.
    Returns {price, mcap, volume_24h, change_24h, change_7d}.
    """
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": coin_id,
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
            "include_7d_change": "true",
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return {}
            data = resp.json()
            coin_data = data.get(coin_id, {})
            return {
                "price": coin_data.get("usd", 0),
                "mcap": coin_data.get("usd_market_cap", 0),
                "volume_24h": coin_data.get("usd_24h_vol", 0),
                "change_24h": coin_data.get("usd_24h_change", 0),
                "change_7d": coin_data.get("usd_7d_change", 0),
            }
    except Exception as e:
        logger.warning(f"coingecko_price failed: {e}")
        return {}


# ─── X/Twitter (improved DuckDuckGo search) ────────────────────────────────

async def x_search_improved(query: str, max_results: int = 5) -> list[dict]:
    """
    Improved X/Twitter search using multiple DuckDuckGo queries.
    Returns list of {title, body, url, author, date}.
    """
    try:
        from ai import web_search

        # Build multiple targeted queries
        queries = [
            f"site:x.com {query}",
            f"site:twitter.com {query}",
            f"x.com {query} latest",
        ]

        all_results = []
        seen_urls = set()

        for q in queries:
            results = await web_search(q, max_results=max_results)
            for r in results:
                url = r.get("href", "")
                if url and url not in seen_urls and ("x.com" in url or "twitter.com" in url):
                    seen_urls.add(url)
                    # Extract author from URL
                    author = ""
                    author_match = re.search(r'(?:x\.com|twitter\.com)/(\w+)', url)
                    if author_match:
                        author = author_match.group(1)
                    all_results.append({
                        "title": r.get("title", ""),
                        "body": r.get("body", "")[:300],
                        "url": url,
                        "author": author,
                    })

        return all_results[:max_results]
    except Exception as e:
        logger.warning(f"x_search_improved failed: {e}")
        return []


# ─── Fear & Greed Index (free) ─────────────────────────────────────────────

async def fear_greed_index() -> dict:
    """
    Get crypto Fear & Greed Index. Free, no API key.
    Returns {value, classification, timestamp}.
    """
    try:
        url = "https://api.alternative.me/fng/"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return {}
            data = resp.json()
            entry = data.get("data", [{}])[0]
            return {
                "value": int(entry.get("value", 0)),
                "classification": entry.get("value_classification", ""),
                "timestamp": entry.get("timestamp", ""),
            }
    except Exception as e:
        logger.warning(f"fear_greed_index failed: {e}")
        return {}


# ─── Unified social search ──────────────────────────────────────────────────

async def search_everywhere(query: str, platforms: list[str] | None = None) -> dict:
    """
    Search across ALL platforms simultaneously.
    Returns dict with results from each platform.
    """
    if platforms is None:
        platforms = ["reddit", "youtube", "github", "coingecko", "x"]

    tasks = {}

    if "reddit" in platforms:
        tasks["reddit"] = reddit_search(query, limit=3)
    if "youtube" in platforms:
        tasks["youtube"] = youtube_search(query, max_results=3)
    if "github" in platforms:
        tasks["github"] = github_search_repos(query, limit=3)
    if "coingecko" in platforms:
        tasks["coingecko"] = coingecko_search(query)
    if "x" in platforms:
        tasks["x"] = x_search_improved(query, max_results=3)

    results = {}
    if tasks:
        gathered = await asyncio.gather(
            *tasks.values(),
            return_exceptions=True,
        )
        for platform, result in zip(tasks.keys(), gathered):
            if isinstance(result, Exception):
                logger.warning(f"search_everywhere {platform} failed: {result}")
                results[platform] = []
            else:
                results[platform] = result

    return results


def format_social_results(results: dict, query: str) -> str:
    """
    Format social search results into a readable string for the AI.
    """
    lines = []

    if results.get("reddit"):
        lines.append("REDDIT:")
        for r in results["reddit"][:3]:
            lines.append(f"  [{r['score']}↑ {r['comments']}💬] {r['title']}")
            lines.append(f"    r/{r['subreddit']} — {r['url']}")
            if r.get("body"):
                lines.append(f"    {r['body'][:150]}")
        lines.append("")

    if results.get("youtube"):
        lines.append("YOUTUBE:")
        for v in results["youtube"][:3]:
            lines.append(f"  {v['title']}")
            lines.append(f"    {v['channel']} — {v['url']}")
        lines.append("")

    if results.get("github"):
        lines.append("GITHUB:")
        for r in results["github"][:3]:
            lines.append(f"  ⭐ {r['stars']} {r['name']}")
            lines.append(f"    {r['description'][:100]}")
            lines.append(f"    {r['url']}")
        lines.append("")

    if results.get("coingecko"):
        lines.append("COINGECKO:")
        for c in results["coingecko"][:3]:
            rank = f"#{c['market_cap_rank']}" if c.get("market_cap_rank") else "?"
            lines.append(f"  {c['name']} ({c['symbol'].upper()}) — Rank {rank}")
        lines.append("")

    if results.get("x"):
        lines.append("X/TWITTER:")
        for r in results["x"][:3]:
            lines.append(f"  @{r['author']}: {r['title'][:80]}")
            lines.append(f"    {r['url']}")
            if r.get("body"):
                lines.append(f"    {r['body'][:150]}")
        lines.append("")

    if not lines:
        return "No results found across any platform."

    return "\n".join(lines)
