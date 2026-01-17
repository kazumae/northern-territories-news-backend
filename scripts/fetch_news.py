#!/usr/bin/env python3
"""
Script to fetch RSS feeds and generate articles.json for the frontend.

Usage:
    python scripts/fetch_news.py
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import RSS_FEEDS, settings
from app.fetcher import RSSFetcher

import httpx


async def main():
    """Fetch RSS feeds and generate articles.json."""
    print("Starting RSS fetch...")
    print(f"Configured feeds: {len(RSS_FEEDS)}")
    print(f"Keywords: {settings.filter_keywords}")
    print()

    fetcher = RSSFetcher()
    all_articles = []
    feed_results = []

    async with httpx.AsyncClient(
        headers={"User-Agent": "NorthernTerritoriesNewsBot/1.0"},
        follow_redirects=True,
    ) as client:
        for feed_config in RSS_FEEDS:
            print(f"Fetching: {feed_config['name']} ({feed_config['url']})...", end=" ")
            articles, status = await fetcher.fetch_feed(client, feed_config)
            all_articles.extend(articles)
            feed_results.append(status)

            if status.success:
                print(f"OK ({status.article_count} articles)")
            else:
                print(f"FAILED: {status.error}")

    print()
    print(f"Total articles fetched: {len(all_articles)}")

    # Filter by keywords
    filtered = fetcher.filter_articles(all_articles)
    print(f"Articles matching keywords: {len(filtered)}")

    # Deduplicate
    filtered = fetcher.deduplicate_articles(filtered)
    print(f"After deduplication: {len(filtered)}")

    # Sort by date
    filtered = fetcher.sort_articles(filtered)

    # Limit
    if len(filtered) > settings.max_total_articles:
        filtered = filtered[: settings.max_total_articles]
        print(f"Limited to: {len(filtered)}")

    # Generate output
    now = datetime.now(timezone.utc)
    data = {
        "lastUpdated": now.isoformat(),
        "articles": [
            {
                "title": a.title,
                "url": str(a.url),
                "source": a.source,
                "publishedAt": a.published_at.isoformat(),
            }
            for a in filtered
        ],
    }

    # Write to frontend data directory
    frontend_path = Path(__file__).parent.parent.parent / "frontend" / "data" / "articles.json"
    frontend_path.parent.mkdir(parents=True, exist_ok=True)

    with open(frontend_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print()
    print(f"✅ Written {len(filtered)} articles to {frontend_path}")

    # Also write to backend data directory
    backend_path = Path(__file__).parent.parent / "data" / "articles.json"
    backend_path.parent.mkdir(parents=True, exist_ok=True)

    with open(backend_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Written {len(filtered)} articles to {backend_path}")

    # Summary
    print()
    print("=== Summary ===")
    successful = sum(1 for r in feed_results if r.success)
    failed = len(feed_results) - successful
    print(f"Feeds: {successful} successful, {failed} failed")
    print(f"Final articles: {len(filtered)}")

    if filtered:
        print()
        print("=== Latest Articles ===")
        for article in filtered[:5]:
            print(f"  - [{article.source}] {article.title[:50]}...")


if __name__ == "__main__":
    asyncio.run(main())
