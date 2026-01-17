"""
Article service for managing news data.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.config import settings
from app.fetcher import fetcher
from app.models import Article, ArticleResponse, FetchResult

logger = logging.getLogger(__name__)

# In-memory storage for articles
_articles: list[Article] = []
_last_updated: datetime = datetime.now(timezone.utc)


def get_data_file_path() -> Path:
    """Get the path to the data file."""
    return Path(__file__).parent.parent / "data" / "articles.json"


def load_articles_from_file() -> None:
    """Load articles from the JSON file if it exists."""
    global _articles, _last_updated

    data_file = get_data_file_path()
    if data_file.exists():
        try:
            with open(data_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            _articles = [
                Article(
                    title=a["title"],
                    url=a["url"],
                    source=a["source"],
                    published_at=datetime.fromisoformat(a["publishedAt"].replace("Z", "+00:00")),
                )
                for a in data.get("articles", [])
            ]
            _last_updated = datetime.fromisoformat(
                data.get("lastUpdated", datetime.now(timezone.utc).isoformat()).replace("Z", "+00:00")
            )
            logger.info(f"Loaded {len(_articles)} articles from file")
        except Exception as e:
            logger.error(f"Failed to load articles from file: {e}")
            _articles = []
            _last_updated = datetime.now(timezone.utc)


def save_articles_to_file() -> None:
    """Save articles to the JSON file."""
    data_file = get_data_file_path()
    data_file.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "lastUpdated": _last_updated.isoformat(),
        "articles": [
            {
                "title": a.title,
                "url": str(a.url),
                "source": a.source,
                "publishedAt": a.published_at.isoformat(),
            }
            for a in _articles
        ],
    }

    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(_articles)} articles to file")


def export_to_frontend() -> None:
    """Export articles to the frontend data directory."""
    frontend_data_file = Path(__file__).parent.parent.parent / "frontend" / "data" / "articles.json"
    frontend_data_file.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "lastUpdated": _last_updated.isoformat(),
        "articles": [
            {
                "title": a.title,
                "url": str(a.url),
                "source": a.source,
                "publishedAt": a.published_at.isoformat(),
            }
            for a in _articles
        ],
    }

    with open(frontend_data_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Exported {len(_articles)} articles to frontend")


async def refresh_articles() -> FetchResult:
    """
    Fetch fresh articles from RSS feeds.

    Returns:
        FetchResult with fetch statistics
    """
    global _articles, _last_updated

    result = await fetcher.fetch_all()

    # Get filtered articles from fetcher
    all_articles: list[Article] = []
    for status in result.feed_statuses:
        if status.success:
            # Re-fetch to get articles (this is a bit redundant but keeps the code clean)
            pass

    # Actually, let's modify the fetcher to return articles
    # For now, let's re-fetch the data
    import asyncio
    import httpx

    async with httpx.AsyncClient(
        headers={"User-Agent": "NorthernTerritoriesNewsBot/1.0"},
        follow_redirects=True,
    ) as client:
        from app.config import RSS_FEEDS

        tasks = [fetcher.fetch_feed(client, feed) for feed in RSS_FEEDS]
        results = await asyncio.gather(*tasks)

        for articles, status in results:
            all_articles.extend(articles)

    # Filter and process
    filtered = fetcher.filter_articles(all_articles)
    filtered = fetcher.deduplicate_articles(filtered)
    filtered = fetcher.sort_articles(filtered)

    if len(filtered) > settings.max_total_articles:
        filtered = filtered[: settings.max_total_articles]

    _articles = filtered
    _last_updated = datetime.now(timezone.utc)

    # Save to files
    save_articles_to_file()
    export_to_frontend()

    return FetchResult(
        total_articles=len(all_articles),
        filtered_articles=len(filtered),
        feed_statuses=result.feed_statuses,
        duration_seconds=result.duration_seconds,
    )


def get_articles(
    source: str | None = None,
    sort_order: str = "desc",
    search_query: str | None = None,
) -> ArticleResponse:
    """
    Get articles with optional filtering.

    Args:
        source: Filter by source name
        sort_order: 'asc' or 'desc' for date sorting
        search_query: Search query for title filtering

    Returns:
        ArticleResponse with filtered articles
    """
    articles = _articles.copy()

    # Filter by source
    if source:
        articles = [a for a in articles if a.source == source]

    # Filter by search query
    if search_query:
        query_lower = search_query.lower()
        articles = [a for a in articles if query_lower in a.title.lower()]

    # Sort
    articles = sorted(articles, key=lambda a: a.published_at, reverse=(sort_order == "desc"))

    return ArticleResponse(
        articles=articles,
        last_updated=_last_updated,
        total_count=len(articles),
    )


def get_sources() -> list[str]:
    """Get unique list of sources."""
    return sorted(set(a.source for a in _articles))


def get_last_updated() -> datetime:
    """Get last update timestamp."""
    return _last_updated


# Initialize by loading from file on module import
load_articles_from_file()
