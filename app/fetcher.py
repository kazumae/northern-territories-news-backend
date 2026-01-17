"""
RSS Feed fetcher for Northern Territories News.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import feedparser
import httpx
from dateutil import parser as date_parser

from app.config import RSS_FEEDS, settings
from app.models import Article, FeedStatus, FetchResult

logger = logging.getLogger(__name__)


class RSSFetcher:
    """Fetches and processes RSS feeds."""

    def __init__(self):
        self.timeout = settings.fetch_timeout
        self.keywords = [kw.lower() for kw in settings.filter_keywords]

    async def fetch_feed(self, client: httpx.AsyncClient, feed_config: dict) -> tuple[list[Article], FeedStatus]:
        """
        Fetch and parse a single RSS feed.

        Args:
            client: HTTP client
            feed_config: Feed configuration dict with name, url, category

        Returns:
            Tuple of (list of articles, feed status)
        """
        name = feed_config["name"]
        url = feed_config["url"]

        try:
            response = await client.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Parse the feed
            feed = feedparser.parse(response.text)

            if feed.bozo and not feed.entries:
                raise ValueError(f"Failed to parse feed: {feed.bozo_exception}")

            articles = []
            for entry in feed.entries[: settings.max_articles_per_source]:
                article = self._parse_entry(entry, name)
                if article:
                    articles.append(article)

            status = FeedStatus(
                name=name,
                url=url,
                success=True,
                article_count=len(articles),
            )

            logger.info(f"Fetched {len(articles)} articles from {name}")
            return articles, status

        except httpx.TimeoutException:
            logger.warning(f"Timeout fetching {name}: {url}")
            return [], FeedStatus(name=name, url=url, success=False, error="Timeout")

        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP error fetching {name}: {e.response.status_code}")
            return [], FeedStatus(name=name, url=url, success=False, error=f"HTTP {e.response.status_code}")

        except Exception as e:
            logger.error(f"Error fetching {name}: {e}")
            return [], FeedStatus(name=name, url=url, success=False, error=str(e))

    def _parse_entry(self, entry: dict, source: str) -> Article | None:
        """
        Parse a feed entry into an Article.

        Args:
            entry: Feed entry from feedparser
            source: News source name

        Returns:
            Article or None if parsing fails
        """
        try:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()

            if not title or not link:
                return None

            # Parse publication date
            published = None
            for date_field in ["published_parsed", "updated_parsed", "created_parsed"]:
                if entry.get(date_field):
                    try:
                        published = datetime(*entry[date_field][:6], tzinfo=timezone.utc)
                        break
                    except (TypeError, ValueError):
                        continue

            # Try string date parsing as fallback
            if not published:
                for date_field in ["published", "updated", "created"]:
                    if entry.get(date_field):
                        try:
                            published = date_parser.parse(entry[date_field])
                            if published.tzinfo is None:
                                published = published.replace(tzinfo=timezone.utc)
                            break
                        except (ValueError, TypeError):
                            continue

            # Default to current time if no date found
            if not published:
                published = datetime.now(timezone.utc)

            return Article(
                title=title,
                url=link,
                source=source,
                published_at=published,
            )

        except Exception as e:
            logger.debug(f"Failed to parse entry: {e}")
            return None

    def filter_articles(self, articles: list[Article]) -> list[Article]:
        """
        Filter articles by keywords related to Northern Territories.

        Args:
            articles: List of articles to filter

        Returns:
            Filtered list of articles
        """
        filtered = []
        for article in articles:
            title_lower = article.title.lower()
            if any(keyword in title_lower for keyword in self.keywords):
                filtered.append(article)

        return filtered

    def deduplicate_articles(self, articles: list[Article]) -> list[Article]:
        """
        Remove duplicate articles based on URL.

        Args:
            articles: List of articles

        Returns:
            Deduplicated list of articles
        """
        seen_urls = set()
        unique = []

        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        return unique

    def sort_articles(self, articles: list[Article], descending: bool = True) -> list[Article]:
        """
        Sort articles by publication date.

        Args:
            articles: List of articles
            descending: Sort newest first if True

        Returns:
            Sorted list of articles
        """
        return sorted(articles, key=lambda a: a.published_at, reverse=descending)

    async def fetch_all(self) -> FetchResult:
        """
        Fetch all configured RSS feeds and return filtered articles.

        Returns:
            FetchResult with articles and status information
        """
        start_time = time.time()
        all_articles: list[Article] = []
        feed_statuses: list[FeedStatus] = []

        async with httpx.AsyncClient(
            headers={"User-Agent": "NorthernTerritoriesNewsBot/1.0"},
            follow_redirects=True,
        ) as client:
            # Fetch all feeds concurrently
            tasks = [self.fetch_feed(client, feed) for feed in RSS_FEEDS]
            results = await asyncio.gather(*tasks)

            for articles, status in results:
                all_articles.extend(articles)
                feed_statuses.append(status)

        total_articles = len(all_articles)

        # Filter by keywords
        filtered_articles = self.filter_articles(all_articles)

        # Deduplicate
        filtered_articles = self.deduplicate_articles(filtered_articles)

        # Sort by date
        filtered_articles = self.sort_articles(filtered_articles)

        # Limit total articles
        if len(filtered_articles) > settings.max_total_articles:
            filtered_articles = filtered_articles[: settings.max_total_articles]

        duration = time.time() - start_time

        logger.info(
            f"Fetch complete: {total_articles} total, {len(filtered_articles)} filtered, {duration:.2f}s"
        )

        return FetchResult(
            total_articles=total_articles,
            filtered_articles=len(filtered_articles),
            feed_statuses=feed_statuses,
            duration_seconds=duration,
        )


# Global fetcher instance
fetcher = RSSFetcher()
