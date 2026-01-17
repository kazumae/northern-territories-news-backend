"""
AWS Lambda handler for Northern Territories News aggregation via Google News RSS.

This handler is triggered by EventBridge schedule to fetch Google News RSS feeds,
collect articles related to Northern Territories, and upload the result to S3.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, urlunparse, quote

import feedparser
import httpx

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client (only import boto3 in Lambda environment)
s3_client = None
if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
    import boto3
    s3_client = boto3.client("s3")

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "northern-territories-news-prod")
S3_KEY = os.environ.get("S3_KEY", "data/articles.json")

# Search keywords for Google News
SEARCH_KEYWORDS = [
    "北方領土",
    "択捉島",
    "国後島",
    "色丹島",
    "歯舞群島",
]

# Maximum articles to keep
MAX_ARTICLES = 500


def normalize_url(url: str) -> str:
    """Normalize URL by removing query parameters for deduplication."""
    if not url:
        return url
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))


def extract_source_from_title(title: str) -> tuple[str, str]:
    """
    Extract source name from Google News title format.

    Google News titles are formatted as: "Article Title - Source Name"

    Returns:
        tuple: (clean_title, source_name)
    """
    if " - " in title:
        parts = title.rsplit(" - ", 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
    return title, "Google News"


def parse_pub_date(pub_date_str: str) -> datetime:
    """Parse publication date from RSS feed."""
    try:
        # Try RFC 2822 format (standard RSS)
        return parsedate_to_datetime(pub_date_str)
    except Exception:
        pass

    try:
        # Try ISO format
        return datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
    except Exception:
        pass

    # Default to now
    return datetime.now(timezone.utc)


def extract_image_url(entry) -> str | None:
    """
    Extract image URL from Google News RSS entry.

    Google News RSS uses media:content for images.
    """
    # Try media_content (feedparser normalizes media:content)
    media_content = entry.get("media_content", [])
    if media_content and isinstance(media_content, list):
        for media in media_content:
            url = media.get("url", "")
            if url and ("jpg" in url or "jpeg" in url or "png" in url or "webp" in url or "image" in url):
                return url

    # Try media_thumbnail
    media_thumbnail = entry.get("media_thumbnail", [])
    if media_thumbnail and isinstance(media_thumbnail, list):
        for thumb in media_thumbnail:
            url = thumb.get("url", "")
            if url:
                return url

    # Try enclosure
    enclosures = entry.get("enclosures", [])
    if enclosures:
        for enc in enclosures:
            if enc.get("type", "").startswith("image/"):
                return enc.get("href") or enc.get("url")

    return None


async def fetch_google_news_rss(keyword: str, client: httpx.AsyncClient) -> list[dict]:
    """
    Fetch articles from Google News RSS for a specific keyword.

    Args:
        keyword: Search keyword
        client: HTTP client

    Returns:
        List of article dictionaries
    """
    encoded_keyword = quote(keyword)
    url = f"https://news.google.com/rss/search?q={encoded_keyword}&hl=ja&gl=JP&ceid=JP:ja"

    try:
        response = await client.get(url, timeout=30.0)
        response.raise_for_status()

        feed = feedparser.parse(response.text)

        if feed.bozo and not feed.entries:
            logger.warning(f"Failed to parse feed for '{keyword}': {feed.bozo_exception}")
            return []

        articles = []
        for entry in feed.entries:
            try:
                raw_title = entry.get("title", "").strip()
                if not raw_title:
                    continue

                # Extract clean title and source
                title, source = extract_source_from_title(raw_title)

                # Get link (Google News redirect URL)
                link = entry.get("link", "").strip()
                if not link:
                    continue

                # Parse publication date
                pub_date_str = entry.get("published", "")
                published_at = parse_pub_date(pub_date_str)

                # Extract image URL
                image_url = extract_image_url(entry)

                article = {
                    "title": title,
                    "url": link,
                    "source": source,
                    "publishedAt": published_at.isoformat(),
                }

                if image_url:
                    article["imageUrl"] = image_url

                articles.append(article)

            except Exception as e:
                logger.debug(f"Failed to parse entry: {e}")
                continue

        logger.info(f"Fetched {len(articles)} articles for '{keyword}'")
        return articles

    except Exception as e:
        logger.error(f"Error fetching RSS for '{keyword}': {e}")
        return []


async def fetch_all_keywords() -> dict:
    """
    Fetch articles for all keywords.

    Returns:
        dict: Result containing articles and stats
    """
    all_articles = []
    stats = {
        "keywords_searched": len(SEARCH_KEYWORDS),
        "successful_fetches": 0,
        "failed_fetches": 0,
    }

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        },
        follow_redirects=True,
    ) as client:
        tasks = [fetch_google_news_rss(kw, client) for kw in SEARCH_KEYWORDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error for keyword '{SEARCH_KEYWORDS[i]}': {result}")
                stats["failed_fetches"] += 1
            else:
                all_articles.extend(result)
                stats["successful_fetches"] += 1

    stats["total_fetched"] = len(all_articles)

    return {
        "articles": all_articles,
        "stats": stats,
    }


def load_existing_articles() -> list[dict]:
    """
    Load existing articles from S3.

    Returns:
        list: Existing articles or empty list if not found
    """
    if not s3_client:
        logger.warning("S3 client not available, returning empty list")
        return []

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        data = json.loads(response['Body'].read().decode('utf-8'))
        articles = data.get('articles', [])
        logger.info(f"Loaded {len(articles)} existing articles from S3")
        return articles
    except s3_client.exceptions.NoSuchKey:
        logger.info("No existing articles file found in S3")
        return []
    except Exception as e:
        logger.warning(f"Failed to load existing articles: {e}")
        return []


def merge_articles(existing: list[dict], new_articles: list[dict]) -> list[dict]:
    """
    Merge new articles with existing ones, avoiding duplicates.

    Args:
        existing: Existing articles from S3
        new_articles: Newly fetched articles

    Returns:
        list: Merged and deduplicated articles
    """
    # Build set of normalized URLs from existing articles
    seen_urls = set()
    seen_titles = set()

    for article in existing:
        normalized = normalize_url(article.get('url', ''))
        seen_urls.add(normalized)
        # Also track titles to avoid duplicates with different URLs
        seen_titles.add(article.get('title', '').lower())

    # Add new articles that don't exist
    merged = existing.copy()
    new_count = 0

    for article in new_articles:
        normalized = normalize_url(article.get('url', ''))
        title_lower = article.get('title', '').lower()

        # Skip if URL or title already exists
        if normalized in seen_urls or title_lower in seen_titles:
            continue

        seen_urls.add(normalized)
        seen_titles.add(title_lower)
        merged.append(article)
        new_count += 1

    logger.info(f"Added {new_count} new articles, total: {len(merged)}")

    # Sort by date (newest first)
    merged.sort(key=lambda x: x.get('publishedAt', ''), reverse=True)

    # Limit to max articles
    if len(merged) > MAX_ARTICLES:
        merged = merged[:MAX_ARTICLES]

    return merged


def upload_to_s3(data: dict) -> dict:
    """
    Upload articles data to S3.

    Args:
        data: Articles data to upload

    Returns:
        dict: Upload result
    """
    if not s3_client:
        logger.warning("S3 client not available, skipping upload")
        return {"uploaded": False, "reason": "S3 client not available"}

    try:
        json_data = json.dumps(data, ensure_ascii=False, indent=2)

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=json_data.encode("utf-8"),
            ContentType="application/json; charset=utf-8",
            CacheControl="max-age=300",  # 5 minutes cache
        )

        logger.info(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
        return {"uploaded": True, "bucket": S3_BUCKET, "key": S3_KEY}

    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        return {"uploaded": False, "error": str(e)}


def handler(event, context):
    """
    Lambda handler function.

    Args:
        event: Lambda event (from EventBridge)
        context: Lambda context

    Returns:
        dict: Lambda response
    """
    logger.info(f"Lambda invoked with event: {json.dumps(event)}")

    try:
        # Load existing articles from S3
        existing_articles = load_existing_articles()

        # Fetch from Google News RSS
        result = asyncio.run(fetch_all_keywords())

        # Merge new articles with existing
        merged_articles = merge_articles(existing_articles, result["articles"])

        # Build final data
        now = datetime.now(timezone.utc)
        final_data = {
            "lastUpdated": now.isoformat(),
            "articles": merged_articles,
        }

        # Upload to S3
        upload_result = upload_to_s3(final_data)

        response = {
            "statusCode": 200,
            "body": {
                "message": "Google News RSS fetch completed successfully",
                "stats": {
                    **result["stats"],
                    "existing_articles": len(existing_articles),
                    "merged_total": len(merged_articles),
                },
                "upload": upload_result,
            },
        }

        logger.info(f"Lambda completed: {result['stats']}, merged: {len(merged_articles)}")
        return response

    except Exception as e:
        logger.error(f"Lambda failed: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": {
                "message": "Google News RSS fetch failed",
                "error": str(e),
            },
        }


# For local testing
if __name__ == "__main__":
    result = asyncio.run(fetch_all_keywords())
    print(json.dumps(result, ensure_ascii=False, indent=2))
