"""
AWS Lambda handler for Northern Territories News RSS aggregation.

This handler is triggered by EventBridge schedule to fetch RSS feeds,
filter articles related to Northern Territories, and upload the result to S3.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

from app.config import settings
from app.fetcher import RSSFetcher

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client (only import boto3 in Lambda environment)
s3_client = None
if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
    import boto3
    s3_client = boto3.client("s3")

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "northern-territories-news")
S3_KEY = os.environ.get("S3_KEY", "data/articles.json")


def normalize_url(url: str) -> str:
    """Normalize URL by removing query parameters for deduplication."""
    if not url:
        return url
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))


async def fetch_and_process():
    """
    Fetch RSS feeds and process articles.

    Returns:
        dict: Result containing articles and metadata
    """
    fetcher = RSSFetcher()

    # Fetch all feeds
    import httpx
    from app.config import RSS_FEEDS

    all_articles = []
    feed_statuses = []

    async with httpx.AsyncClient(
        headers={"User-Agent": "NorthernTerritoriesNewsBot/1.0"},
        follow_redirects=True,
    ) as client:
        tasks = [fetcher.fetch_feed(client, feed) for feed in RSS_FEEDS]
        results = await asyncio.gather(*tasks)

        for articles, status in results:
            all_articles.extend(articles)
            feed_statuses.append(status)

    # Filter and process
    filtered = fetcher.filter_articles(all_articles)
    filtered = fetcher.deduplicate_articles(filtered)
    filtered = fetcher.sort_articles(filtered)

    if len(filtered) > settings.max_total_articles:
        filtered = filtered[: settings.max_total_articles]

    # Build response
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

    return {
        "data": data,
        "stats": {
            "total_fetched": len(all_articles),
            "filtered_count": len(filtered),
            "successful_feeds": sum(1 for s in feed_statuses if s.success),
            "failed_feeds": sum(1 for s in feed_statuses if not s.success),
        },
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
    for article in existing:
        normalized = normalize_url(article.get('url', ''))
        seen_urls.add(normalized)

    # Add new articles that don't exist
    merged = existing.copy()
    new_count = 0
    for article in new_articles:
        normalized = normalize_url(article.get('url', ''))
        if normalized not in seen_urls:
            seen_urls.add(normalized)
            merged.append(article)
            new_count += 1

    logger.info(f"Added {new_count} new articles, total: {len(merged)}")

    # Sort by date (newest first)
    merged.sort(key=lambda x: x.get('publishedAt', ''), reverse=True)

    # Limit to max articles
    if len(merged) > settings.max_total_articles:
        merged = merged[:settings.max_total_articles]

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

        # Run async fetch
        result = asyncio.run(fetch_and_process())

        # Merge new articles with existing
        merged_articles = merge_articles(existing_articles, result["data"]["articles"])

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
                "message": "RSS fetch completed successfully",
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
                "message": "RSS fetch failed",
                "error": str(e),
            },
        }


# For local testing
if __name__ == "__main__":
    # Run without S3 upload for local testing
    result = asyncio.run(fetch_and_process())
    print(json.dumps(result, ensure_ascii=False, indent=2))
