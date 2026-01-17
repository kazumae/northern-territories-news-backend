#!/usr/bin/env python3
"""
Google News scraper for Northern Territories historical data.

This script scrapes Google News search results to collect historical articles
about Northern Territories. For one-time initial data collection only.

Usage:
    pip install playwright httpx
    playwright install chromium
    python scripts/scrape_google_news.py
"""

import asyncio
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse, parse_qs, urlunparse

import httpx


def normalize_url(url: str) -> str:
    """Normalize URL by removing query parameters for deduplication."""
    if not url:
        return url
    parsed = urlparse(url)
    # Remove query parameters and fragment
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def parse_relative_date(date_str: str) -> datetime:
    """
    Parse Japanese relative date strings from Google News.

    Examples: "3 時間前", "2 日前", "1 週間前", "3 か月前", "2024/12/15"
    """
    now = datetime.now(timezone.utc)
    date_str = date_str.strip()

    # Check for absolute date format (YYYY/MM/DD or similar)
    date_patterns = [
        (r"(\d{4})[/年](\d{1,2})[/月](\d{1,2})", "%Y-%m-%d"),
        (r"(\d{1,2})[/月](\d{1,2})[日]?", None),  # Current year assumed
    ]

    for pattern, fmt in date_patterns:
        match = re.search(pattern, date_str)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                return datetime(int(groups[0]), int(groups[1]), int(groups[2]), tzinfo=timezone.utc)
            elif len(groups) == 2:
                return datetime(now.year, int(groups[0]), int(groups[1]), tzinfo=timezone.utc)

    # Relative time patterns
    relative_patterns = [
        (r"(\d+)\s*秒前", lambda x: timedelta(seconds=int(x))),
        (r"(\d+)\s*分前", lambda x: timedelta(minutes=int(x))),
        (r"(\d+)\s*時間前", lambda x: timedelta(hours=int(x))),
        (r"(\d+)\s*日前", lambda x: timedelta(days=int(x))),
        (r"(\d+)\s*週間前", lambda x: timedelta(weeks=int(x))),
        (r"(\d+)\s*か月前", lambda x: timedelta(days=int(x) * 30)),
        (r"(\d+)\s*ヶ月前", lambda x: timedelta(days=int(x) * 30)),
        (r"(\d+)\s*年前", lambda x: timedelta(days=int(x) * 365)),
    ]

    for pattern, delta_func in relative_patterns:
        match = re.search(pattern, date_str)
        if match:
            return now - delta_func(match.group(1))

    # Default to now if parsing fails
    return now


async def resolve_real_url(google_url: str, client: httpx.AsyncClient) -> str:
    """
    Resolve the real article URL by following Google News redirect.

    Args:
        google_url: Google News URL
        client: HTTP client for making requests

    Returns:
        The actual article URL
    """
    if not google_url or "news.google.com" not in google_url:
        return google_url

    try:
        # Follow redirects to get the actual URL
        response = await client.get(
            google_url,
            follow_redirects=True,
            timeout=10.0,
        )
        final_url = str(response.url)

        # Clean up tracking parameters
        parsed = urlparse(final_url)
        # Remove common tracking params
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        return clean_url
    except Exception as e:
        # If resolution fails, return the original URL
        return google_url


def extract_real_url(google_url: str) -> str:
    """Extract the real article URL from Google News redirect URL (sync version)."""
    if "news.google.com" in google_url and "/articles/" in google_url:
        # Google News article URLs are encoded, try to get the original
        # These URLs redirect to the actual article
        return google_url

    # Check for direct URL in query params
    parsed = urlparse(google_url)
    if parsed.query:
        params = parse_qs(parsed.query)
        if "url" in params:
            return unquote(params["url"][0])

    return google_url


async def scrape_google_news(
    query: str = "北方領土",
    max_articles: int = 100,
    headless: bool = True,
) -> list[dict]:
    """
    Scrape Google News search results.

    Args:
        query: Search query
        max_articles: Maximum number of articles to collect
        headless: Run browser in headless mode

    Returns:
        List of article dictionaries
    """
    from playwright.async_api import async_playwright

    articles = []
    seen_titles = set()

    print(f"Searching Google News for: {query}")
    print(f"Target: {max_articles} articles")
    print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Navigate to Google News search
        search_url = f"https://news.google.com/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
        print(f"Navigating to: {search_url}")

        try:
            await page.goto(search_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)  # Wait for dynamic content

            # Debug: save screenshot
            debug_dir = Path(__file__).parent.parent / "debug"
            debug_dir.mkdir(exist_ok=True)
            await page.screenshot(path=str(debug_dir / f"google_news_{query}.png"))
            print(f"Debug screenshot saved to debug/google_news_{query}.png")

            # Debug: save HTML
            html_content = await page.content()
            with open(debug_dir / f"google_news_{query}.html", "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"Debug HTML saved to debug/google_news_{query}.html")

        except Exception as e:
            print(f"Navigation error: {e}")
            await browser.close()
            return articles

        # Scroll to load more articles
        scroll_count = 0
        max_scrolls = 20
        last_count = 0

        while len(articles) < max_articles and scroll_count < max_scrolls:
            # Extract articles using JtKRv links with aria-label
            link_elements = await page.query_selector_all("a.JtKRv[aria-label]")

            for link_el in link_elements:
                if len(articles) >= max_articles:
                    break

                try:
                    # Extract from aria-label: "title - source - date"
                    aria_label = await link_el.get_attribute("aria-label")
                    if not aria_label:
                        continue

                    # Parse aria-label (format: "title - source - date" or "title - source - date - extra")
                    parts = aria_label.rsplit(" - ", 3)
                    if len(parts) < 2:
                        continue

                    title = parts[0].strip()
                    if not title or title in seen_titles:
                        continue

                    # Source and date extraction
                    if len(parts) >= 3:
                        source = parts[1].strip()
                        date_str = parts[2].strip()
                    else:
                        source = parts[1].strip()
                        date_str = ""

                    # Get href
                    href = await link_el.get_attribute("href")
                    if href and href.startswith("./"):
                        href = "https://news.google.com" + href[1:]

                    # Parse date
                    published_at = parse_relative_date(date_str) if date_str else datetime.now(timezone.utc)

                    seen_titles.add(title)
                    articles.append({
                        "title": title,
                        "url": href,
                        "source": source or "不明",
                        "publishedAt": published_at.isoformat(),
                        "dateStr": date_str,
                    })

                    print(f"  [{len(articles)}] {source}: {title[:50]}...")

                except Exception as e:
                    continue

            # Check if we got new articles
            if len(articles) == last_count:
                # Scroll down to load more
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await asyncio.sleep(1.5)
                scroll_count += 1
            else:
                last_count = len(articles)
                scroll_count = 0  # Reset scroll count when finding new articles

            # Break if no more articles after several scrolls
            if scroll_count > 5:
                print("No more articles found")
                break

        await browser.close()

    print()
    print(f"Collected {len(articles)} articles")

    # Note: Google News URLs redirect to actual articles when clicked
    # URL resolution via HTTP is blocked by Google, so we keep the redirect URLs
    # Users clicking on these links will be automatically redirected to the actual article

    return articles


async def main():
    """Main function to scrape Google News and save results."""
    # Keywords to search
    keywords = [
        "北方領土",
        "択捉島",
        "国後島",
        "色丹島",
        "歯舞群島",
    ]

    all_articles = []
    seen_urls = set()

    for keyword in keywords:
        print(f"\n{'='*60}")
        print(f"Searching: {keyword}")
        print('='*60)

        articles = await scrape_google_news(
            query=keyword,
            max_articles=50,  # Per keyword
            headless=True,
        )

        # Deduplicate using normalized URLs
        for article in articles:
            url = article["url"]
            normalized = normalize_url(url)
            if normalized not in seen_urls:
                seen_urls.add(normalized)
                all_articles.append(article)

        # Be nice to Google
        await asyncio.sleep(2)

    print(f"\n{'='*60}")
    print(f"Total unique articles: {len(all_articles)}")
    print('='*60)

    # Sort by date (newest first)
    all_articles.sort(
        key=lambda x: x.get("publishedAt", ""),
        reverse=True
    )

    # Load existing articles
    frontend_path = Path(__file__).parent.parent.parent / "frontend" / "data" / "articles.json"
    existing_articles = []
    existing_urls = set()

    if frontend_path.exists():
        with open(frontend_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            existing_articles = data.get("articles", [])
            existing_urls = {normalize_url(a["url"]) for a in existing_articles}
        print(f"Existing articles: {len(existing_articles)}")

    # Merge with existing (avoid duplicates)
    new_count = 0
    for article in all_articles:
        # Clean up article (remove dateStr)
        clean_article = {
            "title": article["title"],
            "url": article["url"],
            "source": article["source"],
            "publishedAt": article["publishedAt"],
        }

        normalized = normalize_url(article["url"])
        if normalized not in existing_urls:
            existing_articles.append(clean_article)
            existing_urls.add(normalized)
            new_count += 1

    print(f"New articles added: {new_count}")

    # Remove duplicates from existing articles (cleanup)
    seen_normalized = set()
    deduplicated_articles = []
    for article in existing_articles:
        normalized = normalize_url(article["url"])
        if normalized not in seen_normalized:
            seen_normalized.add(normalized)
            deduplicated_articles.append(article)

    if len(deduplicated_articles) < len(existing_articles):
        print(f"Removed {len(existing_articles) - len(deduplicated_articles)} duplicate articles")
    existing_articles = deduplicated_articles

    # Sort all by date
    existing_articles.sort(
        key=lambda x: x.get("publishedAt", ""),
        reverse=True
    )

    # Save
    output_data = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "articles": existing_articles,
    }

    # Save to frontend
    frontend_path.parent.mkdir(parents=True, exist_ok=True)
    with open(frontend_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Saved {len(existing_articles)} articles to {frontend_path}")

    # Also save to backend
    backend_path = Path(__file__).parent.parent / "data" / "articles.json"
    backend_path.parent.mkdir(parents=True, exist_ok=True)
    with open(backend_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {len(existing_articles)} articles to {backend_path}")

    # Show sample
    print(f"\n=== Sample Articles ===")
    for article in existing_articles[:10]:
        print(f"  [{article['source']}] {article['title'][:50]}...")


if __name__ == "__main__":
    asyncio.run(main())
