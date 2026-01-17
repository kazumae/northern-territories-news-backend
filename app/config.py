"""
Configuration settings for the Northern Territories News backend.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True

    # CORS
    cors_origins: list[str] = ["http://localhost:8080", "http://localhost:8081", "http://127.0.0.1:8080"]

    # RSS Feed settings
    fetch_timeout: int = 30  # seconds
    max_articles_per_source: int = 50
    max_total_articles: int = 500

    # Article retention
    retention_days: int = 180  # 6 months

    # Keywords for filtering Northern Territories related news
    filter_keywords: list[str] = [
        "北方領土",
        "択捉",
        "国後",
        "色丹",
        "歯舞",
        "北方四島",
        "千島列島",
        "クリル",
        "日露",
        "日ロ",
    ]

    class Config:
        env_prefix = "NEWS_"


# RSS Feed sources configuration
# Note: Some feeds may require updates as URLs change
RSS_FEEDS = [
    {
        "name": "NHK",
        "url": "https://www.nhk.or.jp/rss/news/cat6.xml",  # 政治
        "category": "politics",
    },
    {
        "name": "NHK",
        "url": "https://www.nhk.or.jp/rss/news/cat1.xml",  # 社会
        "category": "society",
    },
    {
        "name": "朝日新聞",
        "url": "https://www.asahi.com/rss/asahi/newsheadlines.rdf",
        "category": "general",
    },
    {
        "name": "毎日新聞",
        "url": "https://mainichi.jp/rss/etc/mainichi-flash.rss",
        "category": "general",
    },
    {
        "name": "時事通信",
        "url": "https://www.jiji.com/rss/ranking.rdf",
        "category": "general",
    },
    {
        "name": "北海道新聞",
        "url": "https://www.hokkaido-np.co.jp/output/7/free/index.ad.xml",
        "category": "regional",
    },
]


settings = Settings()
