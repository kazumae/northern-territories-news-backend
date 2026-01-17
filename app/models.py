"""
Data models for the Northern Territories News backend.
"""

from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl


class Article(BaseModel):
    """Represents a news article."""

    title: str = Field(..., description="Article title")
    url: HttpUrl = Field(..., description="Article URL")
    source: str = Field(..., description="News source name")
    published_at: datetime = Field(..., description="Publication date/time")
    fetched_at: datetime = Field(default_factory=datetime.now, description="When the article was fetched")

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class ArticleResponse(BaseModel):
    """Response model for article list endpoint."""

    articles: list[Article] = Field(default_factory=list, description="List of articles")
    last_updated: datetime = Field(default_factory=datetime.now, description="Last update timestamp")
    total_count: int = Field(0, description="Total number of articles")

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class FeedStatus(BaseModel):
    """Status of a feed fetch operation."""

    name: str
    url: str
    success: bool
    article_count: int = 0
    error: str | None = None


class FetchResult(BaseModel):
    """Result of fetching all feeds."""

    total_articles: int
    filtered_articles: int
    feed_statuses: list[FeedStatus]
    duration_seconds: float
