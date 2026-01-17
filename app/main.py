"""
FastAPI application for Northern Territories News backend.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.models import ArticleResponse, FetchResult
from app.service import get_articles, get_last_updated, get_sources, refresh_articles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Starting Northern Territories News backend")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="北方領土ニュース API",
    description="Northern Territories news aggregation API",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "Northern Territories News API is running"}


@app.get("/api/articles", response_model=ArticleResponse)
async def list_articles(
    source: str | None = Query(None, description="Filter by source name"),
    sort: str = Query("desc", description="Sort order: 'asc' or 'desc'"),
    q: str | None = Query(None, description="Search query for title"),
):
    """
    Get list of articles with optional filtering.

    - **source**: Filter by news source name (e.g., "NHK", "朝日新聞")
    - **sort**: Sort by date - 'desc' (newest first) or 'asc' (oldest first)
    - **q**: Search query to filter articles by title
    """
    response = get_articles(source=source, sort_order=sort, search_query=q)

    # Convert to JSON-compatible format
    return JSONResponse(
        content={
            "articles": [
                {
                    "title": a.title,
                    "url": str(a.url),
                    "source": a.source,
                    "publishedAt": a.published_at.isoformat(),
                }
                for a in response.articles
            ],
            "lastUpdated": response.last_updated.isoformat(),
            "totalCount": response.total_count,
        }
    )


@app.get("/api/sources")
async def list_sources():
    """Get list of available news sources."""
    return {"sources": get_sources()}


@app.get("/api/status")
async def status():
    """Get API status and last update time."""
    return {
        "status": "ok",
        "lastUpdated": get_last_updated().isoformat(),
    }


@app.post("/api/refresh", response_model=FetchResult)
async def trigger_refresh():
    """
    Trigger a refresh of articles from RSS feeds.

    This endpoint fetches fresh data from all configured RSS feeds,
    filters articles related to Northern Territories, and updates the database.
    """
    result = await refresh_articles()
    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )
