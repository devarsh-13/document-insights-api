import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_DB_NAME", "document_insights_test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient

from app.config import Settings, get_settings
from app.main import create_app
from app.repository import DocumentRepository
from app.services import ActiveJobRateLimiter, DocumentService, SummaryCache
from app.worker import JobProcessor, MockSummarizer


class FakeDispatcher:
    def __init__(self, working: bool = True) -> None:
        self.enqueued: list[str] = []
        self._working = working

    async def enqueue_summarization(self, document_id: str) -> bool:
        if not self._working:
            return False
        self.enqueued.append(document_id)
        return True


class FakeConnection:
    def __init__(self, client=None, reachable: bool = True) -> None:
        self.client = client
        self._reachable = reachable

    async def ping(self) -> bool:
        return self._reachable


@pytest.fixture
def settings() -> Settings:
    return Settings(
        mongo_uri="mongodb://localhost:27017",
        mongo_db_name="document_insights_test",
        redis_url="redis://localhost:6379/0",
        celery_broker_url="memory://",
        job_min_duration_seconds=0.0,
        job_max_duration_seconds=0.0,
        job_failure_rate=0.0,
        job_retry_backoff_base_seconds=0.01,
    )


@pytest.fixture
async def redis() -> AsyncIterator[fakeredis.aioredis.FakeRedis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def database():
    return AsyncMongoMockClient()["document_insights_test"]


@pytest.fixture
def repository(database) -> DocumentRepository:
    return DocumentRepository(database)


@pytest.fixture
def cache(redis, settings) -> SummaryCache:
    return SummaryCache(redis, settings.summary_cache_ttl_seconds)


@pytest.fixture
def rate_limiter(redis, repository, settings) -> ActiveJobRateLimiter:
    return ActiveJobRateLimiter(
        redis=redis,
        limit=settings.max_active_jobs_per_user,
        key_ttl_seconds=settings.rate_limit_key_ttl_seconds,
        fallback_counter=repository.count_active_for_user,
    )


@pytest.fixture
def dispatcher() -> FakeDispatcher:
    return FakeDispatcher()


@pytest.fixture
def service(repository, cache, rate_limiter, dispatcher, settings) -> DocumentService:
    return DocumentService(
        repository=repository,
        cache=cache,
        rate_limiter=rate_limiter,
        dispatcher=dispatcher,
        settings=settings,
    )


@pytest.fixture
def make_processor(repository, cache, rate_limiter, settings):
    def _make(failure_rate: float = 0.0) -> JobProcessor:
        return JobProcessor(
            repository=repository,
            cache=cache,
            rate_limiter=rate_limiter,
            summarizer=MockSummarizer(0.0, 0.0, failure_rate),
            settings=settings,
        )

    return _make


@pytest.fixture
def processor(make_processor) -> JobProcessor:
    return make_processor()


@asynccontextmanager
async def api_client(settings, database, redis, dispatcher, mongo_up=True, redis_up=True):
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings

    @asynccontextmanager
    async def test_lifespan(instance) -> AsyncIterator[None]:
        instance.state.mongo = FakeConnection(reachable=mongo_up)
        instance.state.database = database
        instance.state.redis = FakeConnection(redis, reachable=redis_up)
        instance.state.dispatcher = dispatcher
        yield

    app.router.lifespan_context = test_lifespan
    async with test_lifespan(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as http:
            yield http


@pytest.fixture
async def client(settings, database, redis, dispatcher) -> AsyncIterator[AsyncClient]:
    async with api_client(settings, database, redis, dispatcher) as http:
        yield http
