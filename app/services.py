import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Protocol

from celery import Celery
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.concurrency import run_in_threadpool

from app.config import Settings
from app.models import Document, DocumentStatus, utcnow
from app.repository import DocumentPage, DocumentRepository

logger = logging.getLogger(__name__)

CACHE_KEY_PREFIX = "summary"
RATE_LIMIT_KEY_PREFIX = "ratelimit:active_jobs"
SUMMARIZE_TASK_NAME = "documents.summarize"

RESERVE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == false then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
    current = ARGV[2]
end
if tonumber(current) >= tonumber(ARGV[1]) then
    return -1
end
local updated = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return updated
"""

RELEASE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == false then
    return -1
end
local updated = redis.call('DECR', KEYS[1])
if updated < 0 then
    redis.call('SET', KEYS[1], 0, 'EX', ARGV[1])
    return 0
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return updated
"""


class DocumentNotFoundError(Exception):
    def __init__(self, document_id: str) -> None:
        super().__init__(f"Document {document_id} was not found")
        self.document_id = document_id


class RateLimitExceededError(Exception):
    def __init__(self, user_id: str, limit: int, active: int) -> None:
        super().__init__(f"User {user_id} already has {active} active documents (limit {limit})")
        self.user_id = user_id
        self.limit = limit
        self.active = active


def compute_content_hash(user_id: str, content: str) -> str:
    digest = hashlib.sha256()
    digest.update(user_id.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(content.encode("utf-8"))
    return digest.hexdigest()


class SummaryCache:
    def __init__(self, redis: Redis, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    @staticmethod
    def key(user_id: str, content_hash: str) -> str:
        return f"{CACHE_KEY_PREFIX}:{user_id}:{content_hash}"

    async def get(self, user_id: str, content_hash: str) -> dict[str, Any] | None:
        try:
            raw = await self._redis.get(self.key(user_id, content_hash))
        except (RedisError, OSError) as exc:
            logger.warning("cache read failed, treating as a miss: %s", exc)
            return None

        if raw is None:
            return None

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("discarding malformed cache entry", extra={"user_id": user_id})
            await self.delete(user_id, content_hash)
            return None

        if not isinstance(payload, dict) or "summary" not in payload:
            await self.delete(user_id, content_hash)
            return None
        return payload

    async def set(
        self, user_id: str, content_hash: str, summary: dict[str, Any], source_document_id: str
    ) -> bool:
        payload = json.dumps(
            {"summary": summary, "source_document_id": source_document_id}, default=str
        )
        try:
            await self._redis.set(self.key(user_id, content_hash), payload, ex=self._ttl)
        except (RedisError, OSError) as exc:
            logger.warning("cache write failed: %s", exc)
            return False
        return True

    async def delete(self, user_id: str, content_hash: str) -> None:
        try:
            await self._redis.delete(self.key(user_id, content_hash))
        except (RedisError, OSError) as exc:
            logger.warning("cache delete failed: %s", exc)


@dataclass(frozen=True)
class Reservation:
    allowed: bool
    active: int
    limit: int
    degraded: bool


class ActiveJobRateLimiter:
    def __init__(
        self,
        redis: Redis,
        limit: int,
        key_ttl_seconds: int,
        fallback_counter: Callable[[str], Awaitable[int]],
    ) -> None:
        self._redis = redis
        self._limit = limit
        self._key_ttl = key_ttl_seconds
        self._fallback_counter = fallback_counter

    @staticmethod
    def key(user_id: str) -> str:
        return f"{RATE_LIMIT_KEY_PREFIX}:{user_id}"

    async def reserve(self, user_id: str) -> Reservation:
        seed = await self._fallback_counter(user_id)

        try:
            raw = await self._redis.eval(
                RESERVE_SCRIPT, 1, self.key(user_id), self._limit, seed, self._key_ttl
            )
        except (RedisError, OSError) as exc:
            logger.warning("rate limiter falling back to mongodb counting: %s", exc)
            allowed = seed < self._limit
            return Reservation(
                allowed=allowed,
                active=seed + 1 if allowed else seed,
                limit=self._limit,
                degraded=True,
            )

        value = int(raw)
        if value == -1:
            return Reservation(
                allowed=False, active=max(seed, self._limit), limit=self._limit, degraded=False
            )
        return Reservation(allowed=True, active=value, limit=self._limit, degraded=False)

    async def release(self, user_id: str) -> None:
        try:
            await self._redis.eval(RELEASE_SCRIPT, 1, self.key(user_id), self._key_ttl)
        except (RedisError, OSError) as exc:
            logger.warning("failed to release rate limit slot: %s", exc)


class TaskDispatcher(Protocol):
    async def enqueue_summarization(self, document_id: str) -> bool: ...


class CeleryTaskDispatcher:
    def __init__(self, celery_app: Celery) -> None:
        self._celery_app = celery_app

    async def enqueue_summarization(self, document_id: str) -> bool:
        try:
            await run_in_threadpool(
                self._celery_app.send_task, SUMMARIZE_TASK_NAME, args=[document_id]
            )
        except Exception as exc:
            logger.error(
                "failed to enqueue summarization task: %s",
                exc,
                extra={"document_id": document_id},
            )
            return False
        return True


@dataclass(frozen=True)
class Submission:
    document_id: str
    user_id: str
    status: DocumentStatus
    cached: bool
    created_at: datetime
    summary: dict[str, Any] | None


class DocumentService:
    def __init__(
        self,
        repository: DocumentRepository,
        cache: SummaryCache,
        rate_limiter: ActiveJobRateLimiter,
        dispatcher: TaskDispatcher,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._dispatcher = dispatcher
        self._settings = settings

    async def submit(self, user_id: str, title: str, content: str) -> Submission:
        content_hash = compute_content_hash(user_id, content)

        cached_entry = await self._cache.get(user_id, content_hash)
        if cached_entry is not None:
            return await self._store_cached_result(
                user_id, title, content, content_hash, cached_entry["summary"]
            )

        reservation = await self._rate_limiter.reserve(user_id)
        if not reservation.allowed:
            logger.info(
                "submission rejected, active job limit reached",
                extra={"user_id": user_id, "active": reservation.active},
            )
            raise RateLimitExceededError(user_id, reservation.limit, reservation.active)

        document = Document(
            user_id=user_id, title=title, content=content, content_hash=content_hash
        )
        try:
            document_id = await self._repository.insert(document)
        except Exception:
            await self._rate_limiter.release(user_id)
            raise

        enqueued = await self._dispatcher.enqueue_summarization(document_id)
        logger.info(
            "document queued",
            extra={"document_id": document_id, "user_id": user_id, "enqueued": enqueued},
        )
        return Submission(
            document_id=document_id,
            user_id=user_id,
            status=DocumentStatus.QUEUED,
            cached=False,
            created_at=document.created_at,
            summary=None,
        )

    async def get(self, document_id: str) -> Document:
        document = await self._repository.get(document_id)
        if document is None:
            raise DocumentNotFoundError(document_id)
        return document

    async def list_for_user(
        self, user_id: str, page: int, page_size: int, status: DocumentStatus | None
    ) -> DocumentPage:
        return await self._repository.list_for_user(
            user_id=user_id, page=page, page_size=page_size, status=status
        )

    async def _store_cached_result(
        self, user_id: str, title: str, content: str, content_hash: str, summary: dict[str, Any]
    ) -> Submission:
        now = utcnow()
        document = Document(
            user_id=user_id,
            title=title,
            content=content,
            content_hash=content_hash,
            status=DocumentStatus.COMPLETED,
            summary=summary,
            cached=True,
            started_at=now,
            completed_at=now,
        )
        document_id = await self._repository.insert(document)
        logger.info(
            "served submission from cache",
            extra={"document_id": document_id, "user_id": user_id},
        )
        return Submission(
            document_id=document_id,
            user_id=user_id,
            status=DocumentStatus.COMPLETED,
            cached=True,
            created_at=document.created_at,
            summary=summary,
        )
