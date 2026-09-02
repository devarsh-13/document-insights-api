import asyncio
import logging
import random
import re
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator

from celery import Celery, Task

from app.config import Settings, get_settings
from app.db import MongoConnection, RedisConnection
from app.models import Document, utcnow
from app.repository import DocumentRepository
from app.services import ActiveJobRateLimiter, SummaryCache

logger = logging.getLogger(__name__)

settings = get_settings()

celery_app = Celery("document_insights")
celery_app.conf.update(
    broker_url=settings.celery_broker_url,
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": settings.celery_visibility_timeout_seconds
    },
    result_backend=None,
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    timezone="UTC",
    enable_utc=True,
)

WORD_PATTERN = re.compile(r"[A-Za-z0-9']+")
SENTENCE_PATTERN = re.compile(r"(?<=[.!?])\s+")
STOP_WORDS = frozenset(
    """a an and are as at be but by for from has have he her his if in into is it its of on or
    she that the their them then there these they this to was were what when where which who will
    with would you your our we us not no can could should may might do does did been being""".split()
)


class SummarizationError(RuntimeError):
    pass


class Outcome(str, Enum):
    COMPLETED = "completed"
    RETRY = "retry"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class ProcessingResult:
    outcome: Outcome
    error: str | None = None


def extract_keywords(content: str, limit: int = 5) -> list[str]:
    words = [word.lower() for word in WORD_PATTERN.findall(content)]
    candidates = [word for word in words if len(word) > 3 and word not in STOP_WORDS]
    return [word for word, _ in Counter(candidates).most_common(limit)]


def build_summary(title: str, content: str) -> dict[str, Any]:
    words = WORD_PATTERN.findall(content)
    sentences = [part.strip() for part in SENTENCE_PATTERN.split(content.strip()) if part.strip()]
    lead = " ".join(sentences[:2]) if sentences else content.strip()
    if len(lead) > 300:
        lead = lead[:297].rstrip() + "..."

    keywords = extract_keywords(content)
    return {
        "text": (
            f"'{title}' contains {len(words)} words across {len(sentences) or 1} sentences. "
            f"Key topics: {', '.join(keywords) if keywords else 'none detected'}. "
            f"Opening: {lead}"
        ),
        "word_count": len(words),
        "char_count": len(content),
        "keywords": keywords,
        "reading_time_seconds": max(1, round(len(words) / 200 * 60)),
        "generated_at": utcnow().isoformat(),
    }


class MockSummarizer:
    def __init__(
        self,
        min_duration: float,
        max_duration: float,
        failure_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_duration = min_duration
        self._max_duration = max_duration
        self._failure_rate = failure_rate
        self._rng = rng or random.Random()

    def next_duration(self) -> float:
        return self._rng.uniform(self._min_duration, self._max_duration)

    async def summarize(self, title: str, content: str) -> dict[str, Any]:
        await asyncio.sleep(self.next_duration())
        if self._rng.random() < self._failure_rate:
            raise SummarizationError("summarization backend returned a transient error")
        return build_summary(title, content)


class JobProcessor:
    def __init__(
        self,
        repository: DocumentRepository,
        cache: SummaryCache,
        rate_limiter: ActiveJobRateLimiter,
        summarizer: MockSummarizer,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._summarizer = summarizer
        self._settings = settings

    async def process(self, document_id: str, attempt: int) -> ProcessingResult:
        job = await self._repository.claim(document_id, attempt)
        if job is None:
            logger.info(
                "document not claimable, skipping",
                extra={"document_id": document_id, "attempt": attempt},
            )
            return ProcessingResult(outcome=Outcome.SKIPPED)

        logger.info(
            "processing document",
            extra={"document_id": document_id, "user_id": job.user_id, "attempt": attempt},
        )

        cached = await self._cache.get(job.user_id, job.content_hash)
        if cached is not None:
            await self._complete(job, cached["summary"], from_cache=True)
            return ProcessingResult(outcome=Outcome.COMPLETED)

        try:
            summary = await self._summarizer.summarize(job.title, job.content)
        except asyncio.CancelledError:
            raise
        except SummarizationError as exc:
            return await self._handle_failure(job, str(exc), attempt)
        except Exception as exc:
            logger.exception(
                "unexpected error while summarizing", extra={"document_id": document_id}
            )
            return await self._handle_failure(job, f"unexpected error: {exc}", attempt)

        await self._cache.set(job.user_id, job.content_hash, summary, document_id)
        await self._complete(job, summary, from_cache=False)
        return ProcessingResult(outcome=Outcome.COMPLETED)

    async def _complete(self, job: Document, summary: dict, from_cache: bool) -> None:
        if not await self._repository.mark_completed(job.id, summary, cached=from_cache):
            logger.warning(
                "document no longer owned by this worker at completion",
                extra={"document_id": job.id},
            )
            return

        await self._rate_limiter.release(job.user_id)
        logger.info(
            "document completed",
            extra={"document_id": job.id, "from_cache": from_cache},
        )

    async def _handle_failure(
        self, job: Document, error: str, attempt: int
    ) -> ProcessingResult:
        max_attempts = self._settings.job_max_attempts

        if attempt < max_attempts:
            if not await self._repository.release_for_retry(job.id, error):
                return ProcessingResult(outcome=Outcome.SKIPPED)
            logger.warning(
                "processing failed, handing back to celery for retry",
                extra={"document_id": job.id, "attempt": attempt, "error": error},
            )
            return ProcessingResult(outcome=Outcome.RETRY, error=error)

        if not await self._repository.mark_failed(job.id, error):
            return ProcessingResult(outcome=Outcome.SKIPPED)

        await self._rate_limiter.release(job.user_id)
        logger.error(
            "processing failed permanently",
            extra={"document_id": job.id, "attempt": attempt, "error": error},
        )
        return ProcessingResult(outcome=Outcome.FAILED, error=error)


@asynccontextmanager
async def build_processor() -> AsyncIterator[JobProcessor]:
    active = get_settings()
    mongo = MongoConnection(active)
    redis_connection = RedisConnection(active)

    database = await mongo.connect()
    redis = await redis_connection.connect()

    try:
        repository = DocumentRepository(database)
        yield JobProcessor(
            repository=repository,
            cache=SummaryCache(redis, active.summary_cache_ttl_seconds),
            rate_limiter=ActiveJobRateLimiter(
                redis=redis,
                limit=active.max_active_jobs_per_user,
                key_ttl_seconds=active.rate_limit_key_ttl_seconds,
                fallback_counter=repository.count_active_for_user,
            ),
            summarizer=MockSummarizer(
                min_duration=active.job_min_duration_seconds,
                max_duration=active.job_max_duration_seconds,
                failure_rate=active.job_failure_rate,
            ),
            settings=active,
        )
    finally:
        await redis_connection.close()
        await mongo.close()


def retry_delay(retries: int, base: float, maximum: float) -> float:
    return min(base * (2**retries), maximum)


async def _process(document_id: str, attempt: int) -> ProcessingResult:
    async with build_processor() as processor:
        return await processor.process(document_id, attempt)


@celery_app.task(bind=True, name="documents.summarize", max_retries=None, acks_late=True)
def summarize_document(self: Task, document_id: str) -> str:
    active = get_settings()
    attempt = self.request.retries + 1

    result = asyncio.run(_process(document_id, attempt))

    if result.outcome is not Outcome.RETRY:
        return result.outcome.value

    delay = retry_delay(
        retries=self.request.retries,
        base=active.job_retry_backoff_base_seconds,
        maximum=active.job_retry_backoff_max_seconds,
    )
    logger.info(
        "scheduling celery retry",
        extra={"document_id": document_id, "attempt": attempt, "retry_in_seconds": delay},
    )
    raise self.retry(countdown=delay)
