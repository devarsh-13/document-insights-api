import asyncio
import random

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.services import (
    ActiveJobRateLimiter,
    DocumentNotFoundError,
    RateLimitExceededError,
    SummaryCache,
    compute_content_hash,
)
from app.worker import MockSummarizer, Outcome, SummarizationError, build_summary, retry_delay

CONTENT = "A body of text. It has two sentences."


async def submit(service, content=CONTENT, user_id="alice"):
    return await service.submit(user_id=user_id, title="Title", content=content)


async def slot_count(redis, user_id="alice"):
    return await redis.get(ActiveJobRateLimiter.key(user_id))


class BrokenRedis:
    async def _fail(self, *args, **kwargs):
        raise RedisConnectionError("redis is down")

    eval = get = set = delete = _fail


def counter(value: int):
    async def _count(_user_id: str) -> int:
        return value

    return _count


def limiter_with(redis, active: int = 0, limit: int = 3) -> ActiveJobRateLimiter:
    return ActiveJobRateLimiter(
        redis=redis, limit=limit, key_ttl_seconds=60, fallback_counter=counter(active)
    )


def test_content_hash_is_stable_and_user_scoped():
    assert compute_content_hash("alice", "same") == compute_content_hash("alice", "same")
    assert compute_content_hash("alice", "same") != compute_content_hash("bob", "same")
    assert len(compute_content_hash("alice", "same")) == 64


def test_content_hash_separator_prevents_collision():
    assert compute_content_hash("ab", "c") != compute_content_hash("a", "bc")


async def test_cache_round_trip_and_ttl(cache, redis, settings):
    await cache.set("alice", "hash-1", {"text": "hello"}, "doc-1")

    entry = await cache.get("alice", "hash-1")

    assert entry["summary"] == {"text": "hello"}
    assert entry["source_document_id"] == "doc-1"
    assert 0 < await redis.ttl(SummaryCache.key("alice", "hash-1")) <= settings.summary_cache_ttl_seconds


async def test_cache_miss_returns_none(cache):
    assert await cache.get("alice", "missing") is None


async def test_malformed_cache_entry_is_discarded(cache, redis):
    await redis.set(SummaryCache.key("alice", "bad"), "{not json")

    assert await cache.get("alice", "bad") is None
    assert await redis.exists(SummaryCache.key("alice", "bad")) == 0


async def test_cache_failure_degrades_to_a_miss(settings):
    cache = SummaryCache(BrokenRedis(), settings.summary_cache_ttl_seconds)

    assert await cache.get("alice", "hash-1") is None
    assert await cache.set("alice", "hash-1", {"text": "x"}, "doc-1") is False


async def test_limiter_allows_up_to_the_limit(rate_limiter):
    assert [(await rate_limiter.reserve("alice")).allowed for _ in range(4)] == [
        True,
        True,
        True,
        False,
    ]


async def test_limiter_is_atomic_under_concurrency(rate_limiter, settings):
    results = await asyncio.gather(*[rate_limiter.reserve("alice") for _ in range(10)])

    assert sum(1 for r in results if r.allowed) == settings.max_active_jobs_per_user


async def test_release_frees_a_slot(rate_limiter):
    for _ in range(3):
        await rate_limiter.reserve("alice")
    assert (await rate_limiter.reserve("alice")).allowed is False

    await rate_limiter.release("alice")

    assert (await rate_limiter.reserve("alice")).allowed is True


async def test_release_never_goes_negative(rate_limiter, redis):
    await rate_limiter.reserve("alice")
    for _ in range(5):
        await rate_limiter.release("alice")

    assert await slot_count(redis) == "0"


async def test_counter_is_seeded_from_mongodb(redis):
    reservation = await limiter_with(redis, active=3).reserve("alice")

    assert reservation.allowed is False
    assert reservation.degraded is False


async def test_limiter_falls_back_to_mongodb_when_redis_fails():
    reservation = await limiter_with(BrokenRedis(), active=1).reserve("alice")

    assert reservation.allowed is True
    assert reservation.degraded is True


async def test_degraded_fallback_still_enforces_the_limit():
    reservation = await limiter_with(BrokenRedis(), active=3).reserve("alice")

    assert reservation.allowed is False
    assert reservation.degraded is True


async def test_release_on_broken_redis_does_not_raise():
    await limiter_with(BrokenRedis()).release("alice")


def test_build_summary_extracts_structure():
    summary = build_summary("Report", "Alpha beta alpha. Gamma alpha beta delta epsilon.")

    assert summary["word_count"] == 8
    assert "alpha" in summary["keywords"]
    assert summary["reading_time_seconds"] >= 1


async def test_summarizer_can_fail():
    with pytest.raises(SummarizationError):
        await MockSummarizer(0.0, 0.0, failure_rate=1.0).summarize("t", "c")


def test_summarizer_duration_stays_within_bounds():
    summarizer = MockSummarizer(10.0, 30.0, 0.0, rng=random.Random(7))

    assert all(10.0 <= summarizer.next_duration() <= 30.0 for _ in range(200))


def test_retry_delay_is_exponential_and_capped():
    assert [retry_delay(n, 5.0, 300.0) for n in range(4)] == [5.0, 10.0, 20.0, 40.0]
    assert retry_delay(20, 5.0, 300.0) == 300.0


async def test_claim_moves_document_to_processing(service, repository):
    submission = await submit(service)

    job = await repository.claim(submission.document_id, attempt=1)

    assert job.status == "processing"
    assert job.attempts == 1


async def test_only_one_worker_can_claim_a_document(service, repository):
    submission = await submit(service)

    claims = await asyncio.gather(
        *[repository.claim(submission.document_id, attempt=1) for _ in range(5)]
    )

    assert sum(1 for claim in claims if claim is not None) == 1


async def test_processing_completes_the_document(service, repository, processor):
    submission = await submit(service)

    result = await processor.process(submission.document_id, attempt=1)
    document = await repository.get(submission.document_id)

    assert result.outcome is Outcome.COMPLETED
    assert document.status == "completed"
    assert document.summary["word_count"] > 0
    assert document.error is None
    assert document.completed_at is not None


async def test_completion_populates_the_cache(service, processor, cache):
    submission = await submit(service)

    await processor.process(submission.document_id, attempt=1)

    entry = await cache.get("alice", compute_content_hash("alice", CONTENT))
    assert entry["source_document_id"] == submission.document_id


async def test_completion_releases_the_slot(service, processor, redis):
    submission = await submit(service)

    await processor.process(submission.document_id, attempt=1)

    assert await slot_count(redis) == "0"


async def test_redelivered_job_is_skipped(service, repository, processor):
    submission = await submit(service)
    await processor.process(submission.document_id, attempt=1)

    result = await processor.process(submission.document_id, attempt=1)

    assert result.outcome is Outcome.SKIPPED
    assert (await repository.get(submission.document_id)).status == "completed"


async def test_failure_returns_document_to_queued(service, repository, make_processor):
    submission = await submit(service)

    result = await make_processor(failure_rate=1.0).process(submission.document_id, attempt=1)
    document = await repository.get(submission.document_id)

    assert result.outcome is Outcome.RETRY
    assert document.status == "queued"
    assert document.error


async def test_slot_is_held_across_retries(service, make_processor, redis):
    submission = await submit(service)

    await make_processor(failure_rate=1.0).process(submission.document_id, attempt=1)

    assert await slot_count(redis) == "1"


async def test_failure_becomes_permanent_on_final_attempt(
    service, repository, make_processor, redis, settings
):
    processor = make_processor(failure_rate=1.0)
    submission = await submit(service)

    for attempt in range(1, settings.job_max_attempts + 1):
        result = await processor.process(submission.document_id, attempt=attempt)
    document = await repository.get(submission.document_id)

    assert result.outcome is Outcome.FAILED
    assert document.status == "failed"
    assert document.attempts == settings.job_max_attempts
    assert await slot_count(redis) == "0"


async def test_worker_uses_the_cache_instead_of_redoing_work(
    service, repository, cache, make_processor
):
    submission = await submit(service)
    await cache.set("alice", compute_content_hash("alice", CONTENT), {"text": "precomputed"}, "old")

    result = await make_processor(failure_rate=1.0).process(submission.document_id, attempt=1)
    document = await repository.get(submission.document_id)

    assert result.outcome is Outcome.COMPLETED
    assert document.summary == {"text": "precomputed"}
    assert document.cached is True


async def test_identical_content_is_served_from_cache(service, processor):
    first = await submit(service)
    await processor.process(first.document_id, attempt=1)

    second = await submit(service)

    assert second.status == "completed"
    assert second.cached is True
    assert second.summary is not None
    assert second.document_id != first.document_id


async def test_cache_hit_consumes_no_slot(service, processor, redis):
    first = await submit(service)
    await processor.process(first.document_id, attempt=1)

    for _ in range(10):
        await submit(service)

    assert await slot_count(redis) == "0"


async def test_cache_is_scoped_to_the_user(service, processor):
    first = await submit(service)
    await processor.process(first.document_id, attempt=1)

    bob = await submit(service, user_id="bob")

    assert bob.status == "queued"
    assert bob.cached is False


async def test_service_raises_when_limit_reached(service, settings):
    for index in range(settings.max_active_jobs_per_user):
        await submit(service, content=f"body {index}")

    with pytest.raises(RateLimitExceededError) as excinfo:
        await submit(service, content="one too many")

    assert excinfo.value.limit == settings.max_active_jobs_per_user


async def test_service_raises_for_unknown_and_malformed_ids(service):
    with pytest.raises(DocumentNotFoundError):
        await service.get("507f1f77bcf86cd799439011")
    with pytest.raises(DocumentNotFoundError):
        await service.get("nonsense")
