import time
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.db import MongoConnection, RedisConnection
from app.models import (
    ComponentHealth,
    DocumentCreateRequest,
    DocumentCreateResponse,
    DocumentListItem,
    DocumentPageResponse,
    DocumentResponse,
    DocumentStatus,
    ErrorResponse,
    HealthResponse,
)
from app.repository import DocumentRepository
from app.services import (
    ActiveJobRateLimiter,
    DocumentNotFoundError,
    DocumentService,
    RateLimitExceededError,
    SummaryCache,
    TaskDispatcher,
)

SettingsDep = Annotated[Settings, Depends(get_settings)]


def get_mongo(request: Request) -> MongoConnection:
    return request.app.state.mongo


def get_redis_connection(request: Request) -> RedisConnection:
    return request.app.state.redis


def get_database(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.database


def get_redis(request: Request) -> Redis:
    return request.app.state.redis.client


def get_dispatcher(request: Request) -> TaskDispatcher:
    return request.app.state.dispatcher


def get_repository(
    database: Annotated[AsyncIOMotorDatabase, Depends(get_database)],
) -> DocumentRepository:
    return DocumentRepository(database)


def get_service(
    repository: Annotated[DocumentRepository, Depends(get_repository)],
    redis: Annotated[Redis, Depends(get_redis)],
    dispatcher: Annotated[TaskDispatcher, Depends(get_dispatcher)],
    settings: SettingsDep,
) -> DocumentService:
    return DocumentService(
        repository=repository,
        cache=SummaryCache(redis, settings.summary_cache_ttl_seconds),
        rate_limiter=ActiveJobRateLimiter(
            redis=redis,
            limit=settings.max_active_jobs_per_user,
            key_ttl_seconds=settings.rate_limit_key_ttl_seconds,
            fallback_counter=repository.count_active_for_user,
        ),
        dispatcher=dispatcher,
        settings=settings,
    )


ServiceDep = Annotated[DocumentService, Depends(get_service)]
MongoDep = Annotated[MongoConnection, Depends(get_mongo)]
RedisDep = Annotated[RedisConnection, Depends(get_redis_connection)]

documents_router = APIRouter(prefix="/documents", tags=["documents"])
users_router = APIRouter(prefix="/users", tags=["users"])
health_router = APIRouter(tags=["health"])


@documents_router.post(
    "",
    response_model=DocumentCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a document for summarization",
    responses={
        429: {"model": ErrorResponse, "description": "Active job limit reached"},
        422: {"model": ErrorResponse, "description": "Invalid payload"},
    },
)
async def submit_document(
    payload: DocumentCreateRequest, service: ServiceDep
) -> DocumentCreateResponse:
    submission = await service.submit(
        user_id=payload.user_id, title=payload.title, content=payload.content
    )
    return DocumentCreateResponse(
        document_id=submission.document_id,
        user_id=submission.user_id,
        status=submission.status,
        cached=submission.cached,
        created_at=submission.created_at,
        summary=submission.summary,
    )


@documents_router.get(
    "/{document_id}",
    response_model=DocumentResponse,
    summary="Fetch processing status and summary",
    responses={404: {"model": ErrorResponse, "description": "Document not found"}},
)
async def get_document(document_id: str, service: ServiceDep) -> DocumentResponse:
    return DocumentResponse.from_document(await service.get(document_id))


@users_router.get(
    "/{user_id}/documents",
    response_model=DocumentPageResponse,
    summary="List a user's documents, newest first",
)
async def list_user_documents(
    service: ServiceDep,
    settings: SettingsDep,
    user_id: Annotated[str, Path(min_length=1, max_length=128)],
    page: Annotated[int, Query(ge=1, description="1-based page number")] = 1,
    page_size: Annotated[int | None, Query(ge=1, description="Items per page")] = None,
    status: Annotated[DocumentStatus | None, Query(description="Filter by status")] = None,
) -> DocumentPageResponse:
    result = await service.list_for_user(
        user_id=user_id,
        page=page,
        page_size=min(page_size or settings.default_page_size, settings.max_page_size),
        status=status,
    )
    return DocumentPageResponse(
        items=[DocumentListItem.from_document(document) for document in result.items],
        page=result.page,
        page_size=result.page_size,
        total=result.total,
        total_pages=result.total_pages,
        has_next=result.has_next,
    )


async def _probe(ping) -> ComponentHealth:
    started = time.perf_counter()
    reachable = await ping()
    latency = round((time.perf_counter() - started) * 1000, 2)
    return ComponentHealth(
        status="up" if reachable else "down", latency_ms=latency if reachable else None
    )


@health_router.get("/health", response_model=HealthResponse, summary="Dependency health")
async def health(
    response: Response, mongo: MongoDep, redis: RedisDep, settings: SettingsDep
) -> HealthResponse:
    components = {"mongodb": await _probe(mongo.ping), "redis": await _probe(redis.ping)}

    if components["mongodb"].status != "up":
        overall = "unhealthy"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    elif components["redis"].status != "up":
        overall = "degraded"
    else:
        overall = "healthy"

    return HealthResponse(
        status=overall, database=settings.mongo_db_name, components=components
    )


def register_routes(app: FastAPI) -> None:
    app.include_router(health_router)
    app.include_router(documents_router)
    app.include_router(users_router)

    @app.exception_handler(DocumentNotFoundError)
    async def _not_found(_: Request, exc: DocumentNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc), "code": "document_not_found"},
        )

    @app.exception_handler(RateLimitExceededError)
    async def _rate_limited(_: Request, exc: RateLimitExceededError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={
                "detail": (
                    f"You already have {exc.limit} documents queued or processing. "
                    "Wait for one to finish before submitting another."
                ),
                "code": "active_job_limit_reached",
                "limit": exc.limit,
            },
            headers={"Retry-After": "10"},
        )
