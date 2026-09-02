from datetime import datetime, timezone
from enum import Enum
from typing import Any, NamedTuple

from bson import ObjectId
from bson.errors import InvalidId
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config import get_settings

DOCUMENTS_COLLECTION = "documents"


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


ACTIVE_STATUSES = (DocumentStatus.QUEUED.value, DocumentStatus.PROCESSING.value)


class Index(NamedTuple):
    name: str
    keys: list[tuple[str, int]]


DOCUMENT_INDEXES = [
    Index(name="user_status", keys=[("user_id", 1), ("status", 1)]),
    Index(name="user_recent", keys=[("user_id", 1), ("created_at", -1)]),
    Index(name="user_status_recent", keys=[("user_id", 1), ("status", 1), ("created_at", -1)]),
]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_object_id(raw: str) -> ObjectId | None:
    try:
        return ObjectId(raw)
    except (InvalidId, TypeError, ValueError):
        return None


class Document(BaseModel):
    model_config = ConfigDict(
        use_enum_values=True, populate_by_name=True, validate_default=True
    )

    id: str | None = Field(default=None, alias="_id")
    user_id: str
    title: str
    content: str
    content_hash: str
    status: DocumentStatus = DocumentStatus.QUEUED
    summary: dict[str, Any] | None = None
    error: str | None = None
    attempts: int = 0
    cached: bool = False
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def to_mongo(self) -> dict[str, Any]:
        data = self.model_dump(by_alias=True)
        data.pop("_id", None)
        return data

    @classmethod
    def from_mongo(cls, raw: dict[str, Any]) -> "Document":
        data = dict(raw)
        if "_id" in data:
            data["_id"] = str(data["_id"])
        return cls.model_validate(data)


class DocumentCreateRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    user_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    title: str = Field(min_length=1, max_length=300)
    content: str = Field(min_length=1)

    @field_validator("title", "content")
    @classmethod
    def _reject_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must contain non-whitespace characters")
        return value

    @field_validator("content")
    @classmethod
    def _enforce_max_length(cls, value: str) -> str:
        limit = get_settings().max_content_length
        if len(value) > limit:
            raise ValueError(f"must be at most {limit} characters")
        return value


class DocumentCreateResponse(BaseModel):
    document_id: str
    user_id: str
    status: DocumentStatus
    cached: bool
    created_at: datetime
    summary: dict[str, Any] | None = None


class DocumentResponse(BaseModel):
    document_id: str
    user_id: str
    title: str
    status: DocumentStatus
    content_hash: str
    content_length: int
    attempts: int
    cached: bool
    error: str | None = None
    summary: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @classmethod
    def from_document(cls, document: Document) -> "DocumentResponse":
        return cls(
            document_id=document.id,
            content_length=len(document.content),
            **document.model_dump(
                exclude={"id", "content"}, exclude_none=False
            ),
        )


class DocumentListItem(BaseModel):
    document_id: str
    user_id: str
    title: str
    status: DocumentStatus
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None

    @classmethod
    def from_document(cls, document: Document) -> "DocumentListItem":
        return cls(
            document_id=document.id,
            user_id=document.user_id,
            title=document.title,
            status=document.status,
            created_at=document.created_at,
            updated_at=document.updated_at,
            completed_at=document.completed_at,
        )


class DocumentPageResponse(BaseModel):
    items: list[DocumentListItem]
    page: int
    page_size: int
    total: int
    total_pages: int
    has_next: bool


class ComponentHealth(BaseModel):
    status: str
    latency_ms: float | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
    components: dict[str, ComponentHealth]


class ErrorResponse(BaseModel):
    detail: str
    code: str | None = None
