import logging
from math import ceil
from typing import NamedTuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import DESCENDING, ReturnDocument

from app.models import (
    ACTIVE_STATUSES,
    DOCUMENTS_COLLECTION,
    Document,
    DocumentStatus,
    to_object_id,
    utcnow,
)

logger = logging.getLogger(__name__)


class DocumentPage(NamedTuple):
    items: list[Document]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        return ceil(self.total / self.page_size) if self.total else 0

    @property
    def has_next(self) -> bool:
        return self.page < self.total_pages


class DocumentRepository:
    def __init__(self, database: AsyncIOMotorDatabase) -> None:
        self._collection = database[DOCUMENTS_COLLECTION]

    async def insert(self, document: Document) -> str:
        result = await self._collection.insert_one(document.to_mongo())
        document_id = str(result.inserted_id)
        logger.info(
            "inserted document",
            extra={"document_id": document_id, "user_id": document.user_id},
        )
        return document_id

    async def get(self, document_id: str) -> Document | None:
        object_id = to_object_id(document_id)
        if object_id is None:
            return None

        raw = await self._collection.find_one({"_id": object_id})
        return Document.from_mongo(raw) if raw is not None else None

    async def count_active_for_user(self, user_id: str) -> int:
        return await self._collection.count_documents(
            {"user_id": user_id, "status": {"$in": list(ACTIVE_STATUSES)}}
        )

    async def list_for_user(
        self,
        user_id: str,
        page: int = 1,
        page_size: int = 20,
        status: DocumentStatus | None = None,
    ) -> DocumentPage:
        query: dict[str, object] = {"user_id": user_id}
        if status is not None:
            query["status"] = status.value

        total = await self._collection.count_documents(query)
        cursor = (
            self._collection.find(query)
            .sort([("created_at", DESCENDING), ("_id", DESCENDING)])
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        items = [Document.from_mongo(raw) async for raw in cursor]

        return DocumentPage(items=items, total=total, page=page, page_size=page_size)

    async def delete_all_for_user(self, user_id: str) -> int:
        result = await self._collection.delete_many({"user_id": user_id})
        return result.deleted_count

    async def claim(self, document_id: str, attempt: int) -> Document | None:
        object_id = to_object_id(document_id)
        if object_id is None:
            return None

        now = utcnow()
        raw = await self._collection.find_one_and_update(
            {"_id": object_id, "status": DocumentStatus.QUEUED.value},
            {
                "$set": {
                    "status": DocumentStatus.PROCESSING.value,
                    "attempts": attempt,
                    "started_at": now,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return Document.from_mongo(raw) if raw is not None else None

    async def mark_completed(
        self, document_id: str, summary: dict[str, object], cached: bool = False
    ) -> bool:
        return await self._finish(
            document_id,
            {
                "status": DocumentStatus.COMPLETED.value,
                "summary": summary,
                "error": None,
                "cached": cached,
                "completed_at": utcnow(),
            },
        )

    async def mark_failed(self, document_id: str, error: str) -> bool:
        return await self._finish(
            document_id,
            {
                "status": DocumentStatus.FAILED.value,
                "error": error,
                "completed_at": utcnow(),
            },
        )

    async def release_for_retry(self, document_id: str, error: str) -> bool:
        return await self._finish(
            document_id,
            {
                "status": DocumentStatus.QUEUED.value,
                "error": error,
                "started_at": None,
            },
        )

    async def _finish(self, document_id: str, changes: dict[str, object]) -> bool:
        object_id = to_object_id(document_id)
        if object_id is None:
            return False

        result = await self._collection.update_one(
            {"_id": object_id, "status": DocumentStatus.PROCESSING.value},
            {"$set": {**changes, "updated_at": utcnow()}},
        )
        return result.modified_count == 1
