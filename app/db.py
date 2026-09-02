import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import Settings, get_settings
from app.models import DOCUMENT_INDEXES, DOCUMENTS_COLLECTION

logger = logging.getLogger(__name__)


class NotConnectedError(RuntimeError):
    pass


class MongoConnection:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client: AsyncIOMotorClient | None = None

    @property
    def client(self) -> AsyncIOMotorClient:
        if self._client is None:
            raise NotConnectedError("connect() must be called before using mongodb")
        return self._client

    @property
    def database(self) -> AsyncIOMotorDatabase:
        return self.client[self._settings.mongo_db_name]

    async def connect(self) -> AsyncIOMotorDatabase:
        if self._client is not None:
            return self.database

        self._client = AsyncIOMotorClient(
            self._settings.mongo_uri,
            minPoolSize=self._settings.mongo_min_pool_size,
            maxPoolSize=self._settings.mongo_max_pool_size,
            serverSelectionTimeoutMS=self._settings.mongo_timeout_ms,
            connectTimeoutMS=self._settings.mongo_timeout_ms,
            socketTimeoutMS=self._settings.mongo_timeout_ms * 5,
            retryWrites=True,
            tz_aware=True,
            uuidRepresentation="standard",
        )

        await self._client.admin.command("ping")
        logger.info(
            "connected to mongodb",
            extra={
                "database": self._settings.mongo_db_name,
                "pool": f"{self._settings.mongo_min_pool_size}-{self._settings.mongo_max_pool_size}",
            },
        )
        return self.database

    async def close(self) -> None:
        if self._client is None:
            return
        self._client.close()
        self._client = None
        logger.info("closed mongodb connection pool")

    async def ping(self) -> bool:
        try:
            await self.client.admin.command("ping")
        except (PyMongoError, NotConnectedError) as exc:
            logger.warning("mongodb ping failed: %s", exc)
            return False
        return True


class RedisConnection:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise NotConnectedError("connect() must be called before using redis")
        return self._client

    async def connect(self) -> Redis:
        if self._client is not None:
            return self._client

        self._client = Redis.from_url(
            self._settings.redis_url,
            decode_responses=True,
            max_connections=self._settings.redis_max_connections,
            socket_timeout=self._settings.redis_timeout_seconds,
            socket_connect_timeout=self._settings.redis_timeout_seconds,
            health_check_interval=30,
        )

        await self._client.ping()
        logger.info("connected to redis")
        return self._client

    async def close(self) -> None:
        if self._client is None:
            return
        await self._client.aclose()
        self._client = None
        logger.info("closed redis connection pool")

    async def ping(self) -> bool:
        try:
            await self.client.ping()
        except (RedisError, OSError, NotConnectedError) as exc:
            logger.warning("redis ping failed: %s", exc)
            return False
        return True


async def ensure_indexes(database: AsyncIOMotorDatabase) -> list[str]:
    collection = database[DOCUMENTS_COLLECTION]
    created = [
        await collection.create_index(index.keys, name=index.name)
        for index in DOCUMENT_INDEXES
    ]
    logger.info("ensured indexes", extra={"count": len(created)})
    return created
