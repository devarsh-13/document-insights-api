import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.api import register_routes
from app.config import get_settings
from app.db import MongoConnection, RedisConnection, ensure_indexes
from app.services import CeleryTaskDispatcher
from app.worker import celery_app

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    mongo = MongoConnection(settings)
    database = await mongo.connect()
    await ensure_indexes(database)

    redis = RedisConnection(settings)
    await redis.connect()

    app.state.mongo = mongo
    app.state.database = database
    app.state.redis = redis
    app.state.dispatcher = CeleryTaskDispatcher(celery_app)
    logger.info("api ready")

    try:
        yield
    finally:
        await redis.close()
        await mongo.close()
        logger.info("api shut down")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Insights API",
        description="Submit documents and retrieve structured summaries.",
        version="1.0.0",
        lifespan=lifespan,
    )
    register_routes(app)
    return app


app = create_app()
