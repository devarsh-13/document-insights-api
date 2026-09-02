from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    mongo_uri: str = Field(min_length=1)
    mongo_db_name: str = Field(min_length=1)

    mongo_min_pool_size: int = Field(default=5, ge=0)
    mongo_max_pool_size: int = Field(default=50, ge=1)
    mongo_timeout_ms: int = Field(default=5_000, ge=100)

    redis_url: str = Field(min_length=1)
    redis_max_connections: int = Field(default=50, ge=1)
    redis_timeout_seconds: float = Field(default=2.0, gt=0)

    max_active_jobs_per_user: int = Field(default=3, ge=1)
    summary_cache_ttl_seconds: int = Field(default=86_400, ge=1)
    rate_limit_key_ttl_seconds: int = Field(default=3_600, ge=60)
    max_content_length: int = Field(default=100_000, ge=1)

    celery_broker_url: str = Field(min_length=1)
    celery_visibility_timeout_seconds: int = Field(default=600, ge=60)

    job_min_duration_seconds: float = Field(default=10.0, ge=0)
    job_max_duration_seconds: float = Field(default=30.0, ge=0)
    job_failure_rate: float = Field(default=0.1, ge=0.0, le=1.0)
    job_max_attempts: int = Field(default=3, ge=1)
    job_retry_backoff_base_seconds: float = Field(default=5.0, gt=0)
    job_retry_backoff_max_seconds: float = Field(default=300.0, gt=0)

    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65_535)

    default_page_size: int = Field(default=20, ge=1)
    max_page_size: int = Field(default=100, ge=1)

    @property
    def base_url(self) -> str:
        return f"http://{self.api_host}:{self.api_port}"

    @model_validator(mode="after")
    def _check_ranges(self) -> "Settings":
        if self.mongo_max_pool_size < self.mongo_min_pool_size:
            raise ValueError("mongo_max_pool_size must be >= mongo_min_pool_size")
        if self.max_page_size < self.default_page_size:
            raise ValueError("max_page_size must be >= default_page_size")
        if self.job_max_duration_seconds < self.job_min_duration_seconds:
            raise ValueError("job_max_duration_seconds must be >= job_min_duration_seconds")
        if self.celery_visibility_timeout_seconds <= self.job_max_duration_seconds:
            raise ValueError(
                "celery_visibility_timeout_seconds must exceed job_max_duration_seconds "
                "or the broker will redeliver jobs that are still running"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
