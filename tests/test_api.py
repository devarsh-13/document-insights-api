import pytest

from app.models import Document
from app.services import compute_content_hash
from tests.conftest import FakeDispatcher, api_client

BODY = {
    "user_id": "alice",
    "title": "Quarterly Report",
    "content": "Revenue grew this quarter. Costs stayed flat. The outlook remains positive.",
}


def body(**overrides):
    return {**BODY, **overrides}


async def test_submit_returns_201_and_queued(client):
    response = await client.post("/documents", json=BODY)
    payload = response.json()

    assert response.status_code == 201
    assert payload["status"] == "queued"
    assert payload["cached"] is False
    assert payload["summary"] is None
    assert payload["document_id"]


async def test_submit_enqueues_one_background_job(client, dispatcher):
    response = await client.post("/documents", json=BODY)

    assert dispatcher.enqueued == [response.json()["document_id"]]


async def test_submitted_document_is_retrievable(client):
    document_id = (await client.post("/documents", json=BODY)).json()["document_id"]

    payload = (await client.get(f"/documents/{document_id}")).json()

    assert payload["document_id"] == document_id
    assert payload["user_id"] == "alice"
    assert payload["status"] == "queued"
    assert payload["attempts"] == 0
    assert len(payload["content_hash"]) == 64


async def test_detail_response_omits_the_raw_content(client):
    document_id = (await client.post("/documents", json=BODY)).json()["document_id"]

    payload = (await client.get(f"/documents/{document_id}")).json()

    assert "content" not in payload
    assert payload["content_length"] == len(BODY["content"])


async def test_unknown_document_returns_404(client):
    response = await client.get("/documents/507f1f77bcf86cd799439011")

    assert response.status_code == 404
    assert response.json()["code"] == "document_not_found"


async def test_malformed_document_id_returns_404(client):
    response = await client.get("/documents/not-an-object-id")

    assert response.status_code == 404


@pytest.mark.parametrize(
    "invalid",
    [
        {"user_id": "", "title": "t", "content": "c"},
        {"user_id": "has spaces", "title": "t", "content": "c"},
        {"user_id": "alice", "title": "   ", "content": "c"},
        {"user_id": "alice", "title": "t", "content": ""},
        {"user_id": "alice", "title": "t"},
        {"user_id": "alice", "title": "t", "content": "c", "unexpected": 1},
    ],
)
async def test_invalid_payloads_are_rejected(client, invalid):
    assert (await client.post("/documents", json=invalid)).status_code == 422


async def test_oversized_content_is_rejected(client, settings):
    response = await client.post(
        "/documents", json=body(content="x" * (settings.max_content_length + 1))
    )

    assert response.status_code == 422


async def test_fourth_submission_is_rate_limited(client):
    for index in range(3):
        accepted = await client.post("/documents", json=body(content=f"unique body {index}"))
        assert accepted.status_code == 201

    rejected = await client.post("/documents", json=body(content="one document too many"))

    assert rejected.status_code == 429
    assert rejected.json()["code"] == "active_job_limit_reached"
    assert rejected.json()["limit"] == 3
    assert rejected.headers["retry-after"] == "10"


async def test_rate_limit_is_per_user(client):
    for index in range(3):
        await client.post("/documents", json=body(content=f"alice body {index}"))

    response = await client.post("/documents", json=body(user_id="bob", content="bob body"))

    assert response.status_code == 201


async def test_rate_limited_submission_is_not_enqueued(client, dispatcher):
    for index in range(3):
        await client.post("/documents", json=body(content=f"body {index}"))
    dispatcher.enqueued.clear()

    await client.post("/documents", json=body(content="rejected body"))

    assert dispatcher.enqueued == []


async def test_listing_is_paginated_newest_first(client, repository):
    for index in range(5):
        await repository.insert(
            Document(
                user_id="carol",
                title=f"Doc {index}",
                content=f"carol body {index}",
                content_hash=compute_content_hash("carol", f"carol body {index}"),
            )
        )

    first = (await client.get("/users/carol/documents", params={"page": 1, "page_size": 2})).json()
    second = (await client.get("/users/carol/documents", params={"page": 2, "page_size": 2})).json()
    third = (await client.get("/users/carol/documents", params={"page": 3, "page_size": 2})).json()

    assert first["total"] == 5
    assert first["total_pages"] == 3
    assert first["has_next"] is True
    assert len(first["items"]) == 2
    assert third["has_next"] is False
    assert len(third["items"]) == 1
    assert not {d["document_id"] for d in first["items"]} & {d["document_id"] for d in second["items"]}


async def test_listing_filters_by_status(client, database):
    for index in range(3):
        await client.post("/documents", json=body(user_id="dave", content=f"dave body {index}"))
    await database["documents"].update_one({"user_id": "dave"}, {"$set": {"status": "completed"}})

    completed = (await client.get("/users/dave/documents", params={"status": "completed"})).json()
    queued = (await client.get("/users/dave/documents", params={"status": "queued"})).json()

    assert completed["total"] == 1
    assert queued["total"] == 2


async def test_invalid_status_filter_is_rejected(client):
    response = await client.get("/users/alice/documents", params={"status": "banana"})

    assert response.status_code == 422


async def test_page_size_is_capped(client, settings):
    response = await client.get(
        "/users/alice/documents", params={"page_size": settings.max_page_size + 500}
    )

    assert response.json()["page_size"] == settings.max_page_size


async def test_unknown_user_returns_empty_page(client):
    payload = (await client.get("/users/nobody/documents")).json()

    assert payload["items"] == []
    assert payload["total"] == 0
    assert payload["has_next"] is False


async def test_health_reports_both_dependencies(client):
    payload = (await client.get("/health")).json()

    assert payload["status"] == "healthy"
    assert payload["components"]["mongodb"]["status"] == "up"
    assert payload["components"]["redis"]["status"] == "up"


async def test_health_is_degraded_when_only_redis_is_down(settings, database, redis, dispatcher):
    async with api_client(settings, database, redis, dispatcher, redis_up=False) as http:
        response = await http.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


async def test_health_is_503_when_mongodb_is_down(settings, database, redis, dispatcher):
    async with api_client(settings, database, redis, dispatcher, mongo_up=False) as http:
        response = await http.get("/health")

    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"


async def test_submission_survives_a_broker_outage(settings, database, redis):
    broken = FakeDispatcher(working=False)

    async with api_client(settings, database, redis, broken) as http:
        response = await http.post("/documents", json=BODY)
        payload = response.json()
        stored = (await http.get(f"/documents/{payload['document_id']}")).json()

    assert response.status_code == 201
    assert stored["status"] == "queued"


async def test_only_four_endpoints_are_exposed(client):
    schema = (await client.get("/openapi.json")).json()

    assert sorted(schema["paths"]) == [
        "/documents",
        "/documents/{document_id}",
        "/health",
        "/users/{user_id}/documents",
    ]
