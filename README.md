# Document Insights API

Submit a document, and the service summarizes it in the background. Because summarizing takes 10–30 seconds, the API saves your document, hands the work to a background worker, and replies immediately with an ID. You poll that ID until the summary is ready.

Built with **FastAPI**, **MongoDB**, **Redis** and **Celery**.

---

## Setup

### Option 1 — Docker (recommended)

Only Docker is needed. Nothing else to install.

```bash
git clone https://github.com/devarsh-13/document-insights-api.git
cd document-insights-api
docker compose up --build
```

That's it. Four containers start: the API, a Celery worker, MongoDB and Redis.

Wait for this line, then open <http://localhost:8000/docs>:

```
api-1  | INFO:     Application startup complete.
```

Check it's healthy:

```bash
curl http://localhost:8000/health
```

```json
{"status":"healthy","database":"document_insights",
 "components":{"mongodb":{"status":"up"},"redis":{"status":"up"}}}
```

To stop: `docker compose down` (add `-v` to also delete the stored data).

### Option 2 — Run locally

You'll need Python 3.11+, plus MongoDB and Redis running on your machine.

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt      # Windows
# .venv/bin/pip install -r requirements.txt        # macOS / Linux
```

Copy the config template. **The app will not start without it** — this is deliberate, see [Configuration](#configuration).

```bash
cp .env.example .env
```

Then run the API and the worker in two separate terminals:

```bash
.venv/Scripts/python -m uvicorn app.main:app --reload
```

```bash
.venv/Scripts/python -m celery -A app.worker worker --pool=solo --loglevel=info
```

> **Windows note:** `--pool=solo` is required. Celery's default worker pool needs `fork()`, which Windows doesn't have. Inside Docker (Linux) the default works fine, which is why `docker-compose.yml` doesn't use the flag.

---

## Try it

**1. Submit a document**

```bash
curl -X POST http://localhost:8000/documents \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"alice","title":"Q3 Report","content":"Revenue grew 12 percent. Costs held flat."}'
```

You get back an ID straight away, with status `queued`:

```json
{"document_id":"6a97b2ac501f8a096ffd5092","user_id":"alice","status":"queued","cached":false}
```

**2. Poll it**

```bash
curl http://localhost:8000/documents/6a97b2ac501f8a096ffd5092
```

For the first 10–30 seconds you'll see `queued` then `processing`. After that:

```json
{"status":"completed",
 "summary":{"text":"'Q3 Report' contains 8 words...","word_count":8,
            "keywords":["revenue","grew","percent","costs","held"],
            "reading_time_seconds":2}}
```

**3. Watch the worker do it**

```bash
docker compose logs -f worker
```

```
worker-1 | Task documents.summarize[b09e8c38...] received
worker-1 | processing document
worker-1 | document completed
worker-1 | Task documents.summarize[b09e8c38...] succeeded in 21.86s: 'completed'
```

**4. List a user's documents**

```bash
curl 'http://localhost:8000/users/alice/documents?page=1&page_size=10&status=completed'
```

**5. See the rate limit work** — submit four documents quickly. The fourth returns `429`, because each user may only have three being processed at once.

**6. See the cache work** — submit the *exact same content* twice. The second one comes back instantly, already `completed`, with `"cached": true`. No worker involved.

### Using Postman instead

`postman_collection.json` in the repo root can be imported straight into Postman. It has one folder per endpoint, with ready-made requests covering the cache, the rate limit, and the error cases. Each request carries assertions that run automatically.

---

## The endpoints

| Method | Path | What it does | Returns |
| --- | --- | --- | --- |
| `POST` | `/documents` | Submit a document | `201` — or `429` if you already have 3 in progress, `422` if the payload is invalid |
| `GET` | `/documents/{id}` | Check status, get the summary | `200` — or `404` if not found |
| `GET` | `/users/{user_id}/documents` | List documents, paginated | `200`. Supports `page`, `page_size`, `status` |
| `GET` | `/health` | Are MongoDB and Redis reachable? | `200` healthy — `503` if MongoDB is down |

A document moves through four states:

```
queued  →  processing  →  completed
                       ↘  failed      (after 3 failed attempts)
```

Errors always look the same:

```json
{ "detail": "a human readable message", "code": "a_machine_readable_code" }
```

---

## How it works

```
POST /documents
  │
  ├─ Have we summarized this exact content before?
  │     YES → return the saved summary immediately (no worker needed)
  │     NO  ↓
  │
  ├─ Does this user already have 3 documents in progress?
  │     YES → 429 Too Many Requests
  │     NO  ↓
  │
  ├─ Save to MongoDB as "queued"
  ├─ Send a job message to Redis
  └─ Return 201 with the document ID


Celery worker (a separate process)
  │
  ├─ Pick up the job, mark the document "processing"
  ├─ Summarize it (10–30 seconds, fails ~10% of the time on purpose)
  │
  ├─ Success → save summary, cache it, free the user's slot
  └─ Failure → back to "queued", retry after 5s, then 10s, then 20s
               after 3 attempts → "failed", with the error saved
```

The API and the worker are **completely separate programs**. They never call each other — the API drops a message into Redis, and the worker picks it up. They could run on different machines.

---

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

```
59 passed in 0.97s
```

**You do not need MongoDB, Redis, Docker, or anything else running.** The tests swap in in-memory fakes: `mongomock-motor` stands in for MongoDB and `fakeredis` for Redis. This works because the app is built with dependency injection — every component is handed its dependencies rather than creating them, so tests can hand it a fake instead.

I verified this properly by running the suite with deliberately unreachable database addresses. All 59 still passed, which proves nothing quietly connects to a real database.

### What's in the three files

| File | What it covers |
| --- | --- |
| `tests/conftest.py` | Shared setup ("fixtures") — the fake database, fake Redis, and a test version of the app |
| `tests/test_api.py` | 21 tests through real HTTP calls: status codes, validation, pagination, rate limiting, health |
| `tests/test_worker.py` | 38 tests of the internals: hashing, caching, rate limiting, the worker, retries |

### The tests worth knowing about

These check the tricky behaviour, not just the happy path:

- **`test_limiter_is_atomic_under_concurrency`** — fires 10 requests at the same instant and confirms exactly 3 are allowed. A simpler implementation would let several through.
- **`test_only_one_worker_can_claim_a_document`** — five workers grab the same job simultaneously; exactly one wins.
- **`test_redelivered_job_is_skipped`** — if the same job is delivered twice, the second is ignored instead of doing the work again.
- **`test_degraded_fallback_still_enforces_the_limit`** — with Redis failing on every call, the rate limit is *still* correctly enforced by counting in MongoDB.
- **`test_submission_survives_a_broker_outage`** — if the queue is down, your document is still safely saved.
- **`test_slot_is_held_across_retries`** — a document waiting to retry still counts against your limit.
- **`test_health_is_degraded_when_only_redis_is_down`** — health reports "degraded" rather than "unhealthy", because the service still works.
- **`test_only_four_endpoints_are_exposed`** — guards against accidentally adding endpoints the brief didn't ask for.

`pytest -v` lists every test by name if you want to read through them.

---

## Design decisions

### Why Celery instead of writing my own worker

My first version was a hand-written worker: a loop polling MongoDB, job "leases" with heartbeats so a crashed worker's job wasn't lost, a cleanup loop, and manual retry scheduling. It worked — but it was a few hundred lines rebuilding what a job queue already does, and I'd be discovering its bugs in production instead of reading about them in someone's documentation.

Celery replaces all of that with configuration. Retries with backoff, scaling to more workers, and crash recovery come from a library that's had a decade to find those edge cases.

Redis does three separate jobs here, kept apart so they can't interfere: **database 0** holds the rate-limit counters and the summary cache, **database 1** is the Celery job queue.

### Making sure a job is never done twice

Celery is set up so a job is only marked "done" *after* it finishes (`acks_late=True`). If a worker crashes halfway, the job comes back rather than disappearing. Without this, restarting the worker would silently lose whatever it was working on.

The trade-off is that a job can arrive **twice** — after a crash, or a network hiccup. So the worker has to be safe to run twice, and this is what makes it safe:

```python
find_one_and_update(
    {"_id": document_id, "status": "queued"},        # find it, but only if still queued
    {"$set": {"status": "processing"}},              # and claim it, in the same operation
)
```

Read that as one sentence: *find this document, but only if it's still queued, and in the same breath mark it as processing.* MongoDB guarantees both happen together with nothing able to interrupt.

So if the job arrives twice, the second attempt finds nothing matching (the status already changed) and simply exits. Same protection stops two workers grabbing the same document.

One setting is checked at startup: the job redelivery window (600s) must be longer than the longest job (30s). Otherwise Redis would redeliver jobs that are still perfectly fine, and every slow job would run twice. The app refuses to start if that's misconfigured.

### The rate limit: why it needs a Lua script

Each user gets 3 documents in progress at once. The obvious way is two steps — read the count, then add one. But that's a race: three requests arriving together all read `2`, all decide there's room, and all add one. Now the user has five, and the limit meant nothing.

So the check and the increment run inside a small Lua script, which Redis executes as a **single uninterruptible operation**:

```lua
local current = redis.call('GET', KEYS[1])
if tonumber(current) >= tonumber(ARGV[1]) then return -1 end
return redis.call('INCR', KEYS[1])
```

**If Redis goes down**, the limiter falls back to counting documents directly in MongoDB. Slower, but the limit still holds and the API keeps working. The brief asked for graceful degradation — this is it.

The counter could drift over time (say a process dies at exactly the wrong moment), so three things keep it honest: the slot is released if the database write fails; when the counter is missing it's rebuilt from MongoDB rather than starting at zero; and the key expires after an hour of inactivity, so any drift heals itself.

### The cache

The cache key is a SHA-256 fingerprint of the content. Two details matter:

- **The user ID is part of the fingerprint**, so one user's cached summaries can never be served to another. That costs a little cache efficiency and buys proper isolation between users.
- **A separator byte sits between the user ID and the content.** Without it, user `"ab"` with content `"c"` and user `"a"` with content `"bc"` would produce the same fingerprint.

**A cache hit doesn't use up one of your 3 slots.** The cache is checked *before* the rate limit, because a cache hit does no background work — it shouldn't cost you capacity.

Entries expire after 24 hours. If Redis fails, a cache read just behaves like a miss and the document gets summarized normally.

### Database design

Everything lives in one MongoDB collection, with three indexes matching the three queries actually run:

| Index | Used for |
| --- | --- |
| `user_id + status` | Counting a user's active jobs |
| `user_id + created_at` | Listing documents |
| `user_id + status + created_at` | Listing filtered by status |

Listing sorts by `created_at`, then by `_id` as a tiebreaker. Without the tiebreaker, two documents created in the same millisecond could show up on two different pages, or on neither.

MongoDB's `ObjectId` never leaves the database layer — it's converted to a string on the way out. An invalid ID returns `404` rather than crashing, since "malformed" and "doesn't exist" mean the same thing to whoever's calling.

### Code structure

Three layers, each only talking to the one below:

```
api.py          handles HTTP — knows nothing about MongoDB
services.py     the business rules — knows nothing about HTTP
repository.py   all database queries — the only file that touches MongoDB
```

This is also what makes the tests simple: the services can be handed a fake database because they never create one themselves.

---

## Assumptions

The brief said to make reasonable assumptions and document them. Here they are:

- **No authentication.** `user_id` comes from the request body, since that's what the brief's payload shows. In a real system it would come from a verified login token — as written, anyone can submit as, or read documents belonging to, any user. This is the first thing I'd change.

- **Documents can't be edited or deleted.** No such endpoints exist; nothing in the brief asked for them.

- **The full document text is stored** on the record. At the 100KB limit I set, that's well within MongoDB's 16MB per-document cap. For genuinely large files I'd store the text elsewhere and keep only a reference.

- **The cache is per user, not global.** Two users submitting identical text get two independent summaries. Sharing the cache between them would be more efficient but would leak one user's data to another.

- **The limit counts jobs in progress, not requests per minute.** The brief describes "3 documents in queued or processing state", which is a concurrency limit rather than a rate limit, so there's no time window involved.

- **Listing documents for an unknown user returns an empty list with `200`, not `404`.** There's no user database, so the service genuinely can't tell "this user doesn't exist" apart from "this user has no documents" — and both mean the same thing to the caller.

- **The summary is a flexible object** rather than a fixed shape, since a real AI response format would likely change. It currently holds the summary text, word and character counts, keywords, and an estimated reading time.

- **A cache hit still creates a new document record.** The user did submit a document and expects to see it in their list — it just didn't need reprocessing. It's marked `"cached": true` so the difference is visible.

---

## Known limitations

Being honest about what isn't perfect:

**Two identical documents submitted at the same moment will both be processed.** The worker checks the cache before starting, which handles the common case of someone resubmitting later. But if the first one hasn't finished yet, there's nothing cached to find. Fixing this completely needs a short-lived lock on the content fingerprint. The wasted work is bounded and nothing breaks — it's just not free.

**The rate-limit counter and the database aren't updated together.** They're two separate systems with no shared transaction. The safeguards above keep any drift small and self-healing, but a badly-timed crash could briefly cost a user one slot until the counter expires. Making it exact would mean putting the counter in MongoDB (a write on every request, losing the fast atomic check) — not worth it for a problem whose worst case is "wait a bit longer to submit a fourth document".

**If the job queue is unreachable when you submit**, the document is saved but never picked up, and stays `queued` forever. Your data is safe and the failure is logged, but nothing retries it. See the next section.

---

## What I'd do with more time

Roughly in the order I'd tackle them:

1. **Structured logging.** The code already attaches useful context to log lines (document ID, user ID, attempt number), but the logger outputs plain text so that context is thrown away. Switching to JSON output and adding a request ID would make logs actually searchable.

2. **Authentication.** As above — `user_id` is currently just trusted.

3. **A recovery sweeper.** A scheduled task that looks for documents stuck in `queued` or `processing` for too long and re-queues them. This closes the gap described in Known Limitations, and would use Celery Beat.

4. **Somewhere for failed jobs to go.** Documents that fail all 3 attempts just sit there. They should be routed somewhere visible with a way to inspect and retry them.

5. **Metrics.** Queue depth is the number you'd want alerts on, and right now you can only see it by asking Redis directly. Cache hit rate, retry rate and job duration would follow.

6. **Better pagination.** Page numbers get slow on deep pages and can skip or repeat items if documents are added while you're paging. A cursor fixes both.

7. **Idempotency keys.** If a client retries a request that actually succeeded, they get two documents. An `Idempotency-Key` header would prevent that.

8. **CI.** GitHub Actions running the linter, type checker and tests on every push.

---

## Configuration

Everything is set through environment variables — no connection strings are written into the code. `.env.example` lists them all with sensible defaults.

**`MONGO_URI`, `MONGO_DB_NAME`, `REDIS_URL` and `CELERY_BROKER_URL` have no defaults on purpose.** If they're missing, the app refuses to start:

```
ValidationError: 2 validation errors for Settings
mongo_uri
  Field required
```

That's deliberate. A default of `localhost` would mean a misconfigured production deploy quietly connects to nothing, and you'd debug a confusing timeout instead of reading a clear error at startup.

| Variable | Default | What it does |
| --- | --- | --- |
| `MONGO_URI` | *required* | MongoDB connection string |
| `MONGO_DB_NAME` | *required* | Database name |
| `REDIS_URL` | *required* | Redis for cache and rate limiting |
| `CELERY_BROKER_URL` | *required* | Redis database used as the job queue |
| `MAX_ACTIVE_JOBS_PER_USER` | `3` | How many documents one user can have in progress |
| `SUMMARY_CACHE_TTL_SECONDS` | `86400` | How long cached summaries live (24 hours) |
| `MAX_CONTENT_LENGTH` | `100000` | Largest document accepted |
| `JOB_MIN_DURATION_SECONDS` | `10` | Shortest simulated processing time |
| `JOB_MAX_DURATION_SECONDS` | `30` | Longest simulated processing time |
| `JOB_FAILURE_RATE` | `0.1` | Fraction of jobs that fail on purpose |
| `JOB_MAX_ATTEMPTS` | `3` | Tries before giving up permanently |
| `MAX_PAGE_SIZE` | `100` | Largest allowed `page_size` |

To run more workers: `docker compose up --scale worker=4`. Job claiming is safe across any number of them.

---

## Project structure

Eight files, about 1,360 lines.

```
app/
  main.py          Starts the app; opens database connections once at startup
  config.py        All settings, read from environment variables
  models.py        The document's shape, its statuses, and the API request/response formats
  db.py            MongoDB and Redis connections, and index creation
  repository.py    Every database query lives here and nowhere else
  services.py      Business rules: caching, rate limiting, handling a submission
  worker.py        The Celery job: claim, summarize, save, retry
  api.py           The HTTP endpoints and error handling

tests/
  conftest.py      Shared test setup
  test_api.py      Tests through real HTTP requests
  test_worker.py   Tests of the internals

Dockerfile
docker-compose.yml
.env.example
postman_collection.json   Importable Postman collection, one folder per endpoint
```

**A note on the Docker setup:** only the API is exposed on a port (8000). MongoDB and Redis are reachable between the containers but not from your machine — that avoids clashing with anything already installed locally, and not exposing databases publicly is the safer default. To inspect them during development, use `docker compose exec mongo mongosh`.
