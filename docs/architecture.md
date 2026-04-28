# Architecture

## Directory Map

```
src/
  config/       — env loading (env.ts) and pipeline config (pipelineConfig.ts)
  jobs/         — in-memory job state (jobStore.ts) and main orchestration (researchPipeline.ts)
  routes/       — Express routes: research.ts (all job/queue endpoints), admin.ts (count override)
  services/     — all external API clients and business logic
    observability/  — CSV I/O (csvReader.ts, csvWriter.ts)
  shared/       — SelectedUser type and guard (selectedUser.ts)
  types/        — domain types: Prospect, EnrichedEmployee, CampaignPushData, etc. (prospect.ts)
frontend/src/   — Vue 3 SPA (Vite); ResearchView.vue is the primary upload/status UI
tests/          — Vitest unit tests, one file per src file
data/           — SQLite databases (gitignored); single file: weekly-success.sqlite
```

---

## Layers and How They Relate

### 1. HTTP Layer (`src/routes/`)

`research.ts` owns all job and queue endpoints. On `POST /research` it validates the CSV, calls `enqueueQueueItem()`, and kicks off `processUserQueue()` — a per-user loop that claims items from SQLite, creates an in-memory job, and runs `runResearchPipeline()`. The loop runs until the queue is empty, then exits; the next upload restarts it.

`admin.ts` exposes `POST /admin/adjust-weekly-counts`, protected by `x-admin-key` header. It reads current counts from SQLite and writes a delta row — no destructive updates.

### 2. Orchestration (`src/jobs/researchPipeline.ts`)

The core orchestrator. Reads one `CompanyRow` at a time from the CSV async generator and drives each company through all pipeline stages. Every 50 companies it flushes pending work via `Promise.allSettled` and writes a partial checkpoint to in-memory job state (partial CSV + `CampaignPushData`). See **Pipeline Stages** below.

### 3. State / Storage

Two separate stores with different lifetimes:

| Store | File | Backend | Lifetime | Used for |
|-------|------|---------|----------|----------|
| Job state | `src/jobs/jobStore.ts` | In-memory | 60-min TTL, max 20 jobs | Live progress polling |
| Queue + history | `src/services/queueStore.ts` | SQLite WAL | Survives restarts | Durable audit trail, per-user max-10 cap |
| Weekly counts | `src/services/weeklySuccessStore.ts` | SQLite WAL | Survives restarts | 100 LinkedIn push/week limit per user |

Both SQLite stores share `data/weekly-success.sqlite`. `weeklySuccessStore` tracks counts by appending rows with a `linkedin_delta` / `email_delta` — the current count is always a `SUM()` query, so the admin adjustment endpoint just inserts a negative-delta row.

### 4. Pipeline Stages

Each stage runs per-company inside `researchPipeline.ts`:

| Stage | Files | Reject condition |
|-------|-------|-----------------|
| CSV parse | `observability/csvReader.ts` | Missing Apollo Account ID (skipped rows surfaced in UI) |
| Apify scrape | `services/apifyCompanyEmployees.ts` | — |
| Candidate filter | `services/apifyClient.ts`, `services/computeTenure.ts` | open_to_work, frontend role, contract employment removed |
| LinkedIn selection | `services/sreSelection.ts` | — |
| Email selection | `services/emailCandidateWaterfall.ts` | — |
| Apollo email enrich | `services/apolloBulkEmailEnrichment.ts` | — |
| Lemlist push | `services/lemlistPushQueue.ts`, `services/lemlistEmailPushQueue.ts` | Weekly limit (100 LinkedIn/week per user) |
| CRM sync | `services/apolloBulkUpdateAccounts.ts`, `services/attioAssertCompanyRecords.ts` | — |
| PDF + finalize | `services/pdfReportGenerator.ts`, `jobStore.ts` | — |

### 5. Three-User Model (`src/shared/selectedUser.ts`)

Users are `raihan | cherry | julian`. Each user has full isolation across:
- **Queue**: separate rows in SQLite, max 10 active items enforced at enqueue time (`queueStore.ts:161`)
- **Lemlist campaigns**: 6 campaign IDs each (3 LinkedIn + 3 email), looked up by `RAIHAN_*` / `CHERRY_*` / `JULIAN_*` env var prefix in `lemlistClient.ts`
- **Weekly limits**: counts bucketed by `selected_user` column in SQLite; limit checked in `researchPipeline.ts` before each company's Lemlist push

### 6. External API Clients (`src/services/`)

| Service | Files | Auth | Retry |
|---------|-------|------|-------|
| Apollo.io | `apolloClient.ts`, `apolloBulkEmailEnrichment.ts`, `apolloBulkUpdateAccounts.ts` | `x-api-key` header | 2 retries on 429/5xx |
| Apify | `apifyClient.ts`, `apifyCompanyEmployees.ts` | Bearer token | — |
| Lemlist | `lemlistClient.ts`, `lemlistPushQueue.ts`, `lemlistEmailPushQueue.ts` | Basic auth | Rate limit: 20 req / 2s window |
| Attio | `attioClient.ts`, `attioAssertCompanyRecords.ts` | Bearer token | Retries on 429 (parses `Retry-After` header) |

### 7. Candidate Selection Logic

**LinkedIn (SRE ranking)** — `sreSelection.ts`:
- Tiers 1–4: Head/Director → Manager/Staff → Senior → Junior (≥ 2 months tenure)
- `fillToMinimumWithBackfill()` fills gaps with: past SRE experience → platform engineers → keyword-matched (incident, SLO, postmortem, etc.)

**Email (waterfall)** — `emailCandidateWaterfall.ts`:
- Stage 1: current SRE (leadership titles split into `engLead` bucket)
- Stage 2: past SRE experience (backfill if < 8 from stage 1)
- Stage 3: Infrastructure/DevOps
- Stage 4: general engineers
- Excludes data, frontend, contract across all stages

### 8. Apollo Custom Field IDs

`apolloBulkUpdateAccounts.ts` hardcodes Apollo custom field UUIDs and stage IDs (not fetched dynamically). If Apollo account custom fields or stages are changed in the Apollo UI, those IDs must be updated manually in that file.

### 9. Non-Obvious Conventions

- **`__resetQueueStoreForTests()`** (`queueStore.ts`) closes the SQLite connection for test isolation. Always called in test teardown.
- **`APOLLO_WATERFALL_ENABLED`** env var gates the Apollo waterfall enrichment path. Defaults `false`.
- **`LEMLIST_PUSH_ENABLED`** env var gates actual Lemlist pushes. Useful for dry runs.
- **CSV column names are configurable** via `NAME_COLUMN`, `DOMAIN_COLUMN`, `APOLLO_ACCOUNT_ID_COLUMN` env vars (`pipelineConfig.ts`). Defaults match the standard export format.
- **Checkpoint flush** happens every 50 companies via `Promise.allSettled` — failures in CRM sync don't abort the pipeline.
