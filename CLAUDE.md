# CLAUDE.md

## Commands

```bash
npm run dev          # backend + frontend (concurrent)
npm run dev:backend  # backend only (tsx watch)
npm run dev:frontend # frontend only (Vite, port 5173)
npm run build        # TypeScript + frontend production build
npm start            # production server
npm test             # all tests
npm run test:watch   # watch mode
npx vitest run tests/<file>.test.ts  # single file
npm run lint
npm run format
```

## Directory Map

```
src/config/      — env loading and pipeline config
src/jobs/        — in-memory job state + main orchestration (researchPipeline.ts)
src/routes/      — Express routes (research.ts, admin.ts)
src/services/    — all external API clients and business logic
  observability/ — CSV I/O, Azure OpenAI research, SearchAPI
src/shared/      — SelectedUser type
src/types/       — domain types (prospect.ts)
frontend/src/    — Vue 3 SPA
tests/           — Vitest, one file per src file
data/            — SQLite databases (gitignored)
```

For full architecture details, see @docs/architecture.md

## Documentation

- **Always use context7** before building or modifying anything that involves a library, framework, or external API. Resolve the library ID first (`mcp__context7__resolve-library-id`), then fetch the relevant docs (`mcp__context7__query-docs`). This applies to Express, Vue, Vitest, Apify, Apollo, Attio, Lemlist, Azure OpenAI, SQLite, and any other dependency in this project.
- Do not rely on training data for API shapes, SDK methods, or config options — fetch current docs via context7 every time.

## Critical Constraints

- **Never commit `.env`** — contains all API keys and campaign IDs.
- **Apollo custom field and stage IDs are hardcoded** in `src/services/apolloBulkUpdateAccounts.ts`. If Apollo fields/stages change in the UI, update those IDs manually.
- **`LEMLIST_PUSH_ENABLED`** env var must be `true` for Lemlist pushes to actually fire. Default is off — dry runs are the default.
- **Weekly LinkedIn push limit is 100 per user**, hard-coded in `researchPipeline.ts`. Exceeding it skips remaining companies silently.
- **`__resetQueueStoreForTests()`** must be called in test teardown for any test that touches `queueStore` — it closes the SQLite connection so the next test gets a clean DB.

## Non-Obvious Defaults

- `APOLLO_WATERFALL_ENABLED` defaults `false` — Apollo waterfall enrichment is off unless explicitly set.
- CSV column names default to `"Company Name"`, `"Website"`, `"Apollo Account Id"` but are overridable via `NAME_COLUMN`, `DOMAIN_COLUMN`, `APOLLO_ACCOUNT_ID_COLUMN` env vars.
- Companies are rejected (not just skipped) if their observability tool is not Datadog, Grafana, or Prometheus — this is intentional targeting logic in `observability/openaiClient.ts`.
