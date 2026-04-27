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
  observability/ — CSV I/O (csvReader.ts, csvWriter.ts)
src/shared/      — SelectedUser type
src/types/       — domain types (prospect.ts)
frontend/src/    — Vue 3 SPA
tests/           — Vitest, one file per src file
data/            — SQLite databases (gitignored)
```

For full architecture details, see @docs/architecture.md

## Documentation

- **Always use context7** before building or modifying anything that involves a library, framework, or external API. Resolve the library ID first (`mcp__context7__resolve-library-id`), then fetch the relevant docs (`mcp__context7__query-docs`). This applies to Express, Vue, Vitest, Apify, Apollo, Attio, Lemlist, SQLite, and any other dependency in this project.
- Do not rely on training data for API shapes, SDK methods, or config options — fetch current docs via context7 every time.

## Pre-Commit Lint Check

Always run `npm run lint` before creating any git commit and fix all errors first. Do not commit with lint errors present.

## Testing Rule

Whenever you build or change something:

1. **New behavior → write new tests.** If the change introduces behavior that isn't covered by an existing test, add tests for it in the matching `tests/<file>.test.ts`.
2. **Changed behavior → update existing tests.** If the change alters the shape or semantics of something already covered, update those tests to match the new expectation.
3. **Always run the full suite at the end.** Run `npm test` after the change and fix anything that breaks. Do not leave the task until the suite is green, except for pre-existing failures on `main` that are unrelated to your change — in that case, call them out explicitly.
4. **Test mocks must follow new exports.** If you add a new named export to a module, any `vi.mock(...)` of that module in tests must also export it — otherwise consumers will get `undefined` at runtime and fail in surprising ways.

## Critical Constraints

- **Never commit `.env`** — contains all API keys and campaign IDs.
- **Apollo custom field and stage IDs are hardcoded** in `src/services/apolloBulkUpdateAccounts.ts`. If Apollo fields/stages change in the UI, update those IDs manually.
- **`LEMLIST_PUSH_ENABLED`** env var must be `true` for Lemlist pushes to actually fire. Default is off — dry runs are the default.
- **Weekly LinkedIn push limit is 100 per user**, hard-coded in `researchPipeline.ts`. Exceeding it skips remaining companies silently.
- **`__resetQueueStoreForTests()`** must be called in test teardown for any test that touches `queueStore` — it closes the SQLite connection so the next test gets a clean DB.

## Non-Obvious Defaults

- `APOLLO_WATERFALL_ENABLED` defaults `false` — Apollo waterfall enrichment is off unless explicitly set.
- CSV column names default to `"Company Name"`, `"Website"`, `"Apollo Account Id"` but are overridable via `NAME_COLUMN`, `DOMAIN_COLUMN`, `APOLLO_ACCOUNT_ID_COLUMN` env vars.
