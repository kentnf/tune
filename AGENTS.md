# Repository Guidelines

## Project Structure & Module Organization

`tune/` contains the Python application: `api/` for FastAPI routes and WebSocket endpoints, `core/` for planning, binding, memory, orchestration, runtime, and resource logic, `workers/` for background jobs, and `schemas/` for API payloads.  
`tests/` holds backend pytest coverage.  
`frontend/src/` contains the Vite/React UI, with `components/`, `hooks/`, `i18n/`, and `test/`.  
`migrations/versions/` stores Alembic revisions.  
`scripts/` includes local service helpers.  
`analysis/` is used for design notes and checkpoints, not runtime code.

## Build, Test, and Development Commands

- `pip install -e .` installs the backend in editable mode.
- `tune init` creates workspace config.
- `tune start --workspace-root workspace` starts the app on `localhost:8000`.
- `bash scripts/dev.sh --workspace-root workspace` runs the local dev stack with reload.
- `pytest -q` runs the backend fast suite.
- `pytest tests/test_job_bindings_api.py -q` is useful for supervisor/control-plane changes.
- `cd frontend && npm install` installs UI dependencies.
- `cd frontend && npm run dev` starts the frontend dev server.
- `cd frontend && npm test -- src/components/TaskMonitor.test.tsx` runs a focused Vitest file.
- `cd frontend && npm run build` type-checks and builds the UI.

## Coding Style & Naming Conventions

Target Python is 3.11+. Ruff is configured with a `100` character line limit in [pyproject.toml](/Users/kentnf/projects/tune/pyproject.toml). Use `snake_case` for Python functions/modules and `PascalCase` for React component files such as `TaskMonitor.tsx`. Keep helpers close to the route or subsystem they support. Prefer small, composable functions over broad controller logic.

## Testing Guidelines

Backend tests use `pytest`; frontend tests use `vitest` with Testing Library. Name backend tests `test_*` and colocate frontend specs as `*.test.tsx`. Add or update focused tests with every behavior change, especially for supervisor, orchestration, and UI task-monitor flows. Mark only slower integration checks with `@pytest.mark.slow`.

## Commit & Pull Request Guidelines

Recent history follows Conventional Commit style: `feat: ...`, `fix: ...`, `refactor: ...`. Keep commits scoped and descriptive. PRs should include a short summary, affected areas, test commands run, migration notes if `migrations/` changed, and screenshots for visible frontend changes.

## Security & Configuration Tips

Do not commit API keys or workspace data. Keep runtime config under `workspace/.tune/config.yaml`. Treat `workspace/data/` as input and write outputs under `workspace/analysis/`. When changing models, memory, or orchestration behavior, document the rationale in `analysis/`.
