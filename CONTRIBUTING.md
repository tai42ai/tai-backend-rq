# Contributing to tai42-backend-rq

`tai42-backend-rq` is an RQ **execution backend** for the TAI ecosystem: it
implements `tai42_contract.backend.Backend` — one strategy object that launches the
worker runtime (`worker` / `beat` / `dashboard`) and runs the background tool
executions and recurring schedules its workers pull from Redis. The hard rule
(the plugin rule): **it depends on `tai42-contract` + `tai42-kit` only and never
imports the skeleton.** Importing `tai42_backend_rq` registers everything through
the global `tai42_app` handle as a side-effect (the `RqBackend`, the `backend_*`
tools, and the `sync_task` / `async_task` / `schedule_task` extensions), and a
manifest's `backend_module` names the package. Fleet propagation of config
changes is not a backend concern: a backend-runtime process receives fleet ops
through the skeleton's own worker bus, exactly like a serving HTTP worker.

## Ground rules

- **No skeleton import — ever.** The package is contract-facing; the ban is
  enforced by ruff (`flake8-tidy-imports`), so a stray import fails lint:
  ```bash
  grep -rn "tai42_skeleton" src/   # must be empty
  ```
- **No control plane in the backend.** Fleet ops arrive over the app's worker
  bus; this backend ships no control plane of its own.
- **Loud errors.** No swallowed exceptions, silent fallbacks, or silent
  truncation. A failed task re-raises its stored failure; per-row schedule import
  errors are surfaced as `{"index", "name", "error"}`; capabilities RQ has no
  data model for (`backend_registered_tasks`, `backend_list_failed_tasks`) raise
  `NotImplementedError`.
- **Typed package** (`py.typed`). Pyright runs clean.

## Layout

- `backend.py` — `RqBackend` (the `Backend` impl; `launch` → `worker` / `beat` /
  `dashboard`) and its registration.
- `worker.py`, `liveness.py` — the worker runtime and its fork-safety / liveness
  hooks.
- `tasks.py`, `extensions.py`, `callback.py`, `signatures.py` — queued dispatch,
  the `sync_task` / `async_task` / `schedule_task` extensions, callback chaining,
  and dispatch signatures.
- `tools.py` — the `backend_*` tool surface.
- `schedules.py` — recurring schedules and the export/import round trip.
- `settings.py` — the `RQ_` settings.

## Naming

PyPI is a flat namespace with no owner in the path, so distributions carry the
`tai42-` prefix. GitHub repositories keep their `tai-` names, because the
`tai42ai` organisation already namespaces them. Import packages follow the
distribution.

| Surface | Form |
| --- | --- |
| Distribution — PyPI, `pip install`, dependency pins | `tai42-<name>` |
| Import package | `tai42_<name>` |
| GitHub repository | `tai-<name>` |

So a dependency is declared as `tai42-<name>` while its repository is named
`tai-<name>`, and both spellings are correct in their own context.

Some surfaces are deliberately neither, and must not be renamed: the `tai` CLI
command (`tai42` is an alias), the Prometheus metric namespace (`tai_tool_*`),
`TAI_*` environment variables, and the `tai-plugin.yml` descriptor filename.

## Dev

```bash
uv venv --python 3.13
uv pip install --no-sources --group dev --editable .
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync pyright
uv run --no-sync pytest --cov --cov-report=term-missing
```

`make dev` installs the sibling `tai-contract` and `tai-kit` repos as editable installs for local cross-repo development.

Before any commit, run a secret scan over `src/` and `tests/` (e.g.
`detect-secrets scan`).

## Dependency resolution

`uv.lock` pins the `tai42-*` siblings to their released index versions while `[tool.uv.sources]` points them at local `../tai-*` checkouts. The two disagree deliberately: CI sets `UV_NO_SOURCES=1` and asserts the lock with `uv sync --locked`, so it resolves the artifacts a user installs. A bare `uv lock` beside sibling checkouts re-couples the lock to editable path entries, which then fails that `--locked` check — run `uv lock --no-sources` instead. See [How dependencies resolve](https://tai42.ai/contributing#how-dependencies-resolve).

## License

By contributing you agree your contributions are licensed under Apache-2.0.
