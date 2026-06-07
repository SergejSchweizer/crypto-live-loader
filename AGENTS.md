## Purpose

This repository provides a reusable AGENTS.md baseline for integration into other repositories.

## Scope

Applies to all agent-assisted implementation, refactoring, review, testing, and documentation work.

## Rules

- [MUST] Optimize for maintainability, modularity, reproducibility, testability, documentation quality, and extensibility.
- [MUST] Keep behavior understandable without tribal knowledge.
- [MUST] Prefer explicit contracts, deterministic behavior, and clear ownership of side effects.
- [SHOULD] Favor simple, composable designs over clever abstractions.
- [SHOULD] Preserve backward compatibility unless a breaking change is intentional and documented.

## Agent Action Checklist

- Confirm task scope and expected behavior.
- Identify affected contracts, tests, and docs before editing.
- Apply smallest safe change first.
- Validate behavior and update docs in the same change set.

## Definition of Done

- Change is correct, testable, and understandable.
- Contracts and behavior are explicit.
- Relevant docs and tests are aligned.

## Verification Commands

- `ruff check .`
- `ruff format --check .`
- `pytest -q`

## Exceptions and Escalation

- Ask for confirmation before intentional breaking changes.
- Escalate when requirements conflict or risk data correctness.

## Core Rules

## Scope

Always active across all modules and workflows.

## Rules

- [MUST] Prefer the smallest safe change that fully resolves the issue.
- [MUST] Preserve backward compatibility by default.
- [MUST] Keep business logic separate from framework/storage details.
- [MUST] Isolate side effects behind explicit interfaces and adapters.
- [MUST] Keep execution deterministic where feasible.
- [MUST] Keep operational docs aligned with behavior changes.
- [MUST] Use one shared log root path defined in `config.yaml`, and it must point to the `.logs` directory.
- [MUST] Every module writes to its own logfile under the shared `.logs` directory.
- [MUST] Use one consistent log message structure across all modules.
- [MUST] Do not comment obvious code.
- [SHOULD] Add comments for non-obvious decisions, invariants, and tradeoffs.
- [MUST] Comments and docstrings explain non-obvious decisions, invariants, edge cases, tradeoffs, external system assumptions, and failure handling.
- [MUST] Add inline comments for important non-obvious data logic, including forward-fill, interpolation, resampling, timestamp normalization, timezone handling, rolling windows, and numerical stability safeguards.
- [MUST] For market-data and derivatives workflows, inline comments must document funding normalization, open-interest reconstruction, option-surface reconstruction, feature engineering decisions, leakage prevention, and exchange-specific behavior.
- [SHOULD] Avoid comments that only restate obvious code.
- [MUST] Enforce deny-by-default `.gitignore` patterns, with minimal explicit allowlist.

## Agent Action Checklist

- Before edit: identify contract boundaries and side effects.
- During edit: keep module responsibilities cohesive.
- After edit: confirm logging path and format consistency plus docs alignment.

## Definition of Done

- Boundaries remain explicit.
- Logging is centralized and consistent.
- Documentation reflects behavior.

## Verification Commands

- `rg -n "logfile|logging|config.yaml|\\.logs" .`
- `ruff check .`
- `pytest -q`

## Exceptions and Escalation

- Escalate if a required change introduces unavoidable compatibility break.

## Architecture

## Scope

Applies to system design, module boundaries, refactors, scalability, reliability, and technical tradeoffs.

## Rules

- [SHOULD] Enforce architecture rules with automated tests.
- [MUST] Required architecture checks include forbidden dependency directions.
- [MUST] Required architecture checks include circular imports.
- [MUST] Required architecture checks include infrastructure leaking into domain logic.
- [MUST] Required architecture checks include presentation or API layers importing persistence internals.
- [MUST] Required architecture checks include shared utilities becoming dependency-heavy.
- [SHOULD] Use `import-linter` or dedicated architecture tests to enforce architecture constraints.
- [MUST] Define contract shape first (types, schema, invariants), then implement.
- [MUST] Keep dependency direction from policy and domain to implementation details.
- [MUST] Keep ownership explicit for each module (inputs, outputs, side effects).
- [MUST] Keep operations idempotent and restart-safe by default.
- [MUST] Use bounded, configurable concurrency.
- [MUST] Keep schema changes backward compatible unless versioned intentionally.
- [SHOULD] Prefer incremental and delta processing over full rescans.
- [SHOULD] Prefer composable functions before introducing pattern-heavy class hierarchies.
- [MAY] Use Strategy, Template Method, Factory, and Repository patterns when they reduce duplication and improve extensibility.
- [SHOULD] Prefer `polars` over `pandas` when ecosystem constraints allow.

## Agent Action Checklist

- Identify architecture impact level (none, local, cross-module).
- If contract changes: define compatibility and migration plan.
- If refactor: preserve behavior and add regression coverage.
- If scalability-sensitive: validate idempotency, ordering, and memory and concurrency bounds.

## Definition of Done

- Module boundaries are explicit.
- Contracts are typed and validated.
- Scalability and reliability implications are addressed.
- Regression tests cover changed behavior.

## Verification Commands

- `pytest -q`
- `ruff check .`
- `mypy .` or `pyright`

## Exceptions and Escalation

- Escalate before large boundary shifts or contract versioning decisions.

## Code Quality Gates

## Scope

Applies to review readiness, PR preparation, and pre-merge quality-gate validation.

## Rules

- [MUST] All repositories using this baseline enforce quality gates through pre-commit and CI.
- [MUST] Pre-commit and CI enforce the same logical checks.
- [SHOULD] A change that passes locally also passes in CI without additional manual steps.
- [MUST] CI is the final authority for merge readiness.
- [MUST] Prioritize correctness and regression risk over style.
- [MUST] Validate contract and schema integrity and boundary discipline.
- [MUST] Flag operational risk (idempotency, restartability, observability).
- [MUST] Require tests for risk-heavy behavior changes.
- [MUST] Use explicit typing and return types on public interfaces.
- [SHOULD] Require docstrings for non-trivial modules and functions.
- [MUST] Run lint, format, typing, tests, and coverage checks before merge when practical.
- [MUST] Required quality-gate checks include Ruff linting, Ruff formatting check, Pyright strict type checking, Pytest, coverage threshold, docstring checks, import boundary checks, and architecture tests.
- [MUST] Required documentation checks include `interrogate` docstring coverage and `pydoclint` signature-docstring consistency validation.
- [MUST] Run quality tools in strict mode where available, and fail the build on any reported error.
- [MUST] Do not use permissive flags or downgraded severity levels that hide lint, type, formatting, or test failures.
- [MUST] Agents must not bypass checks with `--no-verify` unless explicitly instructed by the human maintainer.
- [MUST] `.pre-commit-config.yaml` includes and maintains hooks for `ruff`, `interrogate`, `pydoclint`, `pyright`, and `pytest`.

## Review Findings Format

- Severity: `High` | `Medium` | `Low`
- Location: `path:line`
- Risk: what can break
- Recommendation: concrete fix

## Anti-Patterns To Flag

- [MUST] Silent fallback masking broken state.
- [MUST] Broad exception handling without context or re-raise strategy.
- [MUST] Hidden side effects across module boundaries.
- [MUST] Untyped public interfaces.
- [MUST] Contract changes without migration notes.

## Agent Action Checklist

- Read intended behavior and scope first.
- Validate happy path and failure paths.
- Verify tests for changed risk areas.
- Report findings ordered by severity.

## Definition of Done

- Findings are actionable and severity-ranked.
- Risks and missing tests are explicit.
- Documentation, config, and schema impacts are called out.

## Verification Commands

- `ruff check .`
- `ruff format --check .`
- `interrogate .`
- `pydoclint src`
- `pyright --level error`
- `pytest -q`

## Testing

## Scope

Applies when adding or changing tests, fixing bugs, refactoring behavior, adding CLI commands, or validating release readiness.

## Rules

- [MUST] Run targeted tests for changed areas.
- [SHOULD] Run full test suite before finalization when practical.
- [MUST] Disclose checks that could not run and why.
- [MUST] Add regression tests for every bug fix.
- [MUST] Test happy path, edge cases, and failure paths.
- [MUST] Keep tests deterministic.
- [MUST] Run `coverage run -m pytest` and `coverage report` for release-ready validation when practical.

## Coverage Policy

- [MUST] Target repository coverage is 90%.
- [MUST] Preserve or improve coverage for meaningful changes.
- [MUST] If coverage is below 90%, disclose the gap and follow-up work.

## CLI Validation

- [MUST] Every new or modified CLI command has dedicated automated tests.
- [MUST] CLI commands run autonomously as standalone invocations.
- [MUST] Every CLI exposes a `--debug` flag for extensive logging.
- [MUST] Treat logs as a primary debug source for CLI diagnosis.
- [MUST] When debugging, run CLI commands with `--debug` where available and or add targeted log messages.
- [MUST] While a script is running, actively analyze logfile output.

## Agent Action Checklist

- Reproduce with deterministic inputs.
- Execute CLI with `--debug` during diagnosis.
- Analyze logfile output while process runs.
- Add or refine logs only where they improve failure isolation.
- Add or adjust tests before finalizing the fix.
- Run the documented pre-commit command sequence before finalizing: Ruff, `interrogate`, `pydoclint`, type checks, tests, and coverage.

## Definition of Done

- Bug and feature behavior is covered by tests.
- Debug path is observable from logs.
- Coverage impact is reported.

## Verification Commands

- `pytest -q`
- `pytest --maxfail=1 -q`
- `pytest --cov --cov-report=term-missing`
- `coverage run -m pytest`
- `coverage report`

## Exceptions and Escalation

- Escalate if deterministic reproduction is not possible without production-only dependencies.

## Python Tooling

## Scope

Applies to Python quality tooling, typing, formatting, and local validation commands.

## Rules

- [MUST] Python code is fully typed.
- [MUST] Public functions have explicit parameter and return types.
- [MUST] Implicit `Any` is not allowed.
- [MUST] Untyped public APIs are not allowed.
- [MUST] Every `# type: ignore` includes a precise explanation.
- [MUST] Runtime data crossing boundaries uses typed DTOs, dataclasses, Pydantic models, TypedDicts, or explicit schemas.
- [MUST] Prefer making invalid states unrepresentable.
- [MUST] Configure Python tooling primarily in `pyproject.toml`.
- [SHOULD] Configure `ruff`, `pyright`, `pytest`, `coverage`, and docstring tooling via `pyproject.toml` when supported.
- [SHOULD] Avoid scattered configuration files unless a tool does not support `pyproject.toml`.
- [MUST] Keep code compatible with the configured formatter, linter, type checker, and test runner.
- [MUST] Pyright and other configured Python quality tools must run in strict mode where supported.
- [MUST] Do not relax tool strictness or suppress failures globally to make checks pass.
- [MUST] Use type hints consistently, including explicit return types for public interfaces.
- [MUST] Public modules, public classes, public functions, CLIs, and architectural boundaries have concise docstrings.
- [MUST] Every public function, class, and method uses Google-style docstrings.
- [MUST] Function docstrings document: what and why, parameters, returns, raised exceptions, assumptions, side effects, and data semantics.
- [MUST] When applicable, function docstrings also document time-alignment assumptions and exchange-specific quirks.
- [MUST] Enforce docstring coverage with `interrogate`.
- [MUST] Enforce docstring/signature consistency with `pydoclint`.
- [MUST] Keep import boundaries compatible with repository rules when boundary tooling is configured.
- [SHOULD] Prefer one canonical command sequence for local validation to reduce drift across contributors.
- [MUST] `pyproject.toml` includes and maintains Ruff pydocstyle configuration with Google convention.
- [MUST] `pyproject.toml` includes and maintains `interrogate` with `fail-under = 95`.
- [MUST] `pyproject.toml` includes and maintains `pydoclint` with Google style and return-type checks.
- [MUST] `pyproject.toml` includes and maintains coverage report threshold with `fail_under = 90`.
- [MUST] `pyproject.toml` keeps `line-length = 100` and `target-version = "py312"` for Ruff unless intentionally changed and documented.

## Agent Action Checklist

- Run lint and format checks before finalizing changes.
- Run docstring quality checks (`interrogate` and `pydoclint`) before finalizing changes.
- Run type checks for modified modules.
- Run targeted tests first, then broader tests when practical.
- Report any tool that could not be run and why.

## Definition of Done

- Lint, format, typing, and test signals are green or explicitly documented.
- Public interfaces stay typed and understandable.

## Verification Commands

- `ruff check .`
- `ruff format --check .`
- `interrogate .`
- `pydoclint src`
- `mypy .` or `pyright`
- `pytest -q`

## Agent Workflow

## Scope

Applies to day-to-day agent execution flow for implementation, debugging, and delivery.

## Rules

- [MUST] Before changing code, inspect relevant files.
- [MUST] Before changing code, identify the smallest safe change.
- [MUST] Never commit directly to `main`.
- [MUST] Always create a short-lived, task-specific feature branch from latest `main` using `codex/<scope>-<short-description>`.
- [MUST] Use lowercase letters, numbers, and hyphens only in branch names.
- [MUST] Do not use vague branch names such as `codex/fixes`, `codex/update`, `codex/big-change`, `codex/refactor-all`, or `codex/work`.
- [MUST] Keep one branch to one logical change.
- [MUST] Before starting a task, run `git status`, `git branch --show-current`, `git fetch origin`, `git checkout main`, and `git pull --ff-only origin main`.
- [MUST] If the working tree is not clean before starting, stop and report changed files.
- [MUST] Do not overwrite, delete, stash, reset, or otherwise discard user changes unless explicitly instructed.
- [MUST] Before committing, run `ruff check .`, `pyright`, `pytest`, and `coverage run -m pytest`.
- [MUST] If configured, also run `pre-commit run --all-files` and include repository-specific typing or import boundary checks.
- [MUST] If a required check fails, fix it before commit or clearly report why it is unrelated and safe to defer.
- [MUST] Before committing, inspect `git diff` and `git status` and ensure only task-relevant changes are included.
- [MUST] Use concise imperative commit messages.
- [MUST] Push the feature branch and open a pull request into `main`.
- [MUST] Never self-merge a pull request unless explicitly instructed.
- [SHOULD] Prefer squash merge and delete the feature branch after merge.
- [MUST] If rebasing requires history rewrite, only use `git push --force-with-lease`, never plain `git push --force`.
- [MUST] After merge, sync with `git checkout main` and `git pull --ff-only origin main`.
- [MUST] Do not weaken tests to make them pass.
- [MUST] Do not remove type hints.
- [MUST] Do not introduce hidden network calls.
- [MUST] Never commit secrets, credentials, tokens, `.env` files, or private paths.
- [MUST] Do not commit generated local artifacts unless explicitly required by the task.
- [MUST] Keep architecture boundaries explicit.
- [SHOULD] Prefer small, reviewable commits.
- [MUST] Preserve existing public contracts unless explicitly asked to change them.
- [MUST] Add or update tests for behavioral changes.
- [MUST] Run relevant quality gates.
- [MUST] Report any checks that could not be executed.
- [MUST] Do not introduce large rewrites when a targeted change is sufficient.
- [MUST] Understand intended behavior and scope before editing.
- [MUST] Prefer the smallest safe change that resolves the issue.
- [MUST] Keep behavior stable during refactors unless a change is intentional and documented.
- [MUST] Update tests and documentation in the same change set for behavior changes.
- [MUST] During debugging, run CLI commands with `--debug` where available and analyze logfile output while scripts run.
- [SHOULD] Add targeted diagnostic logs when they improve failure isolation.
- [MUST] Do not run destructive commands without explicit instruction, including `git reset --hard`, `git clean -fd`, `git clean -fdx`, `git checkout -- .`, `git restore .`, `git stash`, `git push --force`, and `rm -rf`.

## Pull Request Body Template

Remove checks that do not exist in the repository.

```markdown
## Summary

- Describe what changed.
- Describe why it changed.

## Dataset / Pipeline Impact

- State affected datasets, layers, or commands.
- State whether Bronze, Silver, or Gold behavior changed.

## Validation

- [ ] ruff check .
- [ ] ruff format .
- [ ] mypy .
- [ ] pyright
- [ ] ty check
- [ ] lint-imports --config .importlinter
- [ ] pytest
- [ ] pytest --cov
- [ ] pre-commit run --all-files

## Risk

- Low / Medium / High
- Explain possible breakage or migration concerns.

## Notes

- Mention follow-up work.
- Mention known limitations.
```

## Agent Action Checklist

- Reproduce issue with deterministic inputs.
- Verify clean working tree before branch creation.
- Create focused branch from latest `main`.
- Identify impacted contracts, side effects, and test scope.
- Implement minimal fix or focused improvement.
- Inspect diff to keep only task-related files.
- Push branch and open PR targeting `main`.
- Validate with quality gates and tests.
- Do not merge without explicit instruction.
- Summarize risks, residual gaps, and follow-up work.

## Definition of Done

- Requested change is implemented and validated.
- Work enters `main` only through a pull request from a focused feature branch.
- Debug and failure paths are observable.
- Docs and tests match the updated behavior.

## Verification Commands

- `pytest -q`
- `ruff check .`
- `ruff format --check .`
- `git status`
- `git branch --show-current`
- `git fetch origin`
- `git checkout main`
- `git pull --ff-only origin main`
- `git checkout -b codex/<task-name>`
- `git diff`
- `gh pr create --base main --head codex/<task-name> --fill`
- `gh pr checks`

## Security

## Scope

Applies to configuration, credentials, secrets handling, runtime environment, external inputs, and sensitive data paths.

## Rules

- [MUST] Never commit secrets or credentials.
- [MUST] Keep sensitive values out of code, docs, and artifacts.
- [MUST] Document required runtime variables in canonical configuration.
- [MUST] Use one canonical runtime configuration source per repository.
- [MUST] Validate and sanitize external inputs at trust boundaries.
- [MUST] Prefer explicit allowlists over implicit trust.
- [MUST] Bound third-party calls with timeout, retry, and input validation.
- [SHOULD] Apply least privilege for runtime identities and permissions.
- [SHOULD] Treat logs, metrics, and traces as potential exfiltration paths.

## Agent Action Checklist

- Check for secrets exposure in code, docs, and logs.
- Verify config contract changes are documented in the same change set.
- Validate error messages are actionable without leaking sensitive data.
- Confirm external integrations are bounded and observable.

## Definition of Done

- No secrets exposed.
- Runtime and config contract is explicit and validated.
- Security-impacting changes include safeguards and docs updates.

## Security Checklist

- Secrets excluded from repo and docs.
- Config contract explicit.
- Access scopes minimized.
- Error handling safe and actionable.
- Third-party boundaries enforced.

## Release and Sync

## Scope

Applies to pre-commit synchronization, release readiness, and repository-wide instruction consistency.

## Rules

- [MUST] `AGENTS.md` is generated from `fragments/*.md`.
- [MUST] Agents must not edit `AGENTS.md` directly.
- [MUST] All durable instruction changes must be made in the corresponding fragment file.
- [MUST] After modifying fragments, regenerate `AGENTS.md` and verify generated output is deterministic.
- [MUST] Keep `AGENTS.md` synchronized with fragment source files.
- [MUST] Keep pre-commit sync behavior non-blocking when network access is unavailable.
- [MUST] Keep generated repository instructions deterministic and reproducible.
- [SHOULD] Keep release scope focused and include rollback or mitigation notes for operational risk.
- [MUST] Disclose skipped quality checks and unresolved risks before release.

## Agent Action Checklist

- Confirm fragments are the source of truth.
- Never edit `AGENTS.md` directly for durable policy updates.
- Regenerate or sync `AGENTS.md` after fragment updates.
- Verify determinism by re-running generation and confirming no diff.
- Verify pre-commit sync hook still points to the managed sync script.
- Validate release notes include testing status and known gaps.

## Definition of Done

- `AGENTS.md` matches current fragment set.
- Sync path is operational and documented.
- Release state includes quality-gate and risk visibility.

## Verification Commands

- `python scripts/sync_agents.py`
- `git diff -- AGENTS.md fragments`
- `pytest -q`
