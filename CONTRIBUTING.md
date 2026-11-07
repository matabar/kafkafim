# Contributing

## Branches

- `main` is always releasable. Nobody commits to it directly; every change arrives
  through a pull request from a short-lived branch.
- One branch, one concern. Name it `<type>/<short-kebab-description>`, where `type`
  is one of the commit types below: `feat/route-header-conditions`,
  `fix/offset-tracker-revoke`, `docs/runbook`, `ci/helm-unittest`.
- Rebase on `main` before opening or updating a pull request. Merges into `main`
  are **rebase merges** so each commit lands as written; squash only when asked.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <imperative summary, ≤ 72 chars>

<why, not what; wrap at 72>
```

- Types: `feat`, `fix`, `docs`, `build`, `ci`, `test`, `refactor`, `perf`, `chore`.
- Scopes: `kafka`, `es`, `config`, `route`, `decode`, `dlq`, `supervisor`,
  `telemetry`, `helm`, `docker`, `ci`, `deps`, `style`, `docs`. Omit when global.
- One commit, one responsibility. A commit builds and passes `./mvnw verify` on its
  own. Formatting-only changes are their own commit (`style:`).
- Breaking changes carry `!` after the type and a `BREAKING CHANGE:` footer.

## Before you push

```bash
./mvnw spotless:apply
```

```bash
./mvnw verify
```

`verify` runs Google Java Style checks (Spotless + Checkstyle), unit tests,
Testcontainers integration tests and ArchUnit rules. CI runs the same, plus the
Dockerfile build and Helm checks; all must pass before merge.

## Pull requests

- Title in Conventional Commit form; body says why and how it was verified.
- Keep them small enough to review in one sitting. If a change needs two concerns,
  open two pull requests.
- Design changes come with an ADR in `docs/decisions/` in the same pull request.
