# Contributing

## Branches

- `main` is always releasable. Nobody commits to it directly; every change arrives
  through a pull request from a short-lived branch.
- One branch, one concern. Name it `<type>/<short-kebab-description>`, where `type`
  is one of the commit types below: `feat/route-header-conditions`,
  `fix/offset-tracker-revoke`, `docs/runbook`, `ci/helm-unittest`.
- Rebase on `main` before opening or updating a pull request. Merges into `main`
  are **squash merges** only: a pull request becomes one commit whose subject is
  the PR title and whose body is the PR description, so both are written in
  Conventional Commit form. Commits inside the branch stay atomic for review.

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
  own; a commit that changes only documentation (`*.md` outside `src/`, `docs/**`, `LICENSE`) passes
  `./mvnw -DskipTests verify`, matching CI, which skips the Java and image jobs for
  docs-only pull requests (docs/DELIVERY.md). Formatting-only changes are their own
  commit (`style:`).
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
Dockerfile build and Helm checks; all must pass before merge. A docs-only change
needs only `./mvnw -DskipTests verify`; CI skips the Java and image jobs for it.

## Pull requests

- Title in Conventional Commit form; body says why and how it was verified. They
  become the squash commit on `main`, so write them for the git log, not for chat.
- Every pull request gets an independent review before merge (a reviewer agent or a
  teammate). Findings are fixed on the branch; approval plus green CI merges it and
  the branch is deleted.
- Keep them small enough to review in one sitting. If a change needs two concerns,
  open two pull requests.
- Design changes come with an ADR in `docs/decisions/` in the same pull request.
