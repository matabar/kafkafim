# ADR-0045: A secret file whose content contains `${` is rejected

Status: accepted · Date: 2026-11-14 · Refines [ADR-0012](0012-spring-configuration-helm-configtree-hot-reload.md) (DESIGN §5.5)

## Context
The inline-secret check (DESIGN §5.5) accepts a credential as soon as its value comes
from a `configtree` file, and it does not look at the file's content. The `Binder`
still resolves placeholders in that content, against every property source. A secret
file containing `${my.lit}` therefore bound the literal `my.lit` from YAML, and the
check accepted it: a literal reached the credential through a mounted file (issue
#201). The same mechanism mangles a real secret that happens to contain `${...}`, or
leaves the placeholder text as the password when nothing defines it.

## Decision
- A credential (secret or username) read from a `configtree` file, directly or at the end of a
  `${name}` chain, whose content contains `${` fails startup (and a reload, and the
  `validate` profile). The error names the property and the file's origin, never the
  content.
- This holds also with `kafkafim.allow-inline-secrets=true`: that switch allows
  literals, but a placeholder in a secret file is not a literal that stays as written,
  it is substituted.
- A secret that must contain `${` cannot be mounted as a file; there is no escape,
  because the binder's escape handling is not something kafkafim can rely on across
  the sources that resolve the file.

## Alternatives
- Follow the placeholder in the file like any other and apply the literal rule to
  where it ends. Rejected: it keeps substitution inside secrets, which no one wants,
  and still mangles a secret containing `${`.
- Document an escape for secrets containing `${`. Rejected: such secrets are rare,
  and a clear startup error is simpler and safe.

## Consequences
- The one rare secret containing `${` needs another shape (a different value).
- Only startup validation changes; the bound values and the secrets listing of the
  `validate` profile are as before.
