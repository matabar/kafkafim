# ADR-0024: Google Java Style, enforced by Spotless and Checkstyle

Status: accepted · Date: 2026-11-07

## Decision
All Java code follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

- **Formatting** is mechanical: `google-java-format` through the Spotless Maven
  plugin. `./mvnw spotless:apply` rewrites; `spotless:check` runs in the `validate`
  phase, so an unformatted file fails the build before compilation, locally and in CI.
  Imports are ordered and unused imports removed by the same step.
- **Conventions** that a formatter cannot fix (naming, Javadoc on public types,
  no wildcard imports, modifier order, empty blocks, overload grouping) are checked by
  Checkstyle with the `google_checks.xml` that ships inside Checkstyle; its checks
  report at `warning` severity and the plugin fails the build on warnings
  (`violationSeverity=warning`, `failOnViolation=true`), in the `validate` phase.
- `.editorconfig` mirrors the basics (2-space indent, 100 columns, LF) for IDEs;
  the google-java-format IntelliJ and VS Code plugins apply the exact formatter.
- Generated code and test resources are excluded; test code is held to the same
  style.

## Alternatives
Spring Java Format (the Spring team's own style): coherent, but the team asked for
Google style. Checkstyle alone: cannot auto-fix, so every PR would argue about
whitespace. Formatter alone: misses naming and Javadoc rules.

## Consequences
One extra second per build. Diffs contain no formatting noise. The first commit is
already formatted.
