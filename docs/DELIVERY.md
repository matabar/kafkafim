# Delivery: CI, CD, registries, air gap

Development happens on the internet (GitHub). Production is air-gapped: nothing is
deployed from CI. CI proves the application works; CD publishes a versioned image and
chart that the team transfers into the air-gapped environment and deploys there.

## Principles

1. **The jar that passed tests is the jar in the image.** CI builds once, tests, then
   the Dockerfile only wraps `target/*.jar`. No second compile inside Docker.
2. **Everything that leaves CI is versioned, signed and self-describing.** Image,
   chart, SBOM and checksums share one semver, derived from a git tag.
3. **Nothing in the image or chart reaches the internet at runtime.** No init
   containers, no subcharts, no downloads. The chart's image reference is fully
   overridable for the internal registry.
4. **Publishing is cheap and GitHub-native.** GitHub Container Registry (GHCR) for
   both images and OCI Helm charts, authenticated with the workflow token.

## Versioning

- Semantic versions, driven by git tags `vMAJOR.MINOR.PATCH`. No version-bump
  commits: the pom uses the CI-friendly `${revision}` property (default
  `0.1.0-SNAPSHOT`) and the release workflow passes `-Drevision=1.2.3`.
- Chart `version` and `appVersion` are both the tag, so one number identifies the
  whole deliverable when it is carried into the air gap.
- Image tags per release: `1.2.3`, `1.2`, `sha-<7>`. Pushes to `main` publish
  `main` and `sha-<7>` only (development environments). `latest` is never published.

## Registries and names

| Artifact | Location |
|---|---|
| Container image | `ghcr.io/matabar/kafkafim` |
| Helm chart (OCI) | `oci://ghcr.io/matabar/charts/kafkafim` |
| Release bundle for the air gap | GitHub Release assets on the tag |

Alternatives considered: Docker Hub (anonymous pull rate limits hurt air-gap
transfer scripts), a GitHub Pages `index.yaml` Helm repo (legacy; OCI is the current
Helm distribution model and is what `helm pull`/`helm push` and `skopeo` understand).

## CI (`.github/workflows/ci.yml`) on pull requests and `main`

| Job | What it proves |
|---|---|
| `build` | `./mvnw verify`: compile, unit tests (Surefire, `*Test`/`*Tests`), integration tests (Failsafe, `*IntegrationTest`, Testcontainers Kafka + Elasticsearch), ArchUnit package-boundary tests. Uploads test reports and the jar. |
| `image` | The Dockerfile builds from the tested jar (no push); `hadolint` on the Dockerfile; the container starts read-only and answers `/actuator/health/liveness` within 60 s. |
| `helm` | `helm lint`, `helm template` with every `charts/kafkafim/ci/*-values.yaml`, `kubeconform` against Kubernetes schemas, `helm unittest` for the group-rendering logic. Skipped until the chart exists. |

Branch protection on `main`: PR required, all three jobs required, squash merge.
Dependabot (weekly, grouped) for Maven, GitHub Actions and the Docker base image.

## CD (`.github/workflows/release.yml`) on tag `v*`

1. `./mvnw -Drevision=<ver> verify` (tests run again on the exact tagged commit).
2. Multi-arch image (`linux/amd64`, `linux/arm64`) built with Buildx and pushed to
   GHCR with the tags above and BuildKit provenance attestations.
3. SBOM (SPDX JSON, via Syft) generated from the image and attached to the release.
4. Image signed with Cosign (keyless, GitHub OIDC); signature stored in GHCR.
5. Chart packaged with `--version <ver> --app-version <ver>`, pushed to
   `oci://ghcr.io/matabar/charts`, signed with Cosign.
6. **Air-gap bundle** `kafkafim-<ver>-airgap.tar.gz` attached to the GitHub Release:
   - `kafkafim-<ver>-image-amd64.tar` (OCI image archive; production is amd64)
   - `kafkafim-<ver>.tgz` (the chart)
   - `kafkafim-<ver>.sbom.spdx.json`
   - `SHA256SUMS`
   - `IMPORT.md` (the procedure below, with the exact version filled in)
7. GitHub Release created with auto-generated notes.

## Importing into the air-gapped environment

On a machine with internet access:

```bash
gh release download v1.2.3 --repo matabar/kafkafim --pattern 'kafkafim-1.2.3-airgap.tar.gz'
```

```bash
cosign verify ghcr.io/matabar/kafkafim:1.2.3 --certificate-identity-regexp 'https://github.com/matabar/kafkafim/' --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Signature verification happens here, before transfer, because keyless verification
needs the transparency log. Then carry the bundle across and, inside the air gap:

```bash
sha256sum -c SHA256SUMS
```

```bash
skopeo copy oci-archive:kafkafim-1.2.3-image-amd64.tar docker://registry.internal/kafkafim/kafkafim:1.2.3
```

```bash
helm push kafkafim-1.2.3.tgz oci://registry.internal/charts
```

```bash
helm upgrade --install kafkafim oci://registry.internal/charts/kafkafim --version 1.2.3 -f values-<cluster>.yaml --set image.registry=registry.internal/kafkafim
```

`docker load` + `docker push` works where `skopeo` is unavailable.

## Local developer loop

```bash
./mvnw verify                                # same as CI build job
```

```bash
docker build -t kafkafim:dev .               # same Dockerfile as CI, from target/*.jar
```

```bash
helm template charts/kafkafim -f charts/kafkafim/ci/two-groups-values.yaml
```

## Base image

`eclipse-temurin:27-jre` (Ubuntu). Distroless does not ship a Java 27 runtime yet;
revisit when it does. The image runs as UID 65532 with a read-only root filesystem
and `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`.
Class-data sharing (AppCDS) is a later startup optimisation, not needed for the MVP.
