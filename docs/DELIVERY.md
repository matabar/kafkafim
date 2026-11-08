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
- Image tags per release: `1.2.3`, `1.2`, `sha-<7>`. Nothing is published on pushes
  to `main`; only `v*` tags publish. `latest` is never published.

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
| `build` | `./mvnw verify`: compile, unit tests (Surefire defaults minus `*IntegrationTest`), integration tests (Failsafe, `*IntegrationTest`, Testcontainers Kafka + Elasticsearch), ArchUnit package-boundary tests. Uploads test reports and the jar. |
| `image` | The Dockerfile builds from the tested jar (no push); `hadolint` on the Dockerfile; the container starts read-only and answers `/actuator/health/liveness` within 60 s. |
| `helm` | `helm lint`, `helm template` with every `charts/kafkafim/ci/*-values.yaml`, `kubeconform -strict` (v0.8.0) against Kubernetes schemas, `helm unittest` (helm-unittest 1.2.1) for the group-rendering logic and every template-time `fail`. Helm is pinned (`azure/setup-helm` `version:`, currently v4.3.1, the same in `release.yml`); helm-unittest is installed from its signed release tarball: the tarball and its `.prov` are fetched through the GitHub API asset endpoint (not `releases/download/`), checked against pinned SHA-256s, and Helm verifies the provenance with the release key committed at `.github/keys/helm-unittest-public-key.asc` (fingerprint pinned in `ci.yml`). Both tools are kept in an `actions/cache` keyed on their pins, so once a passing run has warmed it a GitHub or Go-proxy outage cannot fail the job. Skipped until the chart exists. |

Branch protection on `main`: PR required, all three jobs required, squash merge.
Dependabot (weekly, grouped) for Maven, GitHub Actions and the Docker base image.

## CD (`.github/workflows/release.yml`) on tag `v*`

1. `./mvnw -Drevision=<ver> verify` (tests run again on the exact tagged commit).
2. Multi-arch image (`linux/amd64`, `linux/arm64`) built with Buildx and pushed to
   GHCR with the tags above and BuildKit provenance attestations.
3. Image signed with Cosign (keyless, GitHub OIDC) in the classic `.sig`-tag format
   (`--new-bundle-format=false`), because that is the format `cosign save` and
   `cosign load` carry into and out of an OCI layout; `cosign verify` accepts it
   without flags. The signature is stored in GHCR next to the image.
4. SBOM (SPDX JSON, via Syft) generated from the pushed digest and attached to the
   release.
5. The **exact pushed digest** is saved with `cosign save` into an OCI layout
   directory. This carries every platform manifest plus the Cosign signature and
   attestations, so the bundle contains precisely the bytes that were signed. There
   is no second build.
6. Chart packaged with `--version <ver> --app-version <ver>`, pushed to
   `oci://ghcr.io/matabar/charts`, signed with Cosign.
7. **Air-gap bundle** `kafkafim-<ver>-airgap.tar.gz` attached to the GitHub Release:
   - `kafkafim-<ver>-image/` (OCI layout from `cosign save`: image, signature, attestations)
   - `kafkafim-<ver>-image.digest` (the `ghcr.io/...@sha256:...` reference that was signed)
   - `kafkafim-<ver>.tgz` (the chart)
   - `kafkafim-<ver>.sbom.spdx.json`
   - `SHA256SUMS` (every file in the bundle)
   - `IMPORT.md` (the procedure below, with the exact version filled in)
8. GitHub Release created with auto-generated notes.

## Importing into the air-gapped environment

On a machine with internet access:

```bash
gh release download v1.2.3 --repo matabar/kafkafim --pattern 'kafkafim-1.2.3-airgap.tar.gz'
```

```bash
cosign verify ghcr.io/matabar/kafkafim:1.2.3 --certificate-identity-regexp 'https://github.com/matabar/kafkafim/' --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Keyless verification needs the Sigstore transparency log, so the authoritative check
happens here, before transfer. Then carry the bundle across and, inside the air gap:

```bash
sha256sum -c SHA256SUMS
```

```bash
cosign load --dir kafkafim-1.2.3-image registry.internal/kafkafim/kafkafim:1.2.3
```

`cosign load` pushes the image and its signature and attestations together, so the
internal registry holds the same signed artifact as GHCR. To re-check the signature
inside the air gap, the importing machine needs `cosign` and a Sigstore trusted
root obtained on the internet side (`cosign initialize`, then carry its
`~/.sigstore` cache across, or pass `--trusted-root`); then
`cosign verify --offline` on the internal reference works without network access.
Without that, the checksum file is the integrity check inside the air gap. The SBOM
describes the `linux/amd64` image, which is what production runs.

```bash
helm push kafkafim-1.2.3.tgz oci://registry.internal/charts
```

The release is applied per Kubernetes cluster with plain `helm upgrade --install`, run
by the infrastructure repository's CI inside the air gap with that cluster's values
file; there is no Argo CD or Flux. At import time, record the digest from the bundle
in each per-cluster values file, next to the internal registry:

```bash
cut -d@ -f2 kafkafim-1.2.3-image.digest
```

```yaml
image:
  registry: registry.internal/kafkafim
  digest: sha256:...   # the output above
```

```bash
helm upgrade --install kafkafim oci://registry.internal/charts/kafkafim --version 1.2.3 -f values-<cluster>.yaml
```

For a manual install from the extracted bundle, the same values can be passed as
`--set image.registry=registry.internal/kafkafim --set image.digest="$(cut -d@ -f2 kafkafim-1.2.3-image.digest)"`.

`image.digest` pins the pods to the exact image that was signed and checksummed,
so a tag moved or overwritten in the internal registry cannot change what runs.
`kafkafim-1.2.3-image.digest` holds the full reference
(`ghcr.io/matabar/kafkafim@sha256:...`); the chart takes only the `sha256:<64 hex>`
part after `@` (the registry comes from `image.registry`), and its schema rejects
anything else. `cosign load` and `skopeo copy --all --preserve-digests` push the same
bytes, so the digest is the same in the internal registry. To check (prints `OK` or
`MISMATCH`):

```bash
[ "sha256:$(skopeo inspect --raw docker://registry.internal/kafkafim/kafkafim:1.2.3 | sha256sum | cut -d' ' -f1)" = "$(cut -d@ -f2 kafkafim-1.2.3-image.digest)" ] && echo OK || echo MISMATCH
```

The pod image is then `registry.internal/kafkafim/kafkafim@sha256:...`;
the `app.kubernetes.io/version` label still reads `1.2.3` (the chart's appVersion).
Leave `image.tag` empty, or set it to `1.2.3` for a `:1.2.3@sha256:...` reference; never put
`1.2.3@sha256:...` in `image.tag` (the chart fails and points at `image.digest`).

Where `cosign` is unavailable inside the air gap, `skopeo copy --all --preserve-digests oci:kafkafim-1.2.3-image docker://registry.internal/kafkafim/kafkafim:1.2.3` pushes the image without its signature (`--preserve-digests` makes skopeo fail rather than convert the manifests, which would change the digest).

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
