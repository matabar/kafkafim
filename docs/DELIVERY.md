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
| `changes` (check "Detect changes") | Nothing; it classifies the pull request's changed paths for the jobs below (see "Docs-only pull requests"). On pushes to `main` it selects every job. |
| `build` (check "Build and test") | `./mvnw verify -DskipITs`: Spotless, Checkstyle, compile, unit tests (Surefire defaults minus `*IntegrationTest`), ArchUnit package-boundary tests. Uploads the unit test reports and the jar. |
| `integration` (check "Integration tests") | Failsafe only (`*IntegrationTest`, Testcontainers Kafka + Elasticsearch + LGTM): `./mvnw verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.check.skip=true -Dcheckstyle.skip=true`; the classes run in three parallel JVMs (`it.forkCount` in `pom.xml`). Runs in parallel with `build`, needing only `changes`; one job, not a matrix of shards, to stay well inside the Free plan's 20 concurrent jobs. Its first step starts pulling, in the background, every image named by a `DockerImageName.parse("...")` in `src/test` (so the list cannot drift from the tests), overlapping the pulls with Java setup and compilation; a test that starts a container before its pull finishes joins the daemon's in-flight pull. Uploads the Failsafe reports. |
| `image` (check "Container image") | Needs only `build`, so it runs while `integration` is still running. The Dockerfile builds from `build`'s jar (no push); `hadolint` on the Dockerfile; the container starts read-only and answers `/actuator/health/liveness` within 60 s. |
| `helm` (check "Helm chart") | `helm lint`, `helm template` with every `charts/kafkafim/ci/*-values.yaml`, `kubeconform -strict` (v0.8.0) against Kubernetes schemas, `helm unittest` (helm-unittest 1.2.1) for the group-rendering logic and every template-time `fail`. Helm is pinned (`azure/setup-helm` `version:`, currently v4.3.1, the same in `release.yml`); helm-unittest is installed from its signed release tarball: the tarball and its `.prov` are fetched through the GitHub API asset endpoint (not `releases/download/`), checked against pinned SHA-256s, and Helm verifies the provenance with the release key committed at `.github/keys/helm-unittest-public-key.asc` (fingerprint pinned in `ci.yml`). Both tools are kept in an `actions/cache` keyed on their pins, so once a passing run has warmed it (caches unused for 7 days are evicted) an outage of github.com release downloads or of the Go module proxy cannot fail the job. Checkout, the Actions cache service, `azure/setup-helm` (get.helm.sh) and kubeconform's schemas (raw.githubusercontent.com) are still needed on every run. The chart steps are skipped until the chart exists, and on pull requests that change none of `charts/**`, `.github/workflows/ci.yml` or `.github/keys/**`. The same job also checks [`deploy/grafana/`](../deploy/grafana/README.md), so the required checks stay the same: `jq empty` on every `dashboards/*.json`, `promtool check rules` on every `alerts/*.yaml` and `promtool test rules` on every `alerts/*.test.yaml` (run from `alerts/`, since the tests name their rule files relative to themselves); it fails if any of the three globs is empty. promtool comes from the Prometheus 3.15.1 `linux-amd64` release tarball, fetched through the GitHub API asset endpoint, checked against a pinned SHA-256 on every run and kept in its own `actions/cache`. These steps run on pull requests that change `deploy/grafana/**` or `ci.yml`, and on every push to `main`. The job is skipped on pull requests that change none of `charts/**`, `.github/keys/**`, `deploy/grafana/**` and `ci.yml`. |

A new push to a pull request cancels the pull request's run still in progress. Runs on
`main` are never cancelled or replaced: each pushed commit has its own concurrency group
(keyed on the commit SHA), so every merged commit gets a complete run, including the
image build, even when several merges land within minutes.

### Docs-only pull requests

A pull request whose every changed path (both sides of a rename) is `*.md` outside
`src/` (Maven packages `src/main/resources`), under `docs/`, or `LICENSE` skips `build`, `integration` and therefore `image`; `helm` runs only if the
chart, `ci.yml`, `.github/keys/` or `deploy/grafana/` changed (a pull request that changes
only `deploy/grafana/README.md` is docs-only but still gets the Grafana checks). Any other
file under `deploy/grafana/` is not docs, so the Java and image jobs run for it too. The paths are what merging would change
(`git diff` between the PR merge commit and the base tip). The skip is a job-level `if:`,
not a workflow-level `paths-ignore`: GitHub reports a job skipped by `if:` as a passing
check, while a workflow that never starts leaves required checks pending. If `changes`
itself fails, its outputs are empty and every job runs. Pushes to `main` and
`release.yml` always run everything. Locally, docs-only changes still run
`./mvnw -DskipTests verify` (see CONTRIBUTING.md).

Branch protection on `main` (a classic branch protection rule, in force since
2026-11-09):

- Four required status checks: "Build and test", "Integration tests", "Container
  image" and "Helm chart". "Detect changes" is not required. `image` does not wait
  for the integration tests, so "Integration tests" and "Container image" are both
  required: both must pass before a pull request merges. A job skipped by its `if:`
  (docs-only pull requests, or `helm` when none of `charts/**`, `ci.yml`,
  `.github/keys/**` and `deploy/grafana/**` changed) reports as passing, so it does not block the merge.
- Branches must be up to date with `main` before merging (strict status checks).
- Administrators are included: the rule applies to them too.
- Force-pushes to `main` and deleting `main` are blocked.
- The repository allows squash merges only (merge commits and rebase merges are
  off), so every pull request lands as one commit.

The rule requires no approving review; reviews are a process rule (CONTRIBUTING.md).

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

Where `cosign` is unavailable inside the air gap, skopeo (1.18 or later, for the
`@<index>` source syntax) pushes the image, then its signature in a second copy:

```bash
skopeo copy --all --preserve-digests oci:kafkafim-1.2.3-image:@0 docker://registry.internal/kafkafim/kafkafim:1.2.3
```

```bash
skopeo copy --preserve-digests oci:kafkafim-1.2.3-image:@1 "docker://registry.internal/kafkafim/kafkafim:$(cut -d@ -f2 kafkafim-1.2.3-image.digest | tr : -).sig"
```

The layout `cosign save` writes holds more than one manifest (the image index, then
the signature manifest, plus Cosign attestation manifests if any were attached; each
annotated `kind: dev.cosignproject.cosign/...`) and no
`org.opencontainers.image.ref.name`, so plain `oci:kafkafim-1.2.3-image` fails with
"more than one image in oci, choose an image" and a `:<tag>` reference matches
nothing; `:@0` selects the first entry of `index.json`, the image index. Check that
`jq -r '.manifests[0].annotations.kind' kafkafim-1.2.3-image/index.json` prints
`dev.cosignproject.cosign/imageIndex` (or read `index.json`), and run the digest
check above afterwards. `--preserve-digests` makes skopeo fail rather than convert
the manifests, which would change the digest. (Checked with cosign 3.1.3 and
skopeo 1.24.1.)

The second copy recreates what `cosign load` writes: Cosign finds a classic signature
under the tag `sha256-<hex>.sig` next to the image, where `sha256:<hex>` is the signed
digest from `kafkafim-1.2.3-image.digest`. `:@1` is the second entry of `index.json`,
the signature manifest; check that
`jq -r '.manifests[1].annotations.kind' kafkafim-1.2.3-image/index.json` prints
`dev.cosignproject.cosign/sigs`. Skip the second copy and the internal registry holds
the image unsigned: `cosign verify` there fails with `no signatures found`. With both
copies, `cosign verify --offline` works on the internal reference as after
`cosign load`, given the trusted root described above. A release has no Cosign
attestation; a layout that has one (a third entry, kind
`dev.cosignproject.cosign/atts`) takes a third copy, `:@2` to `sha256-<hex>.att`.
(Checked with cosign 3.1.3 and skopeo 1.24.1 against a local registry, signing with
a local key in the release's `--new-bundle-format=false` format: both copies keep
their digests, and `cosign verify` and `cosign verify-attestation` pass on the copy.)
Keyless verification of the copy was not tested; the release's certificate and
transparency-log entry are part of the signature manifest, which skopeo copies
unchanged.

## Local developer loop

```bash
./mvnw verify                                # CI build + integration jobs together
```

```bash
docker build -t kafkafim:dev .               # same Dockerfile as CI, from target/*.jar
```

```bash
helm template charts/kafkafim -f charts/kafkafim/ci/two-groups-values.yaml
```

```bash
deploy/local/demo.sh up                      # the chart on a local kind cluster (deploy/local/README.md)
```

## Base image

`eclipse-temurin:27-jre` (Ubuntu). Distroless does not ship a Java 27 runtime yet;
revisit when it does. The image runs as UID 65532 with a read-only root filesystem
and `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`.
Class-data sharing (AppCDS) is a later startup optimisation, not needed for the MVP.
