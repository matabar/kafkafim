# ADR-0022: Delivery via GitHub Actions and GHCR; signed, versioned air-gap bundles

Status: accepted · Date: 2026-11-07

## Context
Development is on GitHub with internet access. Production is air-gapped; CI/CD never
deploys. The team carries an image and a chart into production and installs them.

## Decision
- **CI** (PRs and `main`): `./mvnw verify` (unit + Testcontainers integration +
  ArchUnit), Dockerfile build from the tested jar with a `validate`-profile smoke
  run, Helm lint/template/kubeconform/unittest. All required for merge.
- **CD** (tag `v*`): rebuild and re-test at the tag, multi-arch image to
  `ghcr.io/matabar/kafkafim`, SBOM (Syft), Cosign keyless signatures for image and
  chart, chart to `oci://ghcr.io/matabar/charts/kafkafim`, and an **air-gap bundle**
  attached to the GitHub Release: the pushed image saved by digest with
  `cosign save` (every platform plus signature and attestations), chart tgz, SBOM,
  checksums, import instructions. The bundle is never a second build.
- **Versioning**: semver from git tags; Maven `${revision}`; chart version =
  appVersion = tag; image tags `X.Y.Z`, `X.Y`, `sha-<7>`; never `latest`.
- **Base image**: `eclipse-temurin:27-jre`, non-root, read-only filesystem.
- **Registry**: GHCR for both images and OCI charts. Free for public repositories,
  authenticated with the workflow token, understood by `skopeo` and `helm` on the
  air-gap side.
- Dependabot for Maven, Actions and the base image.

## Alternatives
Docker Hub (pull rate limits), GitHub Pages Helm repo (legacy index model), Spring
Boot buildpacks (opaque layers, needs Docker in the build, slower).

## Consequences
Signature verification happens on the internet side before transfer (keyless
verification needs the transparency log); inside the air gap, integrity is the
checksum file. The import procedure is documented in `docs/DELIVERY.md` and shipped
inside every bundle.
