# kafkafim Helm chart

Deploys [kafkafim](https://github.com/matabar/kafkafim) (Kafka → Elasticsearch
ingest). One release per Kubernetes cluster; each entry under `groups` becomes a
`Deployment`, `ConfigMap`, `PodDisruptionBudget`, `Service` and optional CPU
`HorizontalPodAutoscaler`. The chart creates no `Secret` and needs no CRDs.

The design, the values shape and every validation rule are documented in
[docs/HELM.md](https://github.com/matabar/kafkafim/blob/main/docs/HELM.md); `values.yaml` is commented field by field.

```bash
helm upgrade --install kafkafim oci://ghcr.io/matabar/charts/kafkafim --version <ver> -f values-<cluster>.yaml
```

Requires Kubernetes 1.27+ (`matchLabelKeys` in the zone topology spread). Development:
`helm lint`, `helm template -f ci/<name>-values.yaml | kubeconform -strict`, and
`helm unittest .` (helm-unittest 1.2.1) from this directory.
