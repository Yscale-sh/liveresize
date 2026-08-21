# LiveResize

LiveResize is a Kubernetes resource-sizing advisor and opt-in custom VPA
recommender. It combines live workload metrics, VPA recommendations, and
Prometheus history to suggest conservative CPU and memory floors. Read-only
advisory mode is the default.

## How it decides

For each Deployment-backed VPA, LiveResize selects the largest signal after
headroom: VPA uncapped target (30%), current pod use (50%), Prometheus 28-day
p95 (20%), and the p95 for the current UTC weekday/hour (20%). The final result
also has hard floors of 25m CPU and 64Mi memory. Time-of-week data makes a new
or restarted workload retain capacity for predictable demand instead of sizing
only from its quiet initial minutes.

`PROMETHEUS_URL` enables historical analysis. Without it, LiveResize keeps
working from Kubernetes metrics and VPA status, and exports its source health.
It requires read-only access to Pods, Deployments, PodMetrics, and VPAs.

## Custom recommender mode

Set `writeMode: true` in the Helm values to allow LiveResize to write the VPA
status subresource. A VPA must opt in explicitly; unselected VPAs are ignored:

```yaml
spec:
  recommenders:
    - name: liveresize
```

LiveResize writes per-container targets and the standard VPA updater and
admission controller apply them. It never patches workloads, VPA specs, pods,
or replica counts. For selected VPAs it does not reuse the prior VPA status as
an input, preventing its own recommendation from compounding on each analysis.
Container resource policies, including wildcard policies, min/max bounds, and
`mode: Off`, are honored. Lower and upper recommendation bounds use a 20%
hysteresis band around the target, clamped by the same resource policy, so the
VPA updater can act promptly without resizing on tiny fluctuations.

The write path deliberately owns only `status.recommendation` and the
`RecommendationProvided` condition. It does not write `status.observedGeneration`,
which is not part of the upstream VPA status schema. With
`updateMode: InPlaceOrRecreate`, the upstream VPA updater first patches the pod
`/resize` subresource and recreates the pod only when Kubernetes cannot apply the
resource change in place.

## Operations

`/healthz` is the liveness endpoint, `/metrics` exports each recommendation,
and `/recommendations` returns the current JSON report. Deploy with
`helm upgrade --install liveresize charts/liveresize --set
image.tag=<immutable-tag> --set
prometheusURL=http://prometheus.example.svc:9090`. Add
`--set writeMode=true` only when selected VPAs should be actuated.

To verify the complete live path, check that the target VPA names the
`liveresize` recommender, inspect the updater for `InPlaceResizedByVPA`, and
confirm the pod carries `vpaInPlaceUpdated: "true"`. A healthy recommendation
alone proves analysis, not actuation.
