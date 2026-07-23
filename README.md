# LiveResize

LiveResize is a read-only Kubernetes resource-sizing advisor. It combines live
workload metrics, VPA recommendations, and Prometheus history to suggest
conservative CPU and memory floors. It never modifies workloads or VPAs.

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

## Operations

`/healthz` is the liveness endpoint, `/metrics` exports each recommendation,
and `/recommendations` returns the current JSON report. The service has no
mutating endpoint or apply mode. Deploy with `helm upgrade --install liveresize
charts/liveresize --set image.tag=<immutable-tag> --set
prometheusURL=http://prometheus.example.svc:9090`.
