package advisor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Yscale-sh/liveresize/internal/prometheus"
	"github.com/Yscale-sh/liveresize/internal/recommend"
	clientprom "github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

var vpaGVR = schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}

type Config struct{ Interval time.Duration }

func ConfigFromEnv() Config {
	d, err := time.ParseDuration(os.Getenv("ANALYZE_INTERVAL"))
	if err != nil || d <= 0 {
		d = 15 * time.Minute
	}
	return Config{Interval: d}
}

type Recommendation struct {
	Namespace, Name, Target string
	CPUm, MemoryBytes       int64
	Sources                 []string
	At                      time.Time
}
type Advisor struct {
	core        kubernetes.Interface
	dynamic     dynamic.Interface
	metrics     metricsclient.Interface
	prom        *prometheus.Client
	log         *slog.Logger
	cfg         Config
	mu          sync.RWMutex
	recs        []Recommendation
	lastSuccess float64
}

var recommendationCPU = clientprom.NewGaugeVec(clientprom.GaugeOpts{Name: "liveresize_recommendation_cpu_millicores", Help: "Advisory recommended VPA minimum CPU."}, []string{"namespace", "vpa", "target"})
var recommendationMemory = clientprom.NewGaugeVec(clientprom.GaugeOpts{Name: "liveresize_recommendation_memory_bytes", Help: "Advisory recommended VPA minimum memory."}, []string{"namespace", "vpa", "target"})
var sourceAvailable = clientprom.NewGaugeVec(clientprom.GaugeOpts{Name: "liveresize_source_available", Help: "Whether a recommendation source was available in the latest run."}, []string{"source"})
var lastSuccess = clientprom.NewGauge(clientprom.GaugeOpts{Name: "liveresize_last_success_unixtime", Help: "Unix time of last complete sizing analysis."})

func init() {
	clientprom.MustRegister(recommendationCPU, recommendationMemory, sourceAvailable, lastSuccess)
}
func New(core kubernetes.Interface, dyn dynamic.Interface, metrics metricsclient.Interface, prom *prometheus.Client, log *slog.Logger, cfg Config) *Advisor {
	return &Advisor{core: core, dynamic: dyn, metrics: metrics, prom: prom, log: log, cfg: cfg}
}
func (a *Advisor) Run(ctx context.Context) {
	a.analyze(ctx)
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.analyze(ctx)
		}
	}
}
func (a *Advisor) Snapshot() []Recommendation {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return append([]Recommendation(nil), a.recs...)
}

func (a *Advisor) analyze(ctx context.Context) {
	items, err := a.dynamic.Resource(vpaGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		a.log.Error("list vpas", "error", err)
		return
	}
	recs := make([]Recommendation, 0, len(items.Items))
	for i := range items.Items {
		r, err := a.one(ctx, &items.Items[i])
		if err != nil {
			a.log.Warn("analyze vpa", "vpa", items.Items[i].GetName(), "error", err)
			continue
		}
		recs = append(recs, r)
		recommendationCPU.WithLabelValues(r.Namespace, r.Name, r.Target).Set(float64(r.CPUm))
		recommendationMemory.WithLabelValues(r.Namespace, r.Name, r.Target).Set(float64(r.MemoryBytes))
		a.log.Info("sizing recommendation", "namespace", r.Namespace, "vpa", r.Name, "target", r.Target, "cpu_m", r.CPUm, "memory_bytes", r.MemoryBytes, "sources", r.Sources)
	}
	a.mu.Lock()
	a.recs = recs
	a.lastSuccess = float64(time.Now().Unix())
	a.mu.Unlock()
	lastSuccess.Set(a.lastSuccess)
	sourceAvailable.WithLabelValues("prometheus").Set(boolFloat(a.prom.Enabled()))
}
func (a *Advisor) one(ctx context.Context, vpa *unstructured.Unstructured) (Recommendation, error) {
	target, found, _ := unstructured.NestedMap(vpa.Object, "spec", "targetRef")
	if !found || target["kind"] != "Deployment" {
		return Recommendation{}, fmt.Errorf("only Deployment targetRefs are supported")
	}
	name, _ := target["name"].(string)
	if name == "" {
		return Recommendation{}, fmt.Errorf("missing target name")
	}
	dep, err := a.core.AppsV1().Deployments(vpa.GetNamespace()).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return Recommendation{}, err
	}
	live, err := a.live(ctx, dep)
	if err != nil {
		a.log.Warn("pod metrics unavailable", "deployment", name, "error", err)
	}
	vpaSignal := quantity(vpa)
	histCPU, seasonalCPU, histMem, seasonalMem := a.history(ctx, vpa.GetNamespace(), name)
	r := recommend.Calculate(recommend.Signals{
		VPA:           vpaSignal,
		Current:       live,
		HistoricalP95: recommend.Quantity{CPU: int64(histCPU * 1000), Memory: int64(histMem)},
		SeasonalPeak:  recommend.Quantity{CPU: int64(seasonalCPU * 1000), Memory: int64(seasonalMem)},
	})
	return Recommendation{Namespace: vpa.GetNamespace(), Name: vpa.GetName(), Target: name, CPUm: r.Minimum.CPU, MemoryBytes: r.Minimum.Memory, Sources: r.Sources, At: time.Now().UTC()}, nil
}
func (a *Advisor) live(ctx context.Context, dep *appsv1.Deployment) (recommend.Quantity, error) {
	pods, err := a.core.CoreV1().Pods(dep.Namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: dep.Spec.Selector.MatchLabels})})
	if err != nil {
		return recommend.Quantity{}, err
	}
	var q recommend.Quantity
	for _, p := range pods.Items {
		m, err := a.metrics.MetricsV1beta1().PodMetricses(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		for _, c := range m.Containers {
			q.CPU += c.Usage.Cpu().MilliValue()
			q.Memory += c.Usage.Memory().Value()
		}
	}
	return q, nil
}
func (a *Advisor) history(ctx context.Context, ns, name string) (float64, float64, float64, float64) {
	if !a.prom.Enabled() {
		return 0, 0, 0, 0
	}
	re := name + "-[a-z0-9]+-[a-z0-9]+"
	cpu, _ := a.prom.Range(ctx, fmt.Sprintf(`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container!="POD",image!=""}[5m]))`, ns, re), time.Now())
	mem, _ := a.prom.Range(ctx, fmt.Sprintf(`sum(container_memory_working_set_bytes{namespace=%q,pod=~%q,container!="POD",image!=""})`, ns, re), time.Now())
	return cpu.P95, cpu.Seasonal, mem.P95, mem.Seasonal
}
func quantity(v *unstructured.Unstructured) recommend.Quantity {
	cs, _, _ := unstructured.NestedSlice(v.Object, "status", "recommendation", "containerRecommendations")
	var q recommend.Quantity
	for _, raw := range cs {
		m, _ := raw.(map[string]interface{})
		target, _ := m["uncappedTarget"].(map[string]interface{})
		q.CPU = max(q.CPU, parseCPU(fmt.Sprint(target["cpu"])))
		q.Memory = max(q.Memory, parseMem(fmt.Sprint(target["memory"])))
	}
	return q
}
func parseCPU(s string) int64 {
	if len(s) > 1 && s[len(s)-1] == 'm' {
		n, _ := strconv.ParseInt(s[:len(s)-1], 10, 64)
		return n
	}
	n, _ := strconv.ParseFloat(s, 64)
	return int64(n * 1000)
}
func parseMem(s string) int64 {
	units := map[string]int64{"Ki": 1024, "Mi": 1024 * 1024, "Gi": 1024 * 1024 * 1024}
	for u, m := range units {
		if len(s) > len(u) && s[len(s)-len(u):] == u {
			n, _ := strconv.ParseFloat(s[:len(s)-len(u)], 64)
			return int64(n * float64(m))
		}
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
