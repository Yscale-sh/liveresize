package advisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

var vpaGVR = schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}

type Config struct {
	Interval        time.Duration
	RecommenderName string
	Write           bool
}

func ConfigFromEnv() Config {
	d, err := time.ParseDuration(os.Getenv("ANALYZE_INTERVAL"))
	if err != nil || d <= 0 {
		d = 15 * time.Minute
	}
	name := os.Getenv("RECOMMENDER_NAME")
	if name == "" {
		name = "liveresize"
	}
	write, _ := strconv.ParseBool(os.Getenv("WRITE_MODE"))
	return Config{Interval: d, RecommenderName: name, Write: write}
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

func (a *Advisor) isSelected(vpa *unstructured.Unstructured) bool {
	recommenders, found, err := unstructured.NestedSlice(vpa.Object, "spec", "recommenders")
	if err != nil || !found || len(recommenders) != 1 {
		return false
	}
	recMap, ok := recommenders[0].(map[string]interface{})
	if !ok {
		return false
	}
	name, _ := recMap["name"].(string)
	return name == a.cfg.RecommenderName
}

type containerResult struct {
	containerName string
	uncapped      recommend.Quantity
	target        recommend.Quantity
}

func (a *Advisor) analyze(ctx context.Context) {
	items, err := a.dynamic.Resource(vpaGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		a.log.Error("list vpas", "error", err)
		return
	}
	recs := make([]Recommendation, 0, len(items.Items))
	for i := range items.Items {
		if a.cfg.Write && !a.isSelected(&items.Items[i]) {
			continue
		}
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
	selected := a.isSelected(vpa)
	if a.cfg.Write && !selected {
		return Recommendation{}, fmt.Errorf("vpa not opted in to recommender %q", a.cfg.RecommenderName)
	}

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

	liveMap, err := a.livePerContainer(ctx, dep)
	if err != nil {
		a.log.Warn("pod metrics unavailable", "deployment", name, "error", err)
	}

	var vpaSignalMap map[string]recommend.Quantity
	if !selected {
		vpaSignalMap = vpaUncappedPerContainer(vpa)
	}

	resourcePolicies := getResourcePolicies(vpa)

	var overallCPU, overallMem int64
	var allSources []string
	sourceSeen := make(map[string]bool)

	cResults := make([]containerResult, 0, len(dep.Spec.Template.Spec.Containers))

	for _, c := range dep.Spec.Template.Spec.Containers {
		cName := c.Name
		policy := matchResourcePolicy(resourcePolicies, cName)
		if policy != nil && policy.mode == "Off" {
			continue
		}
		liveQ := liveMap[cName]
		vpaQ := vpaSignalMap[cName]
		histCPU, seasonalCPU, histMem, seasonalMem := a.historyForContainer(ctx, vpa.GetNamespace(), name, cName)

		res := recommend.Calculate(recommend.Signals{
			VPA:           vpaQ,
			Current:       liveQ,
			HistoricalP95: recommend.Quantity{CPU: int64(histCPU * 1000), Memory: int64(histMem)},
			SeasonalPeak:  recommend.Quantity{CPU: int64(seasonalCPU * 1000), Memory: int64(seasonalMem)},
		})

		clampedTarget := applyResourcePolicy(res.Minimum, policy)

		cResults = append(cResults, containerResult{
			containerName: cName,
			uncapped:      res.Minimum,
			target:        clampedTarget,
		})

		overallCPU += clampedTarget.CPU
		overallMem += clampedTarget.Memory
		for _, s := range res.Sources {
			if !sourceSeen[s] {
				sourceSeen[s] = true
				allSources = append(allSources, s)
			}
		}
	}

	if a.cfg.Write && selected {
		if err := a.updateVPAStatus(ctx, vpa, cResults); err != nil {
			a.log.Error("failed to update vpa status", "vpa", vpa.GetName(), "error", err)
		}
	}

	return Recommendation{
		Namespace:   vpa.GetNamespace(),
		Name:        vpa.GetName(),
		Target:      name,
		CPUm:        overallCPU,
		MemoryBytes: overallMem,
		Sources:     allSources,
		At:          time.Now().UTC(),
	}, nil
}

func (a *Advisor) livePerContainer(ctx context.Context, dep *appsv1.Deployment) (map[string]recommend.Quantity, error) {
	pods, err := a.core.CoreV1().Pods(dep.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: dep.Spec.Selector.MatchLabels}),
	})
	if err != nil {
		return nil, err
	}
	res := make(map[string]recommend.Quantity)
	podMetricsList, err := a.metrics.MetricsV1beta1().PodMetricses(dep.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: dep.Spec.Selector.MatchLabels}),
	})
	if err == nil && len(podMetricsList.Items) > 0 {
		for _, m := range podMetricsList.Items {
			for _, c := range m.Containers {
				q := res[c.Name]
				cpuVal := c.Usage.Cpu().MilliValue()
				memVal := c.Usage.Memory().Value()
				if cpuVal > q.CPU {
					q.CPU = cpuVal
				}
				if memVal > q.Memory {
					q.Memory = memVal
				}
				res[c.Name] = q
			}
		}
		return res, nil
	}
	for _, p := range pods.Items {
		m, getErr := a.metrics.MetricsV1beta1().PodMetricses(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		if getErr != nil {
			continue
		}
		for _, c := range m.Containers {
			q := res[c.Name]
			cpuVal := c.Usage.Cpu().MilliValue()
			memVal := c.Usage.Memory().Value()
			if cpuVal > q.CPU {
				q.CPU = cpuVal
			}
			if memVal > q.Memory {
				q.Memory = memVal
			}
			res[c.Name] = q
		}
	}
	return res, nil
}

func (a *Advisor) historyForContainer(ctx context.Context, ns, depName, containerName string) (float64, float64, float64, float64) {
	if !a.prom.Enabled() {
		return 0, 0, 0, 0
	}
	re := depName + "-[a-z0-9]+-[a-z0-9]+"
	cpu, _ := a.prom.Range(ctx, fmt.Sprintf(`max(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container=%q}[5m]))`, ns, re, containerName), time.Now())
	mem, _ := a.prom.Range(ctx, fmt.Sprintf(`max(container_memory_working_set_bytes{namespace=%q,pod=~%q,container=%q})`, ns, re, containerName), time.Now())
	return cpu.P95, cpu.Seasonal, mem.P95, mem.Seasonal
}

func vpaUncappedPerContainer(v *unstructured.Unstructured) map[string]recommend.Quantity {
	cs, _, _ := unstructured.NestedSlice(v.Object, "status", "recommendation", "containerRecommendations")
	res := make(map[string]recommend.Quantity)
	for _, raw := range cs {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		cName, _ := m["containerName"].(string)
		if cName == "" {
			continue
		}
		target, _ := m["uncappedTarget"].(map[string]interface{})
		res[cName] = recommend.Quantity{
			CPU:    parseCPU(fmt.Sprint(target["cpu"])),
			Memory: parseMem(fmt.Sprint(target["memory"])),
		}
	}
	return res
}

type containerResourcePolicy struct {
	containerName string
	mode          string
	minCPU        *int64
	maxCPU        *int64
	minMem        *int64
	maxMem        *int64
}

func getResourcePolicies(vpa *unstructured.Unstructured) []containerResourcePolicy {
	cps, found, _ := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
	if !found {
		return nil
	}
	var res []containerResourcePolicy
	for _, raw := range cps {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		cName, _ := m["containerName"].(string)
		mode, _ := m["mode"].(string)
		p := containerResourcePolicy{
			containerName: cName,
			mode:          mode,
		}
		if minAllowed, ok := m["minAllowed"].(map[string]interface{}); ok {
			if cpu, ok := minAllowed["cpu"]; ok && fmt.Sprint(cpu) != "" {
				val := parseCPU(fmt.Sprint(cpu))
				p.minCPU = &val
			}
			if mem, ok := minAllowed["memory"]; ok && fmt.Sprint(mem) != "" {
				val := parseMem(fmt.Sprint(mem))
				p.minMem = &val
			}
		}
		if maxAllowed, ok := m["maxAllowed"].(map[string]interface{}); ok {
			if cpu, ok := maxAllowed["cpu"]; ok && fmt.Sprint(cpu) != "" {
				val := parseCPU(fmt.Sprint(cpu))
				p.maxCPU = &val
			}
			if mem, ok := maxAllowed["memory"]; ok && fmt.Sprint(mem) != "" {
				val := parseMem(fmt.Sprint(mem))
				p.maxMem = &val
			}
		}
		res = append(res, p)
	}
	return res
}

func matchResourcePolicy(policies []containerResourcePolicy, containerName string) *containerResourcePolicy {
	for i := range policies {
		if policies[i].containerName == containerName {
			return &policies[i]
		}
	}
	for i := range policies {
		if policies[i].containerName == "*" {
			return &policies[i]
		}
	}
	return nil
}

func applyResourcePolicy(q recommend.Quantity, policy *containerResourcePolicy) recommend.Quantity {
	if policy == nil {
		return q
	}
	if policy.mode == "Off" {
		return recommend.Quantity{CPU: 0, Memory: 0}
	}
	target := q
	if policy.minCPU != nil && target.CPU < *policy.minCPU {
		target.CPU = *policy.minCPU
	}
	if policy.maxCPU != nil && target.CPU > *policy.maxCPU {
		target.CPU = *policy.maxCPU
	}
	if policy.minMem != nil && target.Memory < *policy.minMem {
		target.Memory = *policy.minMem
	}
	if policy.maxMem != nil && target.Memory > *policy.maxMem {
		target.Memory = *policy.maxMem
	}
	return target
}

func formatCPU(milli int64) string {
	return fmt.Sprintf("%dm", milli)
}

func formatMem(bytes int64) string {
	return fmt.Sprintf("%d", bytes)
}

func (a *Advisor) updateVPAStatus(ctx context.Context, vpa *unstructured.Unstructured, cResults []containerResult) error {
	observedGen := vpa.GetGeneration()

	var containerRecs []interface{}
	for _, cr := range cResults {
		crMap := map[string]interface{}{
			"containerName": cr.containerName,
			"target": map[string]interface{}{
				"cpu":    formatCPU(cr.target.CPU),
				"memory": formatMem(cr.target.Memory),
			},
			"uncappedTarget": map[string]interface{}{
				"cpu":    formatCPU(cr.uncapped.CPU),
				"memory": formatMem(cr.uncapped.Memory),
			},
		}
		containerRecs = append(containerRecs, crMap)
	}

	newCond := map[string]interface{}{
		"type":               "RecommendationProvided",
		"status":             "True",
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}

	existingConditions, _, _ := unstructured.NestedSlice(vpa.Object, "status", "conditions")
	var updatedConditions []interface{}
	foundProvided := false
	for _, c := range existingConditions {
		cMap, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cMap["type"] == "RecommendationProvided" {
			foundProvided = true
			if cMap["status"] == "True" {
				if ltt, ok := cMap["lastTransitionTime"].(string); ok {
					newCond["lastTransitionTime"] = ltt
				}
			}
			updatedConditions = append(updatedConditions, newCond)
		} else {
			updatedConditions = append(updatedConditions, c)
		}
	}
	if !foundProvided {
		updatedConditions = append(updatedConditions, newCond)
	}

	existingObservedGen, foundObservedGen, _ := unstructured.NestedInt64(vpa.Object, "status", "observedGeneration")
	existingContainerRecs, _, _ := unstructured.NestedSlice(vpa.Object, "status", "recommendation", "containerRecommendations")

	if foundObservedGen && existingObservedGen == observedGen &&
		reflect.DeepEqual(existingContainerRecs, containerRecs) &&
		reflect.DeepEqual(existingConditions, updatedConditions) {
		a.log.Debug("vpa status unchanged, skipping write", "vpa", vpa.GetName())
		return nil
	}

	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"observedGeneration": observedGen,
			"recommendation": map[string]interface{}{
				"containerRecommendations": containerRecs,
			},
			"conditions": updatedConditions,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	_, err = a.dynamic.Resource(vpaGVR).Namespace(vpa.GetNamespace()).Patch(
		ctx,
		vpa.GetName(),
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		return fmt.Errorf("patch vpa status: %w", err)
	}
	a.log.Info("updated vpa status", "vpa", vpa.GetName(), "namespace", vpa.GetNamespace())
	return nil
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

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
