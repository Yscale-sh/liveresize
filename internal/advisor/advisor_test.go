package advisor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Yscale-sh/liveresize/internal/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

func newTestAdvisor(cfg Config, objs ...runtime.Object) (*Advisor, *dynamicfake.FakeDynamicClient, *k8sfake.Clientset, *metricsfake.Clientset) {
	coreClient := k8sfake.NewSimpleClientset()
	metricsClient := metricsfake.NewSimpleClientset()

	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	gvrToListKind := map[schema.GroupVersionResource]string{
		vpaGVR: "VerticalPodAutoscalerList",
	}
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objs...)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	promClient := prometheus.New("", nil)

	adv := New(coreClient, dynClient, metricsClient, promClient, logger, cfg)
	return adv, dynClient, coreClient, metricsClient
}

func makeVPA(ns, name, targetDeployment string, recommenders []string, containerPolicies []map[string]interface{}) *unstructured.Unstructured {
	vpa := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "autoscaling.k8s.io/v1",
			"kind":       "VerticalPodAutoscaler",
			"metadata": map[string]interface{}{
				"namespace":  ns,
				"name":       name,
				"generation": int64(1),
			},
			"spec": map[string]interface{}{
				"targetRef": map[string]interface{}{
					"kind": "Deployment",
					"name": targetDeployment,
				},
			},
		},
	}
	if len(recommenders) > 0 {
		var recList []interface{}
		for _, r := range recommenders {
			recList = append(recList, map[string]interface{}{"name": r})
		}
		_ = unstructured.SetNestedSlice(vpa.Object, recList, "spec", "recommenders")
	}
	if len(containerPolicies) > 0 {
		var cpList []interface{}
		for _, cp := range containerPolicies {
			cpList = append(cpList, cp)
		}
		_ = unstructured.SetNestedSlice(vpa.Object, cpList, "spec", "resourcePolicy", "containerPolicies")
	}
	return vpa
}

func setupDeploymentAndPods(t *testing.T, coreClient *k8sfake.Clientset, metricsClient *metricsfake.Clientset, ns, depName string, containers []string, podUsages []map[string][2]string) {
	t.Helper()
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: depName},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": depName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
			},
		},
	}
	for _, c := range containers {
		dep.Spec.Template.Spec.Containers = append(dep.Spec.Template.Spec.Containers, corev1.Container{Name: c})
	}
	_, err := coreClient.AppsV1().Deployments(ns).Create(context.Background(), dep, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	for i, usageMap := range podUsages {
		podName := fmt.Sprintf("%s-pod-%d", depName, i)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      podName,
				Labels:    map[string]string{"app": depName},
			},
		}
		_, err := coreClient.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create pod: %v", err)
		}

		podMetrics := &metricsv1beta1.PodMetrics{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      podName,
				Labels:    map[string]string{"app": depName},
			},
			Containers: []metricsv1beta1.ContainerMetrics{},
		}
		for cName, u := range usageMap {
			podMetrics.Containers = append(podMetrics.Containers, metricsv1beta1.ContainerMetrics{
				Name: cName,
				Usage: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(u[0]),
					corev1.ResourceMemory: resource.MustParse(u[1]),
				},
			})
		}
		metricsGVR := schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}
		if err := metricsClient.Tracker().Create(metricsGVR, podMetrics, ns); err != nil {
			t.Fatalf("add pod metrics: %v", err)
		}
	}
}

// 1. Opt-in selection test
func TestOptInSelection(t *testing.T) {
	vpaOptIn := makeVPA("default", "vpa-optin", "dep1", []string{"liveresize"}, nil)
	vpaOther := makeVPA("default", "vpa-other", "dep1", []string{"other-recommender"}, nil)
	vpaDefault := makeVPA("default", "vpa-default", "dep1", nil, nil)

	// In write mode
	cfgWrite := Config{Interval: 15 * time.Minute, RecommenderName: "liveresize", Write: true}
	advWrite, dynWrite, coreWrite, metricsWrite := newTestAdvisor(cfgWrite, vpaOptIn, vpaOther, vpaDefault)
	setupDeploymentAndPods(t, coreWrite, metricsWrite, "default", "dep1", []string{"app"}, []map[string][2]string{{"app": {"100m", "128Mi"}}})

	advWrite.analyze(context.Background())

	// vpaOptIn should have status updated
	vpaOptInUpdated, err := dynWrite.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-optin", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get vpaOptIn: %v", err)
	}
	conds, found, _ := unstructured.NestedSlice(vpaOptInUpdated.Object, "status", "conditions")
	if !found || len(conds) == 0 {
		t.Fatalf("expected status conditions on opted in VPA")
	}

	// vpaOther & vpaDefault should NOT have status updated
	vpaOtherGet, _ := dynWrite.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-other", metav1.GetOptions{})
	if _, found, _ := unstructured.NestedSlice(vpaOtherGet.Object, "status", "conditions"); found {
		t.Fatalf("vpa-other should not have status updated")
	}
	vpaDefaultGet, _ := dynWrite.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-default", metav1.GetOptions{})
	if _, found, _ := unstructured.NestedSlice(vpaDefaultGet.Object, "status", "conditions"); found {
		t.Fatalf("vpa-default should not have status updated")
	}

	// In advisory mode (Write=false)
	cfgRead := Config{Interval: 15 * time.Minute, RecommenderName: "liveresize", Write: false}
	advRead, dynRead, coreRead, metricsRead := newTestAdvisor(cfgRead, vpaOptIn, vpaOther, vpaDefault)
	setupDeploymentAndPods(t, coreRead, metricsRead, "default", "dep1", []string{"app"}, []map[string][2]string{{"app": {"100m", "128Mi"}}})

	advRead.analyze(context.Background())

	// Snapshot should contain analysis for all VPAs targeting dep1
	snaps := advRead.Snapshot()
	if len(snaps) != 3 {
		t.Fatalf("advisory snapshot len=%d, want 3", len(snaps))
	}
	// No status writes in read mode
	vpaOptInRead, _ := dynRead.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-optin", metav1.GetOptions{})
	if _, found, _ := unstructured.NestedSlice(vpaOptInRead.Object, "status", "conditions"); found {
		t.Fatalf("advisory mode should not write status")
	}
}

// 2. Per-container separation & max pod usage test
func TestPerContainerMaxPodUsage(t *testing.T) {
	cfg := Config{Interval: 15 * time.Minute, RecommenderName: "liveresize", Write: true}
	vpa := makeVPA("default", "vpa-multi", "dep-multi", []string{"liveresize"}, nil)
	adv, dyn, core, metrics := newTestAdvisor(cfg, vpa)

	// 2 pods, 2 containers each ("web" and "worker")
	// pod 0: web=100m/100Mi, worker=300m/300Mi
	// pod 1: web=200m/200Mi, worker=150m/150Mi
	// Max per pod: web -> 200m / 200Mi, worker -> 300m / 300Mi
	setupDeploymentAndPods(t, core, metrics, "default", "dep-multi", []string{"web", "worker"}, []map[string][2]string{
		{"web": {"100m", "100Mi"}, "worker": {"300m", "300Mi"}},
		{"web": {"200m", "200Mi"}, "worker": {"150m", "150Mi"}},
	})
	adv.analyze(context.Background())

	vpaUpdated, err := dyn.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-multi", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get vpa: %v", err)
	}

	cRecs, found, _ := unstructured.NestedSlice(vpaUpdated.Object, "status", "recommendation", "containerRecommendations")
	if !found || len(cRecs) != 2 {
		t.Fatalf("expected 2 container recommendations, got %v", cRecs)
	}

	recMap := make(map[string]map[string]interface{})
	for _, raw := range cRecs {
		m := raw.(map[string]interface{})
		cName := m["containerName"].(string)
		recMap[cName] = m
	}

	// web live=200m, 200Mi -> headroom 1.50 -> 300m, 300Mi (314572800 bytes)
	webTarget := recMap["web"]["target"].(map[string]interface{})
	if webTarget["cpu"] != "300m" {
		t.Errorf("web target cpu=%v, want 300m", webTarget["cpu"])
	}
	wantWebMem := formatMem(int64(200 * 1024 * 1024 * 1.5))
	if webTarget["memory"] != wantWebMem {
		t.Errorf("web target memory=%v, want %s", webTarget["memory"], wantWebMem)
	}
	webLower := recMap["web"]["lowerBound"].(map[string]interface{})
	webUpper := recMap["web"]["upperBound"].(map[string]interface{})
	if webLower["cpu"] != "240m" || webUpper["cpu"] != "360m" {
		t.Errorf("web bounds cpu=%v..%v, want 240m..360m", webLower["cpu"], webUpper["cpu"])
	}

	// worker live=300m, 300Mi -> headroom 1.50 -> 450m, 450Mi (471859200 bytes)
	workerTarget := recMap["worker"]["target"].(map[string]interface{})
	if workerTarget["cpu"] != "450m" {
		t.Errorf("worker target cpu=%v, want 450m", workerTarget["cpu"])
	}
	wantWorkerMem := formatMem(int64(300 * 1024 * 1024 * 1.5))
	if workerTarget["memory"] != wantWorkerMem {
		t.Errorf("worker target memory=%v, want %s", workerTarget["memory"], wantWorkerMem)
	}
}

// 3. Resource policy clamping & Off mode test
func TestResourcePolicyClampingAndOff(t *testing.T) {
	cfg := Config{Interval: 15 * time.Minute, RecommenderName: "liveresize", Write: true}

	policies := []map[string]interface{}{
		{
			"containerName": "c-clamp",
			"minAllowed":    map[string]interface{}{"cpu": "500m", "memory": "1Gi"},
			"maxAllowed":    map[string]interface{}{"cpu": "1000m", "memory": "2Gi"},
		},
		{
			"containerName": "c-off",
			"mode":          "Off",
		},
	}
	vpa := makeVPA("default", "vpa-policy", "dep-policy", []string{"liveresize"}, policies)
	adv, dyn, core, metrics := newTestAdvisor(cfg, vpa)

	// pod usage: c-clamp = 100m, 100Mi (uncapped calculate: 150m, 150Mi) -> minAllowed clamps to 500m, 1Gi
	// c-off = 100m, 100Mi -> mode Off -> target: cpu 0m, memory 0
	setupDeploymentAndPods(t, core, metrics, "default", "dep-policy", []string{"c-clamp", "c-off"}, []map[string][2]string{
		{"c-clamp": {"100m", "100Mi"}, "c-off": {"100m", "100Mi"}},
	})

	adv.analyze(context.Background())

	vpaUpdated, err := dyn.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-policy", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get vpa: %v", err)
	}

	cRecs, _, _ := unstructured.NestedSlice(vpaUpdated.Object, "status", "recommendation", "containerRecommendations")
	recMap := make(map[string]map[string]interface{})
	for _, raw := range cRecs {
		m := raw.(map[string]interface{})
		cName := m["containerName"].(string)
		recMap[cName] = m
	}

	// c-clamp
	clampTarget := recMap["c-clamp"]["target"].(map[string]interface{})
	clampUncapped := recMap["c-clamp"]["uncappedTarget"].(map[string]interface{})
	if clampUncapped["cpu"] != "150m" {
		t.Errorf("c-clamp uncapped cpu=%v, want 150m", clampUncapped["cpu"])
	}
	if clampTarget["cpu"] != "500m" {
		t.Errorf("c-clamp target cpu=%v, want clamped 500m", clampTarget["cpu"])
	}
	if clampTarget["memory"] != formatMem(1024*1024*1024) {
		t.Errorf("c-clamp target memory=%v, want 1Gi", clampTarget["memory"])
	}
	clampLower := recMap["c-clamp"]["lowerBound"].(map[string]interface{})
	clampUpper := recMap["c-clamp"]["upperBound"].(map[string]interface{})
	if clampLower["cpu"] != "500m" || clampUpper["cpu"] != "600m" {
		t.Errorf("c-clamp bounds cpu=%v..%v, want policy-clamped 500m..600m", clampLower["cpu"], clampUpper["cpu"])
	}

	// VPA requires containers in Off mode to be omitted entirely.
	if _, found := recMap["c-off"]; found {
		t.Errorf("c-off received a recommendation despite mode Off")
	}
}

// 4. No self-amplification test
func TestNoSelfAmplification(t *testing.T) {
	cfg := Config{Interval: 15 * time.Minute, RecommenderName: "liveresize", Write: true}

	vpa := makeVPA("default", "vpa-optin", "dep-sa", []string{"liveresize"}, nil)
	// Simulate previously written status recommendation with high target
	_ = unstructured.SetNestedSlice(vpa.Object, []interface{}{
		map[string]interface{}{
			"containerName":  "app",
			"target":         map[string]interface{}{"cpu": "5000m", "memory": "10Gi"},
			"uncappedTarget": map[string]interface{}{"cpu": "5000m", "memory": "10Gi"},
		},
	}, "status", "recommendation", "containerRecommendations")

	adv, dyn, core, metrics := newTestAdvisor(cfg, vpa)

	// Low pod usage: 100m, 100Mi -> uncapped calculate should be 150m, 150Mi
	// If VPA signal was read, it would be 5000m * 1.30 = 6500m. It MUST NOT be read.
	setupDeploymentAndPods(t, core, metrics, "default", "dep-sa", []string{"app"}, []map[string][2]string{
		{"app": {"100m", "100Mi"}},
	})

	adv.analyze(context.Background())

	vpaUpdated, err := dyn.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-optin", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get vpa: %v", err)
	}

	cRecs, _, _ := unstructured.NestedSlice(vpaUpdated.Object, "status", "recommendation", "containerRecommendations")
	appRec := cRecs[0].(map[string]interface{})
	uncapped := appRec["uncappedTarget"].(map[string]interface{})
	if uncapped["cpu"] != "150m" {
		t.Fatalf("uncapped cpu=%v, want 150m (self-amplification occurred if > 150m)", uncapped["cpu"])
	}
}

// 5. Status update and no-op test
func TestStatusUpdateAndNoOp(t *testing.T) {
	cfg := Config{Interval: 15 * time.Minute, RecommenderName: "liveresize", Write: true}
	vpa := makeVPA("default", "vpa-noop", "dep-noop", []string{"liveresize"}, nil)
	adv, dyn, core, metrics := newTestAdvisor(cfg, vpa)
	setupDeploymentAndPods(t, core, metrics, "default", "dep-noop", []string{"app"}, []map[string][2]string{
		{"app": {"100m", "100Mi"}},
	})

	// First run
	adv.analyze(context.Background())
	firstPatchCount := 0
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "patch" && action.GetSubresource() == "status" {
			firstPatchCount++
		}
	}
	if firstPatchCount != 1 {
		t.Fatalf("status patches after first run=%d, want 1", firstPatchCount)
	}

	vpa1, _ := dyn.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-noop", metav1.GetOptions{})
	conds1, _, _ := unstructured.NestedSlice(vpa1.Object, "status", "conditions")
	if len(conds1) == 0 {
		t.Fatalf("expected condition on first run")
	}
	ltt1 := conds1[0].(map[string]interface{})["lastTransitionTime"].(string)

	// Sleep slightly to ensure time advances if timestamp were recalculated
	time.Sleep(10 * time.Millisecond)

	// Second run with unchanged status
	adv.analyze(context.Background())
	secondPatchCount := 0
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "patch" && action.GetSubresource() == "status" {
			secondPatchCount++
		}
	}
	if secondPatchCount != firstPatchCount {
		t.Fatalf("unchanged status was patched again: before=%d after=%d", firstPatchCount, secondPatchCount)
	}

	vpa2, _ := dyn.Resource(vpaGVR).Namespace("default").Get(context.Background(), "vpa-noop", metav1.GetOptions{})
	conds2, _, _ := unstructured.NestedSlice(vpa2.Object, "status", "conditions")
	ltt2 := conds2[0].(map[string]interface{})["lastTransitionTime"].(string)

	if ltt1 != ltt2 {
		t.Fatalf("lastTransitionTime changed (%s vs %s), no-op check failed", ltt1, ltt2)
	}
}
