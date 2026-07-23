package recommend

import "math"

// Signals are usage/recommender values in millicores and bytes. Zero means unavailable.
type Signals struct{ VPA, Current, HistoricalP95, SeasonalPeak Quantity }
type Quantity struct {
	CPU    int64
	Memory int64
}
type Result struct {
	Minimum Quantity
	Sources []string
}

const (
	minCPU    = int64(25) // protects tiny workloads from scheduler-noise recommendations
	minMemory = int64(64 * 1024 * 1024)
)

// Calculate chooses the largest independent signal after explicit headroom. A VPA
// uncapped target gets 30%, live use 50%, and historical/seasonal demand 20%.
func Calculate(s Signals) Result {
	r := Result{Minimum: Quantity{CPU: minCPU, Memory: minMemory}}
	apply := func(name string, q Quantity, factor float64) {
		if q.CPU == 0 && q.Memory == 0 {
			return
		}
		r.Sources = append(r.Sources, name)
		r.Minimum.CPU = max(r.Minimum.CPU, ceil(q.CPU, factor))
		r.Minimum.Memory = max(r.Minimum.Memory, ceil(q.Memory, factor))
	}
	apply("vpa_uncapped", s.VPA, 1.30)
	apply("live", s.Current, 1.50)
	apply("prometheus_p95", s.HistoricalP95, 1.20)
	apply("time_of_week", s.SeasonalPeak, 1.20)
	return r
}
func ceil(v int64, f float64) int64 { return int64(math.Ceil(float64(v) * f)) }
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
