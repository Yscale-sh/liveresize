package recommend

import "testing"

func TestCalculateUsesLargestSignalWithHeadroom(t *testing.T) {
	r := Calculate(Signals{VPA: Quantity{CPU: 15, Memory: 100}, Current: Quantity{CPU: 40, Memory: 1000}, HistoricalP95: Quantity{CPU: 80, Memory: 2000}, SeasonalPeak: Quantity{CPU: 120, Memory: 1500}})
	if r.Minimum.CPU != 144 {
		t.Fatalf("cpu=%d, want 144", r.Minimum.CPU)
	}
	if r.Minimum.Memory != 64*1024*1024 {
		t.Fatalf("memory=%d, want hard floor", r.Minimum.Memory)
	}
}
func TestCalculateHasSafeFloorWithoutSignals(t *testing.T) {
	r := Calculate(Signals{})
	if r.Minimum.CPU != 25 || r.Minimum.Memory != 64*1024*1024 {
		t.Fatalf("unsafe zero result: %#v", r.Minimum)
	}
}
