package prometheus

import "testing"

func TestPercentile(t *testing.T) {
	if got := percentile([]float64{1, 100, 2, 3, 4}, .95); got != 4 {
		t.Fatalf("got %v", got)
	}
}
