package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func New(base string, client *http.Client) *Client { return &Client{base: base, http: client} }
func (c *Client) Enabled() bool                    { return c.base != "" }

// History summarizes 28d of hourly samples. Seasonal is the p95 for the same
// UTC weekday/hour as now, making cold starts account for predictable demand.
type History struct{ P95, Seasonal float64 }

func (c *Client) Range(ctx context.Context, query string, now time.Time) (History, error) {
	if !c.Enabled() {
		return History{}, nil
	}
	u, err := url.Parse(c.base + "/api/v1/query_range")
	if err != nil {
		return History{}, err
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("start", now.Add(-28*24*time.Hour).UTC().Format(time.RFC3339))
	q.Set("end", now.UTC().Format(time.RFC3339))
	q.Set("step", "1h")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return History{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return History{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return History{}, fmt.Errorf("prometheus query_range: %s", resp.Status)
	}
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return History{}, err
	}
	var all, seasonal []float64
	for _, series := range body.Data.Result {
		for _, pair := range series.Values {
			if len(pair) != 2 {
				continue
			}
			var ts float64
			var raw string
			if json.Unmarshal(pair[0], &ts) != nil || json.Unmarshal(pair[1], &raw) != nil {
				continue
			}
			var value float64
			if _, err := fmt.Sscan(raw, &value); err != nil {
				continue
			}
			all = append(all, value)
			at := time.Unix(int64(ts), 0).UTC()
			if at.Weekday() == now.UTC().Weekday() && at.Hour() == now.UTC().Hour() {
				seasonal = append(seasonal, value)
			}
		}
	}
	return History{P95: percentile(all, .95), Seasonal: percentile(seasonal, .95)}, nil
}
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	i := int(float64(len(values)-1) * p)
	return values[i]
}
