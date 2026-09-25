// Package carbon reads how clean the electricity grid is right now, so
// the scheduler can decide whether a flexible job should run or wait.
package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// defaultBaseURL points at the UK Carbon Intensity API: free, no key
// required. It's hardcoded rather than accepted as configuration, so a
// job submitter can never redirect this outbound call to an arbitrary
// host (the scheduler's only other outbound calls are the jobs
// themselves, which are restricted to the URL the caller supplied and
// validated as HTTPS at submission time).
const defaultBaseURL = "https://api.carbonintensity.org.uk"

// Index is the qualitative carbon intensity band the API reports,
// from cleanest to dirtiest.
type Index string

const (
	VeryLow  Index = "very low"
	Low      Index = "low"
	Moderate Index = "moderate"
	High     Index = "high"
	VeryHigh Index = "very high"
)

// rank orders bands from cleanest (0) to dirtiest (4) for threshold
// comparisons. An unrecognized band ranks as dirtiest, so an unexpected
// API response fails closed (jobs wait) rather than open (jobs run).
var rank = map[Index]int{
	VeryLow:  0,
	Low:      1,
	Moderate: 2,
	High:     3,
	VeryHigh: 4,
}

func (i Index) rank() int {
	if r, ok := rank[i]; ok {
		return r
	}
	return len(rank)
}

// CleanEnough reports whether i is at least as clean as threshold.
func (i Index) CleanEnough(threshold Index) bool {
	return i.rank() <= threshold.rank()
}

// Source is the read the scheduler depends on. *Client satisfies it;
// tests substitute a fake so scheduling logic never needs the network.
type Source interface {
	Current(ctx context.Context) (Index, error)
}

// Client fetches carbon intensity over HTTP.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a Client with a bounded request timeout, so a slow
// or unreachable API can never hang the scheduling loop.
func NewClient() *Client {
	return &Client{
		baseURL: defaultBaseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

type intensityResponse struct {
	Data []struct {
		Intensity struct {
			Forecast int   `json:"forecast"`
			Actual   int   `json:"actual"`
			Index    Index `json:"index"`
		} `json:"intensity"`
	} `json:"data"`
}

// Current fetches the current national carbon intensity band.
func (c *Client) Current(ctx context.Context) (Index, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/intensity", nil)
	if err != nil {
		return "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("carbon: unexpected status %d", resp.StatusCode)
	}

	var parsed intensityResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("carbon: decoding response: %w", err)
	}
	if len(parsed.Data) == 0 {
		return "", fmt.Errorf("carbon: empty response")
	}

	return parsed.Data[0].Intensity.Index, nil
}
