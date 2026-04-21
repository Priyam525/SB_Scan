package yahoo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ChartResponse matches Yahoo v8 finance/chart JSON (see
// https://query1.finance.yahoo.com/v8/finance/chart/SYMBOL). Extra fields
// (meta, adjclose, open/high/low, etc.) are ignored by encoding/json.
type ChartResponse struct {
	Chart struct {
		Result []struct {
			Timestamp  []int64 `json:"timestamp"`
			Indicators struct {
				Quote []struct {
					Close  []float64 `json:"close"`
					Volume []int64   `json:"volume"`
				} `json:"quote"`
			} `json:"indicators"`
		} `json:"result"`
		Error json.RawMessage `json:"error"`
	} `json:"chart"`
}

type Candle struct {
	Time   int64
	Close  float64
	Volume int64
}

// SharedTransport is tuned for many concurrent requests to the same host.
var SharedTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          512,
	MaxIdleConnsPerHost:   512,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   8 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

// SharedClient has no Timeout so per-call context (e.g. a global deadline) controls cancellation.
var SharedClient = &http.Client{
	Transport: SharedTransport,
}

type candleCacheEntry struct {
	candles    []Candle
	expiresAt  time.Time
	fetching   bool
	fetchDone  chan struct{}
}

var (
	cacheMu      sync.Mutex
	candleBySym  = make(map[string]*candleCacheEntry)
)

func FetchDaily(symbol string) ([]Candle, error) {
	return FetchDailyWithClient(context.Background(), SharedClient, symbol)
}

// FetchDailyWithClient loads daily candles for symbol. ctx should carry deadlines when scanning many symbols.
func FetchDailyWithClient(ctx context.Context, client *http.Client, symbol string) ([]Candle, error) {
	cacheKey := strings.ToUpper(strings.TrimSpace(symbol))
	if cacheKey == "" {
		return nil, fmt.Errorf("empty symbol")
	}

	for {
		cacheMu.Lock()
		entry := candleBySym[cacheKey]
		if entry == nil {
			entry = &candleCacheEntry{}
			candleBySym[cacheKey] = entry
		}

		if time.Now().Before(entry.expiresAt) && len(entry.candles) > 0 {
			cached := cloneCandles(entry.candles)
			cacheMu.Unlock()
			return cached, nil
		}

		if !entry.fetching {
			entry.fetching = true
			entry.fetchDone = make(chan struct{})
			cacheMu.Unlock()
			break
		}

		waitCh := entry.fetchDone
		cacheMu.Unlock()
		select {
		case <-waitCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	candles, err := fetchDailyFromYahoo(ctx, client, symbol)

	cacheMu.Lock()
	entry := candleBySym[cacheKey]
	if err == nil {
		entry.candles = cloneCandles(candles)
		entry.expiresAt = time.Now().Add(nextCacheTTL())
	}
	entry.fetching = false
	close(entry.fetchDone)
	cacheMu.Unlock()

	if err != nil {
		return nil, err
	}
	return candles, nil
}

func fetchDailyFromYahoo(ctx context.Context, client *http.Client, symbol string) ([]Candle, error) {
	if client == nil {
		client = SharedClient
	}

	url := fmt.Sprintf(
		"https://query1.finance.yahoo.com/v8/finance/chart/%s?range=1mo&interval=1d",
		symbol,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json,text/plain,*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		preview := truncateString(body, 200)
		return nil, fmt.Errorf("yahoo %s: http %d: %q", symbol, resp.StatusCode, preview)
	}

	var data ChartResponse
	if err := json.Unmarshal(body, &data); err != nil {
		preview := truncateString(body, 200)
		return nil, fmt.Errorf("decode chart for %s: %w (body prefix %q)", symbol, err, preview)
	}

	if len(data.Chart.Error) > 0 && string(data.Chart.Error) != "null" {
		return nil, fmt.Errorf("yahoo chart error for %s: %s", symbol, string(data.Chart.Error))
	}

	if len(data.Chart.Result) == 0 {
		return nil, fmt.Errorf("no data for %s", symbol)
	}

	result := data.Chart.Result[0]
	timestamps := result.Timestamp
	quotes := result.Indicators.Quote
	if len(quotes) == 0 {
		return nil, fmt.Errorf("no quote series for %s", symbol)
	}

	closes := quotes[0].Close
	volumes := quotes[0].Volume
	if len(timestamps) != len(closes) || len(timestamps) != len(volumes) {
		return nil, fmt.Errorf("mismatched series lengths for %s", symbol)
	}

	candles := make([]Candle, 0, len(timestamps))
	for i := range timestamps {
		if closes[i] == 0 || volumes[i] == 0 {
			continue
		}
		candles = append(candles, Candle{
			Time:   timestamps[i],
			Close:  closes[i],
			Volume: volumes[i],
		})
	}
	return candles, nil
}

func truncateString(b []byte, max int) string {
	s := string(b)
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func nextCacheTTL() time.Duration {
	// Jitter cache TTL between 15 and 20 minutes.
	return time.Duration(15+time.Now().UnixNano()%6) * time.Minute
}

func cloneCandles(in []Candle) []Candle {
	out := make([]Candle, len(in))
	copy(out, in)
	return out
}
