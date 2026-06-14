package universe_scan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const scannerURL = "https://scanner.tradingview.com/america/scan?label-product=screener-stock"

const pageSize = 200
const pageWorkers = 6

var (
	cacheMu        sync.Mutex
	cacheCond      = sync.NewCond(&cacheMu)
	cachedTickers  []string
	cacheExpiresAt time.Time
	cacheFetching  bool
)

type scanResponse struct {
	TotalCount int `json:"totalCount"`
	Data       []struct {
		Symbol string `json:"s"`
	} `json:"data"`
}

type scanPayload struct {
	Columns             []string       `json:"columns"`
	Filter              []filterExpr   `json:"filter"`
	IgnoreUnknownFields bool           `json:"ignore_unknown_fields"`
	Options             map[string]any `json:"options"`
	Range               [2]int         `json:"range"`
	Sort                map[string]any `json:"sort"`
	Symbols             map[string]any `json:"symbols"`
	Markets             []string       `json:"markets"`
	Filter2             map[string]any `json:"filter2"`
}

type filterExpr struct {
	Left      string `json:"left"`
	Operation string `json:"operation"`
	Right     any    `json:"right"`
}

// FetchTickers returns tickers from the TradingView scanner API.
func FetchTickers() ([]string, error) {
	cacheMu.Lock()
	for {
		if time.Now().Before(cacheExpiresAt) && len(cachedTickers) > 0 {
			out := append([]string(nil), cachedTickers...)
			cacheMu.Unlock()
			return out, nil
		}
		if !cacheFetching {
			cacheFetching = true
			break
		}
		cacheCond.Wait()
	}
	cacheMu.Unlock()

	out, err := fetchTickersFromAPI()

	cacheMu.Lock()
	defer cacheMu.Unlock()
	cacheFetching = false
	defer cacheCond.Broadcast()

	if err != nil {
		return nil, err
	}

	cachedTickers = append(cachedTickers[:0], out...)
	cacheExpiresAt = time.Now().Add(nextCacheTTL())
	return append([]string(nil), cachedTickers...), nil
}

func fetchTickersFromAPI() ([]string, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	firstPage, err := fetchPage(client, 0, pageSize-1)
	if err != nil {
		return nil, err
	}

	firstTickers := make([]string, 0, len(firstPage.Data))
	for _, row := range firstPage.Data {
		firstTickers = append(firstTickers, stripExchangePrefix(row.Symbol))
	}

	totalCount := firstPage.TotalCount
	if totalCount <= len(firstTickers) || len(firstPage.Data) == 0 {
		return firstTickers, nil
	}

	lastStart := totalCount - 1
	lastStart -= lastStart % pageSize
	starts := make([]int, 0, lastStart/pageSize)
	for start := pageSize; start <= lastStart; start += pageSize {
		starts = append(starts, start)
	}

	type pageResult struct {
		start   int
		tickers []string
	}

	jobs := make(chan int, len(starts))
	results := make(chan pageResult, len(starts))
	errCh := make(chan error, 1)
	done := make(chan struct{})
	var once sync.Once
	reportErr := func(e error) {
		once.Do(func() {
			errCh <- e
			close(done)
		})
	}

	var wg sync.WaitGroup
	for range pageWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case start, ok := <-jobs:
					if !ok {
						return
					}
					resp, fetchErr := fetchPage(client, start, start+pageSize-1)
					if fetchErr != nil {
						reportErr(fetchErr)
						return
					}
					tickers := make([]string, 0, len(resp.Data))
					for _, row := range resp.Data {
						tickers = append(tickers, stripExchangePrefix(row.Symbol))
					}
					select {
					case <-done:
						return
					case results <- pageResult{start: start, tickers: tickers}:
					}
				}
			}
		}()
	}

	for _, start := range starts {
		jobs <- start
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	pages := make(map[int][]string, len(starts))
	for r := range results {
		pages[r.start] = r.tickers
	}

	select {
	case fetchErr := <-errCh:
		return nil, fetchErr
	default:
	}

	out := make([]string, 0, totalCount)
	out = append(out, firstTickers...)
	for _, start := range starts {
		out = append(out, pages[start]...)
	}
	if len(out) > totalCount {
		out = out[:totalCount]
	}
	return out, nil
}

func nextCacheTTL() time.Duration {
	// Jitter cache TTL between 15 and 20 minutes.
	return time.Duration(15+time.Now().UnixNano()%6) * time.Minute
}

func fetchPage(client *http.Client, start, end int) (*scanResponse, error) {
	payload, err := json.Marshal(buildPayload(start, end))
	if err != nil {
		return nil, fmt.Errorf("marshal scanner payload: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, scannerURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create scanner request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute scanner request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("scanner request failed: status %s", res.Status)
	}

	var parsed scanResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode scanner response: %w", err)
	}
	return &parsed, nil
}

func buildPayload(start, end int) scanPayload {
	return scanPayload{
		Columns: []string{
			"ticker-view",
			"close",
			"type",
			"typespecs",
			"pricescale",
			"minmov",
			"fractional",
			"minmove2",
			"currency",
			"change",
			"volume",
			"relative_volume_10d_calc",
			"market_cap_basic",
			"fundamental_currency_code",
			"price_earnings_ttm",
			"earnings_per_share_diluted_ttm",
			"earnings_per_share_diluted_yoy_growth_ttm",
			"dividends_yield_current",
			"sector.tr",
			"market",
			"sector",
			"AnalystRating",
			"AnalystRating.tr",
		},
		Filter: []filterExpr{
			{Left: "close", Operation: "egreater", Right: 5},
			{Left: "volume", Operation: "greater", Right: 100000},
		},
		IgnoreUnknownFields: false,
		Options:             map[string]any{"lang": "en"},
		Range:               [2]int{start, end},
		Sort: map[string]any{
			"sortBy":    "market_cap_basic",
			"sortOrder": "desc",
		},
		Symbols: map[string]any{},
		Markets: []string{"america"},
		Filter2: map[string]any{
			"operator": "and",
			"operands": []any{
				map[string]any{
					"operation": map[string]any{
						"operator": "or",
						"operands": []any{
							map[string]any{
								"operation": map[string]any{
									"operator": "and",
									"operands": []any{
										map[string]any{
											"expression": map[string]any{"left": "type", "operation": "equal", "right": "stock"},
										},
										map[string]any{
											"expression": map[string]any{"left": "typespecs", "operation": "has", "right": []string{"common"}},
										},
									},
								},
							},
							map[string]any{
								"operation": map[string]any{
									"operator": "and",
									"operands": []any{
										map[string]any{
											"expression": map[string]any{"left": "type", "operation": "equal", "right": "stock"},
										},
										map[string]any{
											"expression": map[string]any{"left": "typespecs", "operation": "has", "right": []string{"preferred"}},
										},
									},
								},
							},
							map[string]any{
								"operation": map[string]any{
									"operator": "and",
									"operands": []any{
										map[string]any{
											"expression": map[string]any{"left": "type", "operation": "equal", "right": "dr"},
										},
									},
								},
							},
							map[string]any{
								"operation": map[string]any{
									"operator": "and",
									"operands": []any{
										map[string]any{
											"expression": map[string]any{"left": "type", "operation": "equal", "right": "fund"},
										},
										map[string]any{
											"expression": map[string]any{"left": "typespecs", "operation": "has_none_of", "right": []string{"etf", "mutual", "closedend"}},
										},
									},
								},
							},
						},
					},
				},
				map[string]any{
					"expression": map[string]any{"left": "typespecs", "operation": "has_none_of", "right": []string{"pre-ipo"}},
				},
			},
		},
	}
}

func stripExchangePrefix(full string) string {
	if _, ticker, ok := strings.Cut(full, ":"); ok {
		return ticker
	}
	return full
}
