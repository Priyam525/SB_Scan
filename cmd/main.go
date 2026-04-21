package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"SB/scanner"
	"SB/universe_scan"
	"SB/yahoo"
)

const (
	scanWorkers   = 80
	scanDeadline  = 15 * time.Second
	conditionDays = 20
	passThreshold = 5
)

// scanLimit: 0 = entire universe (~1000). Tuned for Yahoo latency + rate limits.
const scanLimit = 0

type outcome struct {
	symbol string
	hits   int
	err    error
}

func main() {
	start := time.Now()
	t0 := time.Now()
	fmt.Println("Fetching tickers...")
	tickers, err := universe_scan.FetchTickers()
	if err != nil {
		log.Fatal(err)
	}

	n := len(tickers)
	if scanLimit > 0 && scanLimit < n {
		n = scanLimit
	}
	symbols := tickers[:n]
	fmt.Println("Tickers fetched:", time.Since(t0))
	fmt.Println("Fetching candles...")
	fmt.Printf("=== Universe (%d tickers) ===\n", len(symbols))

	ctx, cancel := context.WithTimeout(context.Background(), scanDeadline)
	defer cancel()

	jobs := make(chan string, len(symbols))

	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := make([]outcome, 0, len(symbols))
	var errCount int
	var passCount int

	for range scanWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := yahoo.SharedClient
			buf := make([]scanner.Candle, 0, 32)

			for symbol := range jobs {
				if ctx.Err() != nil {
					mu.Lock()
					outcomes = append(outcomes, outcome{symbol: symbol, err: ctx.Err()})
					mu.Unlock()
					continue
				}

				candles, fetchErr := yahoo.FetchDailyWithClient(ctx, client, symbol)
				if fetchErr != nil {
					mu.Lock()
					outcomes = append(outcomes, outcome{symbol: symbol, err: fetchErr})
					errCount++
					mu.Unlock()
					continue
				}

				buf = buf[:0]
				for i := range candles {
					buf = append(buf, scanner.Candle{
						Time:   candles[i].Time,
						Close:  candles[i].Close,
						Volume: candles[i].Volume,
					})
				}

				hits := scanner.CountConditions(buf, conditionDays)
				mu.Lock()
				outcomes = append(outcomes, outcome{symbol: symbol, hits: hits})
				if hits >= passThreshold {
					passCount++
				}
				mu.Unlock()
			}
		}()
	}

	for _, s := range symbols {
		jobs <- s
	}
	close(jobs)

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf(
		"scan: %d symbols in %s (%d pass, %d yahoo errors, deadline=%v)",
		len(symbols), elapsed.Round(time.Millisecond), passCount, errCount, scanDeadline,
	)
	if ctx.Err() != nil {
		log.Printf("context: %v", ctx.Err())
	}

	sort.Slice(outcomes, func(i, j int) bool {
		// Show ranked results by hit count; push errors to the bottom.
		if outcomes[i].err != nil && outcomes[j].err == nil {
			return false
		}
		if outcomes[i].err == nil && outcomes[j].err != nil {
			return true
		}
		if outcomes[i].hits != outcomes[j].hits {
			return outcomes[i].hits > outcomes[j].hits
		}
		return outcomes[i].symbol < outcomes[j].symbol
	})

	printN := len(outcomes)
	if printN > 25 {
		printN = 25
	}

	fmt.Printf("=== Scan results (top %d by hits) ===\n", printN)
	fmt.Printf("%-8s %5s  %s\n", "symbol", "hits", "status")
	for _, o := range outcomes[:printN] {
		switch {
		case o.err != nil:
			fmt.Printf("%-8s %5s  ERR %v\n", o.symbol, "—", o.err)
		case o.hits >= passThreshold:
			fmt.Printf("%-8s %5d  PASS\n", o.symbol, o.hits)
		default:
			fmt.Printf("%-8s %5d  —\n", o.symbol, o.hits)
		}
	}
}
