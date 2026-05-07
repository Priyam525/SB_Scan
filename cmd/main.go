package main

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"SB/scanner"
	"SB/universe_scan"
	"SB/yahoo"
)

const (
	scanWorkers   = 80
	scanDeadline  = 15 * time.Second
	passThreshold = 3
	printTopN     = 25
)

// scanLimit: 0 = entire universe (~1000). Tuned for Yahoo latency + rate limits.
const scanLimit = 0

type outcome struct {
	symbol string
	hits   map[int]int
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
	passCounts := make(map[int]int, len(scanner.ScanWindows))

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

				windowHits := scanner.CountConditionsForWindows(buf, scanner.ScanWindows)
				hitsByDays := make(map[int]int, len(windowHits))
				for _, wh := range windowHits {
					hitsByDays[wh.Days] = wh.Hits
				}
				mu.Lock()
				outcomes = append(outcomes, outcome{symbol: symbol, hits: hitsByDays})
				for _, days := range scanner.ScanWindows {
					if hitsByDays[days] >= passThreshold {
						passCounts[days]++
					}
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
	passParts := make([]string, 0, len(scanner.ScanWindows))
	for _, d := range scanner.ScanWindows {
		passParts = append(passParts, fmt.Sprintf("pass %dd=%d", d, passCounts[d]))
	}
	log.Printf(
		"scan: %d symbols in %s (%d yahoo errors, deadline=%v, %s)",
		len(symbols),
		elapsed.Round(time.Millisecond),
		errCount,
		scanDeadline,
		strings.Join(passParts, ", "),
	)
	if ctx.Err() != nil {
		log.Printf("context: %v", ctx.Err())
	}

	csv, copiedCount := buildTopTickersCSV(outcomes, scanner.ScanWindows)
	if copiedCount == 0 {
		fmt.Println("Clipboard copy skipped: no successful ticker rows.")
		return
	}
	if err := copyToClipboard(csv); err != nil {
		fmt.Printf("Clipboard copy failed (%d tickers): %v\n", copiedCount, err)
		fmt.Printf("Tickers CSV: %s\n", csv)
		return
	}
	fmt.Printf("Copied %d unique tickers to clipboard (comma-separated).\n", copiedCount)
}

func buildTopTickersCSV(outcomes []outcome, windows []int) (string, int) {
	seen := make(map[string]struct{}, printTopN*len(windows))
	ordered := make([]string, 0, printTopN*len(windows))

	for _, days := range windows {
		ranked := rankOutcomesByWindow(outcomes, days)
		limit := len(ranked)
		if limit > printTopN {
			limit = printTopN
		}
		for _, o := range ranked[:limit] {
			if o.err != nil {
				continue
			}
			if _, ok := seen[o.symbol]; ok {
				continue
			}
			seen[o.symbol] = struct{}{}
			ordered = append(ordered, o.symbol)
		}
	}

	return strings.Join(ordered, ","), len(ordered)
}

func rankOutcomesByWindow(outcomes []outcome, days int) []outcome {
	ranked := make([]outcome, len(outcomes))
	copy(ranked, outcomes)

	sort.Slice(ranked, func(i, j int) bool {
		// Show ranked results by hit count for each window; push errors to the bottom.
		if ranked[i].err != nil && ranked[j].err == nil {
			return false
		}
		if ranked[i].err == nil && ranked[j].err != nil {
			return true
		}
		ih := ranked[i].hits[days]
		jh := ranked[j].hits[days]
		if ih != jh {
			return ih > jh
		}
		return ranked[i].symbol < ranked[j].symbol
	})
	return ranked
}

func copyToClipboard(text string) error {
	switch runtime.GOOS {
	case "windows":
		cmd := exec.Command("powershell", "-NoProfile", "-Command", "Set-Clipboard -Value @'\n"+text+"\n'@")
		return cmd.Run()
	case "darwin":
		cmd := exec.Command("pbcopy")
		cmd.Stdin = strings.NewReader(text)
		return cmd.Run()
	default:
		cmd := exec.Command("sh", "-c", "command -v wl-copy >/dev/null 2>&1 && wl-copy || xclip -selection clipboard")
		cmd.Stdin = strings.NewReader(text)
		return cmd.Run()
	}
}
