package scanner

type Candle struct {
	Time   int64
	Close  float64
	Volume int64
}

var ScanWindows = []int{1450, 1260, 1008, 756, 504, 252, 126, 50, 20, 10}

type WindowHits struct {
	Days int
	Hits int
}

func CountConditions(candles []Candle, days int) int {
	if len(candles) < 2 {
		return 0
	}

	count := 0
	maxI := days
	if maxI > len(candles)-1 {
		maxI = len(candles) - 1
	}

	for i := 1; i <= maxI; i++ {
		today := candles[len(candles)-i]
		prev := candles[len(candles)-i-1]

		if prev.Close == 0 {
			continue
		}

		ratio := today.Close / prev.Close

		if ratio >= 1.04 && today.Volume > 9_000_000 && today.Volume > prev.Volume {
			count++
		}
	}

	return count
}

func CountConditionsForWindows(candles []Candle, days []int) []WindowHits {
	out := make([]WindowHits, 0, len(days))
	for _, d := range days {
		out = append(out, WindowHits{
			Days: d,
			Hits: CountConditions(candles, d),
		})
	}
	return out
}
