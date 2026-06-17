package gateway

// admission.go ports src/services/router_service/router/admission.py:
// binary search for the largest admit count whose predicted TPOT stays within
// budget.

func computeMaxSafeAdmit(lp latencyPredictor, currentInflight int, tpotBudgetS float64, accumulatedLenAvg int) int {
	const maxSearch = 128
	if tpotBudgetS <= 0 {
		return 1
	}

	lo, hi := 1, maxSearch
	best := 1
	for lo <= hi {
		mid := (lo + hi) / 2
		predicted := lp.predictTPOT(currentInflight+mid, maxInt(1, accumulatedLenAvg))
		if predicted <= tpotBudgetS {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return maxInt(1, best)
}
