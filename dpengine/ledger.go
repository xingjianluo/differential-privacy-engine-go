package dpengine

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Composition describes how the privacy cost of a query is combined with
// previously consumed budget of the same dataset.
type Composition int

const (
	// SequentialComposition adds the query epsilon to the spent total
	// (standard sequential composition of pure DP).
	SequentialComposition Composition = iota
	// ParallelComposition charges only max(spent, epsilon), modelling
	// parallel composition over disjoint partitions.
	ParallelComposition
)

// Dataset is the privacy-budget account of one dataset.
type Dataset struct {
	Name          string
	TotalEpsilon  float64
	SpentEpsilon  float64
	ConsumedUnits []Consumption
}

// Consumption is one immutable, auditable budget-consumption record.
type Consumption struct {
	RequestID   string
	Epsilon     float64
	Composition Composition
	SpentAfter  float64
}

// Ledger tracks budgets of all datasets. It is safe for concurrent use:
// the whole check-and-charge operation happens under one mutex, so
// concurrent consumers can never overspend and duplicate request IDs are
// never charged twice.
type Ledger struct {
	mu       sync.Mutex
	datasets map[string]*Dataset
	// processed maps requestID -> recorded consumption for idempotent
	// replay: a repeated request returns its original record and is
	// never charged twice.
	processed map[string]Consumption
	// reservations maps batchID -> batch hold (all lifecycle states).
	reservations map[string]*Reservation
	// now is the ledger clock; overridable in tests for deterministic
	// expiry semantics.
	now func() time.Time
}

func NewLedger() *Ledger {
	return &Ledger{
		datasets:     make(map[string]*Dataset),
		processed:    make(map[string]Consumption),
		reservations: make(map[string]*Reservation),
		now:          time.Now,
	}
}

// withClock returns a ledger using the given clock (tests only).
func withClock(now func() time.Time) *Ledger {
	l := NewLedger()
	l.now = now
	return l
}

// reservedOf returns the total epsilon currently held by pending
// (not yet settled and not reclaimed) reservations of a dataset. Caller
// must hold l.mu.
func (l *Ledger) reservedOf(ds *Dataset) float64 {
	var sum float64
	for _, r := range l.reservations {
		if r.Dataset == ds.Name && r.Status == ReservationPending {
			sum += r.Amount
		}
	}
	return sum
}

// AddDataset registers a dataset with a total epsilon budget.
func (l *Ledger) AddDataset(name string, totalEpsilon float64) error {
	if name == "" {
		return newError(ReasonInvalidParameter, "dataset name must not be empty")
	}
	if totalEpsilon <= 0 || math.IsNaN(totalEpsilon) || math.IsInf(totalEpsilon, 0) {
		return newError(ReasonInvalidParameter,
			fmt.Sprintf("total epsilon must be a finite positive number, got %v", totalEpsilon))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.datasets[name]; exists {
		return newError(ReasonInvalidParameter, "dataset already exists: "+name)
	}
	l.datasets[name] = &Dataset{Name: name, TotalEpsilon: totalEpsilon}
	return nil
}

// Consume attempts to charge epsilon against the dataset budget under the
// given composition rule. It is idempotent per requestID: replaying a
// requestID returns the originally recorded consumption without charging
// again. Consumption is monotonic: SpentEpsilon never decreases.
func (l *Ledger) Consume(dataset, requestID string, epsilon float64, comp Composition) (Consumption, error) {
	if requestID == "" {
		return Consumption{}, newError(ReasonInvalidParameter, "request id must not be empty")
	}
	if epsilon <= 0 || math.IsNaN(epsilon) || math.IsInf(epsilon, 0) {
		return Consumption{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("epsilon must be a finite positive number, got %v", epsilon))
	}
	if comp != SequentialComposition && comp != ParallelComposition {
		return Consumption{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("unknown composition rule %d", comp))
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Replay: same requestID returns the original record, no double charge.
	if prev, ok := l.processed[requestID]; ok {
		return prev, nil
	}

	ds, ok := l.datasets[dataset]
	if !ok {
		return Consumption{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}

	var newSpent float64
	switch comp {
	case SequentialComposition:
		newSpent = ds.SpentEpsilon + epsilon
	case ParallelComposition:
		newSpent = math.Max(ds.SpentEpsilon, epsilon)
	}
	// Pending reservations are already unavailable, so a direct consume
	// can never push the (spent + held) total beyond the budget.
	reserved := l.reservedOf(ds)
	if newSpent+reserved > ds.TotalEpsilon {
		return Consumption{}, newError(ReasonBudgetExhausted,
			fmt.Sprintf("dataset %q: need spent=%.6f reserved=%.6f but budget=%.6f (already spent %.6f)",
				dataset, newSpent, reserved, ds.TotalEpsilon, ds.SpentEpsilon))
	}

	c := Consumption{
		RequestID:   requestID,
		Epsilon:     epsilon,
		Composition: comp,
		SpentAfter:  newSpent,
	}
	ds.SpentEpsilon = newSpent
	ds.ConsumedUnits = append(ds.ConsumedUnits, c)
	l.processed[requestID] = c
	return c, nil
}

// Spent returns the spent and total epsilon of a dataset.
func (l *Ledger) Spent(dataset string) (spent, total float64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ds, ok := l.datasets[dataset]
	if !ok {
		return 0, 0, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}
	return ds.SpentEpsilon, ds.TotalEpsilon, nil
}
