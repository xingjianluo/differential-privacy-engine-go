package dpengine

import (
	"fmt"
	"sync"
)

// Query describes one noised sum query against a dataset.
type Query struct {
	Dataset     string
	RequestID   string
	Epsilon     float64
	Composition Composition
	Values      []float64
	Lower       float64
	Upper       float64
	BoundMode   BoundMode
}

// Result is the outcome of an executed query.
type Result struct {
	NoisySum    float64
	ClampedRows int
	Consumption Consumption
}

// Engine couples a LaplaceMechanism with a budget Ledger. The mechanism
// is serialized with a mutex so concurrent queries draw noise in a
// deterministic order per seed.
type Engine struct {
	mech   *LaplaceMechanism
	ledger *Ledger
	mechMu sync.Mutex
}

func NewEngine(mech *LaplaceMechanism, ledger *Ledger) *Engine {
	return &Engine{mech: mech, ledger: ledger}
}

// Ledger exposes the underlying ledger (e.g. for snapshots).
func (e *Engine) Ledger() *Ledger { return e.ledger }

// Execute validates the query, charges budget, and returns the noised
// sum. Validation and bounding happen before any budget is charged, so
// rejected queries never consume budget.
func (e *Engine) Execute(q Query) (Result, error) {
	if q.BoundMode != BoundReject && q.BoundMode != BoundClamp {
		return Result{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("unknown bound mode %d", q.BoundMode))
	}
	if len(q.Values) == 0 {
		return Result{}, newError(ReasonInvalidParameter, "query has no values")
	}
	if !(q.Lower < q.Upper) {
		return Result{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("contribution bounds must satisfy lower < upper, got [%v, %v]", q.Lower, q.Upper))
	}

	// Contribution bounding: every value must be finite and inside
	// [Lower, Upper] (rejected or clamped per BoundMode).
	sum := 0.0
	clamped := 0
	for _, v := range q.Values {
		cv, err := ClampBounds(v, q.Lower, q.Upper, q.BoundMode)
		if err != nil {
			return Result{}, err
		}
		if cv != v {
			clamped++
		}
		sum += cv
	}

	// Charge budget only after all validation passed.
	c, err := e.ledger.Consume(q.Dataset, q.RequestID, q.Epsilon, q.Composition)
	if err != nil {
		return Result{}, err
	}

	// Sensitivity of a sum with per-row contribution bounded to
	// [Lower, Upper] is (Upper - Lower).
	e.mechMu.Lock()
	noise := e.mech.Noise(q.Upper-q.Lower, q.Epsilon)
	e.mechMu.Unlock()

	return Result{
		NoisySum:    sum + noise,
		ClampedRows: clamped,
		Consumption: c,
	}, nil
}
