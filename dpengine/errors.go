// Package dpengine implements a local differential-privacy query engine
// with a persistent privacy-budget ledger.
package dpengine

// Reason classifies why a request was rejected so callers can
// programmatically distinguish failure causes.
type Reason string

const (
	ReasonDatasetNotFound  Reason = "dataset_not_found"
	ReasonInvalidParameter Reason = "invalid_parameter"
	ReasonBudgetExhausted  Reason = "budget_exhausted"
	ReasonUnboundedInput   Reason = "unbounded_input"
	ReasonVersionMismatch  Reason = "version_incompatible"
	ReasonCorruptSnapshot  Reason = "corrupt_snapshot"
	ReasonBatchNotFound    Reason = "batch_not_found"
	ReasonInvalidState     Reason = "invalid_state"
	// ReasonReservationExpired: the batch's hold deadline (ExpiresAt) has
	// passed. The deadline alone decides this: whether or not an explicit
	// expiry sweep happened to run is irrelevant.
	ReasonReservationExpired Reason = "reservation_expired"
)

// Error is the single error type returned by the engine. Reason is always
// set so errors can be classified without string matching.
type Error struct {
	Reason  Reason
	Message string
}

func (e *Error) Error() string { return string(e.Reason) + ": " + e.Message }

func newError(r Reason, msg string) *Error { return &Error{Reason: r, Message: msg} }
