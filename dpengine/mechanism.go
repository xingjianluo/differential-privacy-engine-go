package dpengine

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// BoundMode decides how out-of-bound contributions are handled.
type BoundMode int

const (
	// BoundReject rejects queries whose values fall outside [Lower, Upper].
	BoundReject BoundMode = iota
	// BoundClamp clamps values into [Lower, Upper] before aggregation.
	BoundClamp
)

// LaplaceMechanism is a pure-DP Laplace noise mechanism with a
// deterministic, seedable random source so results are reproducible:
// the same seed always yields the same noise sequence.
type LaplaceMechanism struct {
	rng *rand.Rand
}

// NewLaplaceMechanism builds a mechanism drawing uniforms from the given
// seed. Same seed => same noise sequence.
func NewLaplaceMechanism(seed uint64) *LaplaceMechanism {
	return &LaplaceMechanism{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

// Noise returns one Laplace(0, sensitivity/epsilon) sample via inversion
// of the CDF: X = -b * sign(U) * ln(1 - 2|U|), U ~ Uniform(-1/2, 1/2).
func (m *LaplaceMechanism) Noise(sensitivity, epsilon float64) float64 {
	b := sensitivity / epsilon
	u := m.rng.Float64() - 0.5
	return -b * math.Copysign(math.Log1p(-2*math.Abs(u)), u)
}

// ClampBounds validates and (per mode) clamps a value into [lo, hi].
// NaN is always rejected as an unbounded input. With BoundReject,
// out-of-range values are rejected explicitly; with BoundClamp they are
// clamped so the L1 sensitivity of the sum stays bounded by (hi-lo).
func ClampBounds(v, lo, hi float64, mode BoundMode) (float64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, newError(ReasonUnboundedInput,
			fmt.Sprintf("value %v is not finite; provide bounded contributions", v))
	}
	if v < lo || v > hi {
		if mode == BoundReject {
			return 0, newError(ReasonUnboundedInput,
				fmt.Sprintf("value %v outside contribution bounds [%v, %v]", v, lo, hi))
		}
		return math.Min(math.Max(v, lo), hi), nil
	}
	return v, nil
}
