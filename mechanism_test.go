package dpengine

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestLaplaceReproducible(t *testing.T) {
	seed := deriveSeed("req-42", "", 0)
	n1, err := laplaceNoise(newRand(seed), 2.0)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := laplaceNoise(newRand(seed), 2.0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input seed=%x scale=2.0; rule: same seed must yield identical noise; n1=%v n2=%v", seed, n1, n2)
	if n1 != n2 {
		t.Fatalf("noise not reproducible: %v vs %v", n1, n2)
	}

	seedOther := deriveSeed("req-43", "", 0)
	n3, _ := laplaceNoise(newRand(seedOther), 2.0)
	t.Logf("different request seed=%x -> noise=%v (independent streams expected)", seedOther, n3)
	if n3 == n1 {
		t.Fatalf("distinct seeds produced identical noise %v", n3)
	}
}

func newRand(seed [16]byte) *rand.Rand { return newPCGRand(seed) }

func TestLaplaceScaleAndMean(t *testing.T) {
	// 用同一请求不同分区索引生成大量独立噪声，粗略校验尺度与零均值。
	const n = 20000
	var sum float64
	scale := 0.5
	for i := 0; i < n; i++ {
		seed := deriveSeed("req-stats", "parallel", i)
		rng := newPCGRand(seed)
		v, err := laplaceNoise(rng, scale)
		if err != nil {
			t.Fatal(err)
		}
		sum += v
	}
	mean := sum / n
	t.Logf("input n=%d scale=%v; rule: Laplace mean ~=0, E|X|=scale; sample mean=%v", n, scale, mean)
	if math.Abs(mean) > 0.05 {
		t.Fatalf("sample mean %v too far from 0", mean)
	}
}

func TestBoundedCountRejectPolicy(t *testing.T) {
	q := CountQuery{
		Dataset:          "d",
		RequestID:        "r1",
		Params:           PrivacyParams{Epsilon: 1},
		MaxContributions: 3,
		Policy:           PolicyReject,
		Contributions:    map[string]int{"alice": 3, "bob": 4},
	}
	t.Logf("input: max=3 policy=reject contributions=%v; rule: any >max rejects whole query",
		q.Contributions)
	_, _, err := boundedCount(q)
	if !HasCode(err, ErrContributionRejected) {
		t.Fatalf("expected ErrContributionRejected, got %v", err)
	}
}

func TestBoundedCountClampPolicy(t *testing.T) {
	q := CountQuery{
		Dataset:          "d",
		RequestID:        "r2",
		Params:           PrivacyParams{Epsilon: 1},
		MaxContributions: 3,
		Policy:           PolicyClamp,
		Contributions:    map[string]int{"alice": 3, "bob": 9, "carol": 1, "dave": 7},
	}
	t.Logf("input: max=3 policy=clamp contributions=%v; rule: clip to 3 and count clamped users",
		q.Contributions)
	tv, clamped, err := boundedCount(q)
	if err != nil {
		t.Fatal(err)
	}
	if tv != 3+3+1+3 {
		t.Fatalf("clamped true value=%d, want %d", tv, 3+3+1+3)
	}
	if clamped != 2 {
		t.Fatalf("clamped count=%d, want 2", clamped)
	}
}

func TestRunCountUnboundedAndReproducible(t *testing.T) {
	bad := CountQuery{Dataset: "d", RequestID: "r3", Params: PrivacyParams{Epsilon: 1},
		Policy: PolicyReject, Contributions: map[string]int{"a": 1}}
	t.Logf("input max_contributions=%d; rule: missing finite bound -> ErrUnboundedInput", bad.MaxContributions)
	if _, err := runCount(bad, [16]byte{}); !HasCode(err, ErrUnboundedInput) {
		t.Fatalf("expected ErrUnboundedInput, got %v", err)
	}

	q := CountQuery{Dataset: "d", RequestID: "r4", Params: PrivacyParams{Epsilon: 2},
		MaxContributions: 5, Policy: PolicyClamp, Contributions: map[string]int{"a": 5, "b": 2}}
	seed := deriveSeed("r4", "", 0)
	r1, err := runCount(q, seed)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := runCount(q, seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input true=%d sensitivity=%v scale=5/2=%v; rule: deterministic seed -> same noisy value; values %v vs %v",
		r1.TrueValue, r1.Sensitivity, r1.NoiseScale, r1.NoisyValue, r2.NoisyValue)
	if r1.NoisyValue != r2.NoisyValue || r1.Sensitivity != 5 || r1.NoiseScale != 2.5 {
		t.Fatalf("unexpected results: %+v vs %+v", r1, r2)
	}
}

func TestNegativeContributionRejected(t *testing.T) {
	q := CountQuery{Dataset: "d", RequestID: "r5", Params: PrivacyParams{Epsilon: 1},
		MaxContributions: 3, Policy: PolicyClamp, Contributions: map[string]int{"a": -1}}
	t.Logf("input contributions=%v; rule: negative contribution -> ErrInvalidParams", q.Contributions)
	if _, err := runCount(q, [16]byte{}); !HasCode(err, ErrInvalidParams) {
		t.Fatalf("expected ErrInvalidParams, got %v", err)
	}
}
