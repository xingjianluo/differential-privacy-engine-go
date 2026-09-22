package dpengine

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestBudgetAccessorAndStorePath(t *testing.T) {
	path := mustStorePath(t)
	e, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatal(err)
	}
	if e.StorePath() != path {
		t.Fatalf("store path=%q, want %q", e.StorePath(), path)
	}
	if e.LoadedFromSnapshot() {
		t.Fatal("fresh engine should not report loaded from snapshot")
	}
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: 2, Delta: 1e-5}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Count(countReq2("d", "edge-1", 0.5, 1)); err != nil {
		t.Fatal(err)
	}
	te, td, ue, ud, ok := e.Budget("d")
	t.Logf("input dataset d; total=(%v,%v) used=(%v,%v) ok=%v", te, td, ue, ud, ok)
	if !ok || te != 2 || td != 1e-5 || math.Abs(ue-0.5) > 1e-12 || ud != 0 {
		t.Fatalf("budget accessor wrong: %v %v %v %v %v", te, td, ue, ud, ok)
	}
	if _, _, _, _, ok := e.Budget("missing"); ok {
		t.Fatal("missing dataset must report ok=false")
	}
	if _, _, ok := e.Used("missing"); ok {
		t.Fatal("missing dataset must report ok=false from Used")
	}
	if _, ok := e.Events("missing"); ok {
		t.Fatal("missing dataset must report ok=false from Events")
	}
	_ = e.Close()
}

func TestErrorWrappingAndHasCode(t *testing.T) {
	base := errorf(ErrBudgetExceeded, "base")
	wrapped := fmt.Errorf("outer: %w", base)
	t.Logf("input wrapped=%v; rule: HasCode traverses Unwrap chain", wrapped)
	if !HasCode(wrapped, ErrBudgetExceeded) {
		t.Fatal("HasCode must find code through wrapping")
	}
	if HasCode(nil, ErrBudgetExceeded) {
		t.Fatal("HasCode(nil) must be false")
	}
	plain := errors.New("plain")
	if HasCode(plain, ErrBudgetExceeded) {
		t.Fatal("plain error must not match a code")
	}
	withCause := wrapError(ErrInternal, plain, "context")
	if !errors.Is(withCause, plain) {
		t.Fatal("Unwrap must expose Cause")
	}
	if !strings.Contains(withCause.Error(), "internal_error") {
		t.Fatalf("error text must include code: %v", withCause)
	}
}

func TestInvalidQueryShapes(t *testing.T) {
	e := newTestEngine(t)

	// 未知裁剪策略
	q := countReq2("users", "edge-policy", 0.1, 1)
	q.Policy = BoundPolicy(42)
	_, err := e.Count(q)
	t.Logf("input policy=42; decision: ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}

	// 空 requestID
	q = countReq2("users", "", 0.1, 1)
	_, err = e.Count(q)
	t.Logf("input empty request_id; decision: ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}

	// 空并行请求
	_, err = e.ParallelCount(ParallelCountQuery{Dataset: "users", RequestID: "p-empty"})
	t.Logf("input parallel with no children; decision: ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}

	// 并行子查询自带不同 requestID
	_, err = e.ParallelCount(ParallelCountQuery{
		Dataset:   "users",
		RequestID: "p-child-id",
		Children: []CountQuery{{
			RequestID: "own-id", Params: PrivacyParams{Epsilon: 0.1},
			MaxContributions: 1, Policy: PolicyClamp,
		}},
	})
	t.Logf("input child carrying own request_id; decision: ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}

	// 并行子查询隐私参数非法
	_, err = e.ParallelCount(ParallelCountQuery{
		Dataset: "users", RequestID: "p-bad",
		Children: []CountQuery{{
			Params: PrivacyParams{Epsilon: 0}, MaxContributions: 1, Policy: PolicyClamp,
		}},
	})
	t.Logf("input child epsilon=0; decision: ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}

	// 并行查询引用不存在数据集
	_, err = e.ParallelCount(ParallelCountQuery{
		Dataset: "ghost", RequestID: "p-missing",
		Children: []CountQuery{{
			Params: PrivacyParams{Epsilon: 0.1}, MaxContributions: 1, Policy: PolicyClamp,
		}},
	})
	t.Logf("input parallel on missing dataset; decision: ErrDatasetNotFound; got=%v", err)
	if !HasCode(err, ErrDatasetNotFound) {
		t.Fatalf("want ErrDatasetNotFound, got %v", err)
	}

	// 并行请求在裁剪拒绝路径下：不记账
	_, err = e.ParallelCount(ParallelCountQuery{
		Dataset: "users", RequestID: "p-reject",
		Children: []CountQuery{{
			Params: PrivacyParams{Epsilon: 0.1}, MaxContributions: 1, Policy: PolicyReject,
			Contributions: map[string]int{"u": 2},
		}},
	})
	t.Logf("input parallel child contribution=2 max=1 reject; decision: ErrContributionRejected, no billing; got=%v", err)
	if !HasCode(err, ErrContributionRejected) {
		t.Fatalf("want ErrContributionRejected, got %v", err)
	}
	events, _ := e.Events("users")
	if len(events) != 0 {
		t.Fatalf("rejected parallel must not bill, events=%+v", events)
	}
}

func TestExternalRandomSourceFailure(t *testing.T) {
	e, err := NewEngine(WithRandomSource(errReader{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	q := countReq2("d", "edge-bad-rng", 0.1, 1)
	_, err = e.Count(q)
	t.Logf("input failing random source; decision: ErrInternal; got=%v", err)
	if !HasCode(err, ErrInternal) {
		t.Fatalf("want ErrInternal, got %v", err)
	}
	if eps, _, _ := e.Used("d"); eps != 0 {
		t.Fatalf("rng failure must not bill, used=%v", eps)
	}
	_ = e.Close()
}

type errReader struct{}

func (errReader) Read(p []byte) (int, error) { return 0, errors.New("rng boom") }

func TestLaplaceInvalidScale(t *testing.T) {
	for _, scale := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		_, err := laplaceNoise(newPCGRand([16]byte{}), scale)
		t.Logf("input scale=%v; decision: ErrInternal; got=%v", scale, err)
		if !HasCode(err, ErrInternal) {
			t.Fatalf("scale %v: want ErrInternal, got %v", scale, err)
		}
	}
}

func TestEmptyContributionsValidZero(t *testing.T) {
	e := newTestEngine(t)
	q := countReq2("users", "edge-empty", 0.1, 1)
	q.Contributions = map[string]int{}
	res, err := e.Count(q)
	if err != nil {
		t.Fatal(err)
	}
	// 真值 0 + 确定性噪声；结果有限且可解释。
	t.Logf("input empty contributions; decision: accepted true=%d noisy=%v scale=%v",
		res.TrueValue, res.NoisyValue, res.NoiseScale)
	if res.TrueValue != 0 || math.IsNaN(res.NoisyValue) || math.IsInf(res.NoisyValue, 0) {
		t.Fatal("zero-input result must be finite")
	}
}
