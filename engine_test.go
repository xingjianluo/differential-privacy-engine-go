package dpengine

import (
	"errors"
	"math"
	"path/filepath"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := NewEngine()
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.RegisterDataset("users", PrivacyParams{Epsilon: 1.0, Delta: 1e-6}); err != nil {
		t.Fatalf("register: %v", err)
	}
	return e
}

func countReq(id string, eps float64, maxC int) CountQuery {
	return CountQuery{
		Dataset:          "users",
		RequestID:        id,
		Params:           PrivacyParams{Epsilon: eps, Delta: 0},
		MaxContributions: maxC,
		Policy:           PolicyClamp,
		Contributions:    map[string]int{"alice": maxC, "bob": 1},
	}
}

func TestEngineCountSuccessAndAudit(t *testing.T) {
	e := newTestEngine(t)
	res, err := e.Count(countReq("q1", 0.5, 3))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input request=q1 eps=0.5 true=%d; decision: accepted; noisy=%v scale=3/0.5=%v",
		res.TrueValue, res.NoisyValue, res.NoiseScale)
	if res.NoiseScale != 6.0 || res.Sensitivity != 3 {
		t.Fatalf("unexpected mechanism params: %+v", res)
	}
	eps, delta, ok := e.Used("users")
	if !ok || math.Abs(eps-0.5) > 1e-12 || delta != 0 {
		t.Fatalf("used wrong: eps=%v delta=%v ok=%v", eps, delta, ok)
	}
	events, _ := e.Events("users")
	if len(events) != 1 || events[0].RequestID != "q1" || events[0].Kind != ChargeSequential {
		t.Fatalf("audit events wrong: %+v", events)
	}
}

func TestEngineErrorClassification(t *testing.T) {
	e := newTestEngine(t)

	// 数据集不存在
	_, err := e.Count(func() CountQuery {
		q := countReq("q-missing", 0.1, 1)
		q.Dataset = "ghost"
		return q
	}())
	t.Logf("input dataset=ghost; decision: reject with ErrDatasetNotFound; got=%v", err)
	if !HasCode(err, ErrDatasetNotFound) {
		t.Fatalf("want ErrDatasetNotFound, got %v", err)
	}

	// 参数非法（epsilon 非法）
	bad := countReq("q-bad", 0, 1)
	bad.Params.Epsilon = -1
	_, err = e.Count(bad)
	t.Logf("input epsilon=-1; decision: reject with ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}

	// 无界输入（缺少贡献上界）
	unbounded := countReq("q-unbounded", 0.1, 0)
	_, err = e.Count(unbounded)
	t.Logf("input max_contributions=0; decision: reject with ErrUnboundedInput; got=%v", err)
	if !HasCode(err, ErrUnboundedInput) {
		t.Fatalf("want ErrUnboundedInput, got %v", err)
	}

	// 注册非法预算数据集
	err = e.RegisterDataset("bad-budget", PrivacyParams{Epsilon: 0})
	t.Logf("input register eps=0; decision: reject with ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}
}

func TestEngineBudgetExhaustionBoundary(t *testing.T) {
	e := newTestEngine(t)

	// 0.4 + 0.4 + 0.2 = 1.0：三次都应放行（最后一次恰好打满）。
	for i, id := range []string{"b1", "b2", "b3"} {
		res, err := e.Count(countReq(id, []float64{0.4, 0.4, 0.2}[i], 1))
		if err != nil {
			t.Fatalf("request %s should fit, got %v", id, err)
		}
		eps, _, _ := e.Used("users")
		t.Logf("input request=%s eps=%v; decision: accepted; used after=%v noisy=%v",
			id, res.NoiseScale, eps, res.NoisyValue)
	}
	eps, _, _ := e.Used("users")
	if math.Abs(eps-1.0) > 1e-12 {
		t.Fatalf("used=%v, want exactly 1.0", eps)
	}

	// 预算已打满：再小的正数请求也必须被拒绝，且不记账。
	_, err := e.Count(countReq("b4", 1e-6, 1))
	t.Logf("input request=b4 eps=1e-6 against exhausted budget; decision: reject ErrBudgetExceeded; got=%v", err)
	if !HasCode(err, ErrBudgetExceeded) {
		t.Fatalf("want ErrBudgetExceeded, got %v", err)
	}
	eps2, _, _ := e.Used("users")
	if math.Abs(eps2-1.0) > 1e-12 {
		t.Fatalf("rejected request must not be billed, used=%v", eps2)
	}
	events, _ := e.Events("users")
	if len(events) != 3 {
		t.Fatalf("rejected request must not append event, events=%d", len(events))
	}
}

func TestEngineIdempotentDuplicate(t *testing.T) {
	e := newTestEngine(t)
	r1, err := e.Count(countReq("dup", 0.3, 2))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e.Count(countReq("dup", 0.3, 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input same request_id=dup twice; rule: second is replay, same noisy value and no double billing; %v vs %v",
		r1.NoisyValue, r2.NoisyValue)
	if r1.NoisyValue != r2.NoisyValue {
		t.Fatalf("duplicate returned different result: %v vs %v", r1.NoisyValue, r2.NoisyValue)
	}
	eps, _, _ := e.Used("users")
	if math.Abs(eps-0.3) > 1e-12 {
		t.Fatalf("duplicate was billed twice, used=%v", eps)
	}

	// 同一 requestID 挪用到别的数据集：明确拒绝。
	q := countReq("dup", 0.1, 1)
	q.Dataset = "other"
	_ = e.RegisterDataset("other", PrivacyParams{Epsilon: 1})
	_, err = e.Count(q)
	t.Logf("input reuse request_id=dup on dataset=other; decision: reject ErrInvalidParams; got=%v", err)
	if !HasCode(err, ErrInvalidParams) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}
}

func TestEngineRejectPolicyDoesNotBill(t *testing.T) {
	e := newTestEngine(t)
	q := countReq("rej", 0.3, 2)
	q.Policy = PolicyReject
	q.Contributions = map[string]int{"alice": 5}
	_, err := e.Count(q)
	t.Logf("input contribution=5 max=2 policy=reject; decision: ErrContributionRejected and no billing; got=%v", err)
	if !HasCode(err, ErrContributionRejected) {
		t.Fatalf("want ErrContributionRejected, got %v", err)
	}
	if eps, _, ok := e.Used("users"); !ok || eps != 0 {
		t.Fatalf("rejected query must not consume budget, used=%v", eps)
	}
}

func TestEngineClampPathAudit(t *testing.T) {
	e := newTestEngine(t)
	q := countReq("clamp", 0.3, 2)
	q.Policy = PolicyClamp
	q.Contributions = map[string]int{"alice": 9, "bob": 1}
	res, err := e.Count(q)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input contributions alice=9 bob=1 max=2 policy=clamp; decision: accepted after clip, true=%d clamped=%d noisy=%v",
		res.TrueValue, res.ClampedContributions, res.NoisyValue)
	if res.TrueValue != 3 || res.ClampedContributions != 1 {
		t.Fatalf("clamp result wrong: %+v", res)
	}
}

func TestEngineParallelComposition(t *testing.T) {
	e := newTestEngine(t)
	pq := ParallelCountQuery{
		Dataset:   "users",
		RequestID: "par1",
		Children: []CountQuery{
			{Params: PrivacyParams{Epsilon: 0.3}, MaxContributions: 1, Policy: PolicyClamp,
				Contributions: map[string]int{"a": 1}, Partition: "p0"},
			{Params: PrivacyParams{Epsilon: 0.7}, MaxContributions: 1, Policy: PolicyClamp,
				Contributions: map[string]int{"b": 1}, Partition: "p1"},
		},
	}
	results, err := e.ParallelCount(pq)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input children eps=[0.3,0.7]; rule parallel: bill max=0.7 once; partitions=%s,%s noisy=%v,%v",
		results[0].Partition, results[1].Partition, results[0].NoisyValue, results[1].NoisyValue)
	eps, _, _ := e.Used("users")
	if math.Abs(eps-0.7) > 1e-12 {
		t.Fatalf("parallel should bill max eps 0.7, got %v", eps)
	}
	events, _ := e.Events("users")
	if len(events) != 1 || events[0].Kind != ChargeParallel {
		t.Fatalf("expected one parallel event, got %+v", events)
	}

	// 重放并行请求：结果一致，不重复计费。
	replay, err := e.ParallelCount(pq)
	if err != nil {
		t.Fatal(err)
	}
	if replay[0].NoisyValue != results[0].NoisyValue || replay[1].NoisyValue != results[1].NoisyValue {
		t.Fatalf("parallel replay diverged: %v,%v vs %v,%v",
			replay[0].NoisyValue, replay[1].NoisyValue, results[0].NoisyValue, results[1].NoisyValue)
	}
	eps2, _, _ := e.Used("users")
	if math.Abs(eps2-0.7) > 1e-12 {
		t.Fatalf("parallel replay double billed, used=%v", eps2)
	}
}

func TestEngineParallelBudgetExceeded(t *testing.T) {
	e := newTestEngine(t)
	pq := ParallelCountQuery{
		Dataset:   "users",
		RequestID: "parx",
		Children: []CountQuery{
			{Params: PrivacyParams{Epsilon: 0.6}, MaxContributions: 1, Policy: PolicyClamp},
			{Params: PrivacyParams{Epsilon: 0.6}, MaxContributions: 1, Policy: PolicyClamp},
		},
	}
	// 先消耗 0.5，剩余 0.5；并行组合 max=0.6 应被拒绝。
	if _, err := e.Count(countReq("pre", 0.5, 1)); err != nil {
		t.Fatal(err)
	}
	_, err := e.ParallelCount(pq)
	t.Logf("input used=0.5 parallel charge=max(0.6,0.6)=0.6; decision: ErrBudgetExceeded; got=%v", err)
	if !HasCode(err, ErrBudgetExceeded) {
		t.Fatalf("want ErrBudgetExceeded, got %v", err)
	}
}

func TestEngineExternalRandomSource(t *testing.T) {
	rng := &byteReader{data: makeDeterministicBytes(32)}
	e, err := NewEngine(WithRandomSource(rng))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	q := CountQuery{Dataset: "d", RequestID: "ext1", Params: PrivacyParams{Epsilon: 1},
		MaxContributions: 1, Policy: PolicyClamp, Contributions: map[string]int{"a": 1}}
	r1, err := e.Count(q)
	if err != nil {
		t.Fatal(err)
	}
	// requestID 派生与外部源互不影响：重放返回缓存结果。
	r2, err := e.Count(q)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("external RNG consumed=%d bytes; replay cached=%v; rule: first result cached and replayed",
		rng.consumed, r1.NoisyValue == r2.NoisyValue)
	if r1.NoisyValue != r2.NoisyValue {
		t.Fatal("replay mismatch with external random source")
	}
	if rng.consumed != 16 {
		t.Fatalf("expected exactly 16 bytes consumed, got %d", rng.consumed)
	}
}

type byteReader struct {
	data     []byte
	consumed int
}

func (b *byteReader) Read(p []byte) (int, error) {
	n := copy(p, b.data[b.consumed:])
	b.consumed += n
	if n < len(p) {
		return n, errors.New("deterministic reader exhausted")
	}
	return n, nil
}

func makeDeterministicBytes(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*73 + 17) % 256)
	}
	return out
}

func mustStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "state.json")
}
