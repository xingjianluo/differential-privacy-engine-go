package dpengine

import (
	"math"
	"sync"
	"testing"
)

// TestConcurrentBudgetNoOverspend：预算恰好允许 N 个等大请求时，
// 从 N*4 个并发 goroutine 中必须恰好放行 N 个，其余全部得到
// ErrBudgetExceeded，且最终已用预算不超过上限。
func TestConcurrentBudgetNoOverspend(t *testing.T) {
	const (
		budget = 1.0
		cost   = 0.1
		groups = 40 // 40 个 0.1 竞争，只有 10 个能成功
	)
	e, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: budget}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, rejected := 0, 0
	reasons := map[ErrorCode]int{}
	start := make(chan struct{})
	for i := 0; i < groups; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			q := countReq2("d", "conc-"+itoa2(i), cost, 1)
			_, err := e.Count(q)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
				return
			}
			rejected++
			ee, ok := err.(*EngineError)
			if !ok {
				t.Errorf("non-engine error: %v", err)
				return
			}
			reasons[ee.Code]++
		}(i)
	}
	close(start)
	wg.Wait()

	eps, delta, ok := e.Used("d")
	t.Logf("input: budget=%v cost=%v concurrency=%d; decision: accepted=%d rejected=%d reasons=%v; final used eps=%v delta=%v",
		budget, cost, groups, accepted, rejected, reasons, eps, delta)
	if !ok {
		t.Fatal("dataset vanished")
	}
	wantAccepted := int(math.Round(budget / cost))
	if accepted != wantAccepted {
		t.Fatalf("accepted=%d, want exactly %d", accepted, wantAccepted)
	}
	if accepted+rejected != groups {
		t.Fatalf("accounting lost requests: %d+%d != %d", accepted, rejected, groups)
	}
	if reasons[ErrBudgetExceeded] != groups-wantAccepted {
		t.Fatalf("rejected reasons=%v, all losers must be ErrBudgetExceeded", reasons)
	}
	if math.Abs(eps-budget) > budgetTolerance {
		t.Fatalf("budget overspent: used=%v budget=%v", eps, budget)
	}
	if delta != 0 {
		t.Fatalf("delta used=%v, want 0", delta)
	}
	events, _ := e.Events("d")
	if len(events) != accepted {
		t.Fatalf("events=%d but accepted=%d", len(events), accepted)
	}
	// 事件序列号必须连续且从 1 开始。
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatalf("event seq gap at %d: %d", i, ev.Seq)
		}
	}
}

// TestConcurrentDuplicateRequest：同一 requestID 被高并发重放多次，
// 只能成功计费一次；所有 goroutine 拿到完全相同的噪声结果。
func TestConcurrentDuplicateRequest(t *testing.T) {
	const replicas = 50
	e, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	q := countReq2("d", "same-id", 0.2, 2)

	var wg sync.WaitGroup
	results := make([]float64, replicas)
	errs := make([]error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r, err := e.Count(q)
			if err == nil {
				results[i] = r.NoisyValue
			}
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	var first float64
	okCount := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d got error: %v", i, err)
			continue
		}
		if okCount == 0 {
			first = results[i]
		} else if results[i] != first {
			t.Errorf("replica %d result %v != first %v", i, results[i], first)
		}
		okCount++
	}
	eps, _, _ := e.Used("d")
	t.Logf("input: %d concurrent replicas of request_id=same-id cost=0.2; decision: all served=%d identical noisy=%v, final used eps=%v",
		replicas, okCount, first, eps)
	if okCount != replicas {
		t.Fatalf("served=%d, want all %d replicas served from replay cache", okCount, replicas)
	}
	if math.Abs(eps-0.2) > budgetTolerance {
		t.Fatalf("duplicate must bill exactly once, used=%v", eps)
	}
	events, _ := e.Events("d")
	if len(events) != 1 {
		t.Fatalf("events=%d, want exactly 1", len(events))
	}
}

func countReq2(dataset, id string, eps float64, maxC int) CountQuery {
	return CountQuery{
		Dataset:          dataset,
		RequestID:        id,
		Params:           PrivacyParams{Epsilon: eps},
		MaxContributions: maxC,
		Policy:           PolicyClamp,
		Contributions:    map[string]int{"u": maxC},
	}
}

func itoa2(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// TestConcurrentBudgetThenRestart：并发把预算打满后，重启引擎，
// 磁盘上的已用预算与成功事件集合必须与内存完全一致，且重启后没有任何
// 额外请求还能放行（预算耗尽语义跨重启保持）。
func TestConcurrentBudgetThenRestart(t *testing.T) {
	path := mustStorePath(t)
	e, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}

	const (
		cost   = 0.25
		groups = 20
	)
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := map[string]bool{}
	start := make(chan struct{})
	for i := 0; i < groups; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := "mix-" + itoa2(i)
			_, err := e.Count(countReq2("d", id, cost, 1))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners[id] = true
			} else if !HasCode(err, ErrBudgetExceeded) {
				t.Errorf("unexpected error for %s: %v", id, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	eps, _, _ := e.Used("d")
	t.Logf("input concurrent groups=%d cost=%v; decision: %d winners, used=%v before restart",
		groups, cost, len(winners), eps)
	if len(winners) != 4 {
		t.Fatalf("winners=%d, want 4", len(winners))
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	eps2, _, _ := e2.Used("d")
	events, _ := e2.Events("d")
	t.Logf("after restart used=%v events=%d winners-on-disk:", eps2, len(events))
	onDisk := map[string]bool{}
	for _, ev := range events {
		onDisk[ev.RequestID] = true
		t.Logf("  event seq=%d request=%q kind=%s eps=%v", ev.Seq, ev.RequestID, ev.Kind, ev.Epsilon)
	}
	if len(onDisk) != len(winners) {
		t.Fatalf("on-disk winners=%d, in-memory winners=%d", len(onDisk), len(winners))
	}
	for id := range winners {
		if !onDisk[id] {
			t.Fatalf("winner %q missing after restart", id)
		}
	}

	// 重启后预算仍然耗尽：新请求必须被拒绝；旧胜者重放不二次计费。
	_, err = e2.Count(countReq2("d", "after-restart-new", 1e-9, 1))
	if !HasCode(err, ErrBudgetExceeded) {
		t.Fatalf("post-restart request want ErrBudgetExceeded, got %v", err)
	}
	var firstWinner string
	for id := range winners {
		firstWinner = id
		break
	}
	r1, err := e2.Count(countReq2("d", firstWinner, cost, 1))
	if err != nil {
		t.Fatalf("winner replay after restart: %v", err)
	}
	eps3, _, _ := e2.Used("d")
	t.Logf("after restart new request rejected; winner %q replay noisy=%v, used stays %v",
		firstWinner, r1.NoisyValue, eps3)
	if eps3 != eps2 {
		t.Fatalf("replay changed used budget: %v -> %v", eps2, eps3)
	}
	_ = e2.Close()
}
