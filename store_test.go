package dpengine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRestartRestoresBudgetAndReplay：提交若干请求后用同一快照路径重建引擎，
// 预算、事件与噪声结果必须与崩溃前完全一致，重复请求不二次计费。
func TestRestartRestoresBudgetAndReplay(t *testing.T) {
	path := mustStorePath(t)

	e1, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.RegisterDataset("d", PrivacyParams{Epsilon: 1, Delta: 1e-6}); err != nil {
		t.Fatal(err)
	}
	q1 := countReq2("d", "restart-1", 0.3, 2)
	q2 := countReq2("d", "restart-2", 0.4, 1)
	r1, err := e1.Count(q1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e1.Count(q2)
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("input snapshot=%s; rule: reopen engine, budget/events/noise must match pre-crash state", path)
	e2, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !e2.LoadedFromSnapshot() {
		t.Fatal("expected LoadedFromSnapshot=true")
	}
	eps, delta, ok := e2.Used("d")
	if !ok || !approx(eps, 0.7) || delta != 0 {
		t.Fatalf("restored used wrong: eps=%v delta=%v ok=%v", eps, delta, ok)
	}
	events, _ := e2.Events("d")
	if len(events) != 2 || events[0].RequestID != "restart-1" || events[1].Seq != 2 {
		t.Fatalf("restored events wrong: %+v", events)
	}

	// 重启后重复旧 requestID：返回相同噪声结果且不二次计费。
	r1b, err := e2.Count(q1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("replay after restart request=restart-1 noisy before=%v after=%v; rule: identical, no double billing",
		r1.NoisyValue, r1b.NoisyValue)
	if r1b.NoisyValue != r1.NoisyValue {
		t.Fatalf("noise diverged after restart: %v vs %v", r1b.NoisyValue, r1.NoisyValue)
	}
	eps2, _, _ := e2.Used("d")
	if !approx(eps2, 0.7) {
		t.Fatalf("replay after restart double billed: used=%v", eps2)
	}

	// 重启后新请求继续记账，且序列号在恢复值之后单调递增。
	r3, err := e2.Count(countReq2("d", "restart-3", 0.2, 1))
	if err != nil {
		t.Fatal(err)
	}
	events, _ = e2.Events("d")
	if len(events) != 3 || events[2].Seq != 3 || events[2].RequestID != "restart-3" {
		t.Fatalf("post-restart event wrong: %+v", events)
	}
	t.Logf("new request after restart accepted noisy=%v seq=%d; rule: seq continues from recovered state",
		r3.NoisyValue, events[2].Seq)
	_ = r2
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次重启，预算应为 0.9。
	e3, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatal(err)
	}
	eps3, _, _ := e3.Used("d")
	t.Logf("second restart used eps=%v; want 0.9", eps3)
	if !approx(eps3, 0.9) {
		t.Fatalf("second restart used=%v, want 0.9", eps3)
	}
	_ = e3.Close()
}

// TestRestartDoesNotReopenBudgetOnReRegister：恢复出的数据集不能重复注册。
func TestRestartDatasetExists(t *testing.T) {
	path := mustStorePath(t)
	e, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterDataset("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()

	e2, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatal(err)
	}
	err = e2.RegisterDataset("d", PrivacyParams{Epsilon: 5})
	t.Logf("input re-register restored dataset d; decision: ErrDatasetExists; got=%v", err)
	if !HasCode(err, ErrDatasetExists) {
		t.Fatalf("want ErrDatasetExists, got %v", err)
	}
	_ = e2.Close()
}

// TestLoadLegacyMissingFields：手工构造缺字段的旧版快照，
// 缺失 format_version 按版本 1 默认加载，缺失内联名称/used 按事件重放恢复。
func TestLoadLegacyMissingFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.json")
	// 注意：无 format_version、DatasetBudget 无 name/epsilon_used/delta_used/next_seq。
	legacy := `{
  "ledger": {
    "datasets": {
      "legacy-ds": {
        "epsilon_budget": 2.0,
        "delta_budget": 0,
        "events": [
          {"seq": 1, "request_id": "old-1", "kind": "sequential", "epsilon": 0.5, "delta": 0, "detail": ""}
        ]
      }
    }
  }
}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("input legacy snapshot without format_version/new fields; rule: default v1, rebuild used from events")
	e, err := NewEngine(WithStore(path))
	if err != nil {
		t.Fatalf("legacy load should succeed with defaults, got %v", err)
	}
	eps, delta, ok := e.Used("legacy-ds")
	if !ok || !approx(eps, 0.5) || delta != 0 {
		t.Fatalf("legacy restored state wrong: eps=%v delta=%v ok=%v", eps, delta, ok)
	}
	events, _ := e.Events("legacy-ds")
	if len(events) != 1 || events[0].Seq != 1 {
		t.Fatalf("legacy events wrong: %+v", events)
	}
	// 旧 requestID 仍被识别为已处理（不重复计费）。
	r1, err := e.Count(CountQuery{Dataset: "legacy-ds", RequestID: "old-1",
		Params: PrivacyParams{Epsilon: 0.1}, MaxContributions: 1, Policy: PolicyClamp})
	if err != nil {
		t.Fatalf("legacy replay should serve cached result, got %v", err)
	}
	epsAfter, _, _ := e.Used("legacy-ds")
	t.Logf("legacy replay served noisy=%v, used stays eps=%v", r1.NoisyValue, epsAfter)
	if !approx(epsAfter, 0.5) {
		t.Fatalf("legacy replay changed budget: %v", epsAfter)
	}
	_ = e.Close()
}

// TestLoadFutureVersionRejected：未来版本必须显式拒绝，不静默降级。
func TestLoadFutureVersionRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "future.json")
	future := `{
  "format_version": 99,
  "ledger": {"datasets": {}, "requests": {}}
}`
	if err := os.WriteFile(path, []byte(future), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewEngine(WithStore(path))
	t.Logf("input format_version=99 supported=%d; decision: ErrIncompatibleVersion; got=%v",
		CurrentFormatVersion, err)
	if !HasCode(err, ErrIncompatibleVersion) {
		t.Fatalf("want ErrIncompatibleVersion, got %v", err)
	}
}

// TestLoadCorruptSnapshots：各类语义损坏必须显式拒绝为 ErrCorruptState。
func TestLoadCorruptSnapshots(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"bad json", `{not json`},
		{"event without request id", `{"format_version":1,"ledger":{"datasets":{"d":{
			"epsilon_budget":1,"delta_budget":0,"epsilon_used":0,"delta_used":0,
			"events":[{"seq":1,"kind":"sequential","epsilon":0.1,"delta":0}]}}}}`},
		{"used exceeds budget", `{"format_version":1,"ledger":{"datasets":{"d":{
			"epsilon_budget":1,"delta_budget":0,"epsilon_used":2,"delta_used":0,
			"events":[{"seq":1,"request_id":"x","kind":"sequential","epsilon":2,"delta":0}]}}}}`},
		{"duplicate request across datasets", `{"format_version":1,"ledger":{"datasets":{
			"a":{"epsilon_budget":1,"delta_budget":0,"events":[
				{"seq":1,"request_id":"dup","kind":"sequential","epsilon":0.1,"delta":0}]},
			"b":{"epsilon_budget":1,"delta_budget":0,"events":[
				{"seq":1,"request_id":"dup","kind":"sequential","epsilon":0.1,"delta":0}]}}}}`},
		{"orphan cached result", `{"format_version":1,"ledger":{"datasets":{}},
			"results":{"ghost":{"request_id":"ghost","noisy_value":1,"noise_scale":1}}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "corrupt.json")
			if err := os.WriteFile(path, []byte(stripWS(c.json)), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := NewEngine(WithStore(path))
			t.Logf("input corrupt case=%s; decision: ErrCorruptState; got=%v", c.name, err)
			if !HasCode(err, ErrCorruptState) {
				t.Fatalf("want ErrCorruptState, got %v", err)
			}
		})
	}
}

// TestSnapshotWriteFailureRollsBack：快照路径不可写时，注册/请求都必须失败，
// 且成功路径上的内存记账必须整体回滚（不留下事件、预算与去重键）。
func TestSnapshotWriteFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	// 父目录不存在且不会被自动创建，CreateTemp 必然失败。
	badPath := filepath.Join(dir, "missing-subdir", "state.json")
	e, err := NewEngine(WithStore(badPath))
	if err != nil {
		t.Fatalf("missing snapshot file should start fresh, got %v", err)
	}
	err = e.RegisterDataset("d", PrivacyParams{Epsilon: 1})
	t.Logf("input register with unwritable snapshot path; decision: ErrCorruptState; got=%v", err)
	if !HasCode(err, ErrCorruptState) {
		t.Fatalf("want ErrCorruptState on snapshot failure, got %v", err)
	}
	if _, ok := e.ledger.Dataset("d"); ok {
		t.Fatal("failed register must not leave a dataset behind")
	}

	// 改用纯内存引擎手工绑定坏路径，验证查询提交失败后的回滚。
	e2, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.RegisterDataset("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	e2.store = badPath
	_, err = e2.Count(countReq2("d", "will-rollback", 0.3, 1))
	t.Logf("input count with snapshot write failing; decision: ErrCorruptState and full rollback; got=%v", err)
	if !HasCode(err, ErrCorruptState) {
		t.Fatalf("want ErrCorruptState, got %v", err)
	}
	eps, _, ok := e2.Used("d")
	if !ok || eps != 0 {
		t.Fatalf("rolled-back request must leave zero usage, used=%v ok=%v", eps, ok)
	}
	events, _ := e2.Events("d")
	if len(events) != 0 {
		t.Fatalf("rolled-back request must leave no events, got %+v", events)
	}
	if _, exists := e2.results["will-rollback"]; exists {
		t.Fatal("rolled-back request must leave no cached result")
	}
	// 同一 requestID 在回滚后必须可以重试成功（内存模式）。
	e2.store = ""
	retry, err := e2.Count(countReq2("d", "will-rollback", 0.3, 1))
	if err != nil {
		t.Fatalf("retry after rollback should succeed, got %v", err)
	}
	t.Logf("retry after rollback accepted noisy=%v; rule: dedup key rolled back so retry is allowed",
		retry.NoisyValue)
	_ = e.Close()
	_ = e2.Close()
}

func approx(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-9
}

func stripWS(s string) string {
	return strings.NewReplacer("\n", "", "\t", "", " ", "").Replace(s)
}
