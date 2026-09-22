package dpengine

import (
	"math"
	"testing"
)

func TestLedgerRegisterAndDuplicate(t *testing.T) {
	l := NewLedger()
	t.Logf("input: register users eps=1.0; rule: first register ok, second -> ErrDatasetExists")
	if err := l.Register("users", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	if err := l.Register("users", PrivacyParams{Epsilon: 2}); !HasCode(err, ErrDatasetExists) {
		t.Fatalf("expected ErrDatasetExists, got %v", err)
	}
	if err := l.Register("", PrivacyParams{Epsilon: 1}); !HasCode(err, ErrInvalidParams) {
		t.Fatalf("expected ErrInvalidParams for empty name, got %v", err)
	}
}

func TestLedgerChargeAndDedup(t *testing.T) {
	l := NewLedger()
	if err := l.Register("d", PrivacyParams{Epsilon: 1, Delta: 1e-6}); err != nil {
		t.Fatal(err)
	}
	db, _ := l.Dataset("d")

	t.Logf("input: charge 0.4 then 0.3 sequential on budget 1.0; rule: used sums and events append")
	if !db.canCharge(PrivacyParams{Epsilon: 0.4}) {
		t.Fatal("first charge should fit")
	}
	ev1 := l.charge("d", ChargeSequential, PrivacyParams{Epsilon: 0.4, Delta: 1e-7}, "r1", "first")
	if owner, ok := l.hasRequest("r1"); !ok || owner != "d" {
		t.Fatalf("dedup index wrong: owner=%q ok=%v", owner, ok)
	}
	l.charge("d", ChargeSequential, PrivacyParams{Epsilon: 0.3, Delta: 2e-7}, "r2", "second")
	if db.EpsilonUsed != 0.7 || db.DeltaUsed != 3e-7 {
		t.Fatalf("unexpected used: eps=%v delta=%v", db.EpsilonUsed, db.DeltaUsed)
	}
	if len(db.Events) != 2 || db.Events[0].Seq != 1 || db.Events[1].Seq != 2 {
		t.Fatalf("events not monotonic: %+v", db.Events)
	}

	t.Logf("input next charge 0.5 against remaining 0.3; rule: 0.7+0.5>1 -> cannot charge")
	if db.canCharge(PrivacyParams{Epsilon: 0.5}) {
		t.Fatal("charge exceeding budget must not fit")
	}

	// 模拟引擎落盘失败后的回滚：撤销最后一笔（r2）。
	t.Logf("input: rollback last event seq=%d; rule: event removed, totals restored, dedup key deleted",
		ev1.Seq)
	last := db.Events[len(db.Events)-1]
	db.Events = db.Events[:len(db.Events)-1]
	db.EpsilonUsed -= last.Epsilon
	db.DeltaUsed -= last.Delta
	delete(l.Requests, last.RequestID)
	if math.Abs(db.EpsilonUsed-0.4) > 1e-12 || db.DeltaUsed != 1e-7 {
		t.Fatalf("rollback totals wrong: eps=%v delta=%v", db.EpsilonUsed, db.DeltaUsed)
	}
	if _, ok := l.hasRequest("r2"); ok {
		t.Fatal("dedup key must be removed after rollback so retry is allowed")
	}
}

func TestLedgerParallelChargeRecorded(t *testing.T) {
	l := NewLedger()
	if err := l.Register("d", PrivacyParams{Epsilon: 1}); err != nil {
		t.Fatal(err)
	}
	charge := parallelCompose([]PrivacyParams{{Epsilon: 0.3}, {Epsilon: 0.8}})
	t.Logf("input: parallel children eps=[0.3,0.8]; rule: charge max=%v once", charge.Epsilon)
	l.charge("d", ChargeParallel, charge, "p1", "parallel")
	db, _ := l.Dataset("d")
	if db.EpsilonUsed != 0.8 {
		t.Fatalf("parallel used=%v, want 0.8", db.EpsilonUsed)
	}
	if db.Events[0].Kind != ChargeParallel {
		t.Fatalf("event kind=%v, want parallel", db.Events[0].Kind)
	}
}
