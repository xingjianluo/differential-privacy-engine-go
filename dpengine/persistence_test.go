package dpengine

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestSnapshotRoundTripRestoresBudget(t *testing.T) {
	l := mustLedger(t, "ds", 2.0)
	if _, err := l.Consume("ds", "r1", 0.7, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Consume("ds", "r2", 0.5, ParallelComposition); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	restored, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}

	spentBefore, totalBefore, _ := l.Spent("ds")
	spentAfter, totalAfter, _ := restored.Spent("ds")
	t.Logf("input: snapshot after spent=%.4f/%.1f -> restored spent=%.4f/%.1f",
		spentBefore, totalBefore, spentAfter, totalAfter)
	if spentBefore != spentAfter || totalBefore != totalAfter {
		t.Fatalf("budget state changed across restart: (%v,%v) -> (%v,%v)",
			spentBefore, totalBefore, spentAfter, totalAfter)
	}

	// Replay of a pre-crash request must not double-charge after restart.
	c, err := restored.Consume("ds", "r1", 0.7, SequentialComposition)
	if err != nil {
		t.Fatalf("replay after restart rejected: %v", err)
	}
	if c.SpentAfter != spentBefore {
		t.Fatalf("replay record mismatch: %+v", c)
	}
	spent, _, _ := restored.Spent("ds")
	t.Logf("input: replay pre-crash request r1 after restart -> spent=%.4f (unchanged)", spent)
	if spent != spentBefore {
		t.Fatalf("replay after restart double-charged: spent=%v", spent)
	}

	// New consumption continues monotonically from restored state.
	if _, err := restored.Consume("ds", "r3", 0.8, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	spent, _, _ = restored.Spent("ds")
	t.Logf("input: new request eps=0.8 after restart -> spent=%.4f", spent)
	if spent != spentBefore+0.8 {
		t.Fatalf("post-restart consumption not monotonic: spent=%v", spent)
	}
}

func TestLoadSnapshotVersionMismatchRejected(t *testing.T) {
	data := `{"version": 999, "datasets": []}`
	_, err := LoadSnapshot(strings.NewReader(data))
	var dpe *Error
	errors.As(err, &dpe)
	t.Logf("input: snapshot version=999 (supported=%d) -> reason=%q", SnapshotVersion, dpe.Reason)
	if dpe == nil || dpe.Reason != ReasonVersionMismatch {
		t.Fatalf("want ReasonVersionMismatch, got %v", err)
	}
}

func TestLoadSnapshotLegacyMissingFields(t *testing.T) {
	// Legacy snapshot: version 0, datasets carry only the audit trail,
	// SpentEpsilon is absent. It must be derived deterministically.
	data := `{"datasets": [{"Name": "ds", "TotalEpsilon": 2.0,
		"ConsumedUnits": [
			{"RequestID": "a", "Epsilon": 0.5, "Composition": 0, "SpentAfter": 0.5},
			{"RequestID": "b", "Epsilon": 0.25, "Composition": 0, "SpentAfter": 0.75}
		]}]}`
	l, err := LoadSnapshot(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	spent, total, err := l.Spent("ds")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: legacy snapshot without SpentEpsilon -> derived spent=%.4f total=%.1f", spent, total)
	if spent != 0.75 || total != 2.0 {
		t.Fatalf("deterministic default derivation failed: spent=%v total=%v", spent, total)
	}
	// Replay index must be rebuilt from the audit trail.
	if _, err := l.Consume("ds", "a", 0.5, SequentialComposition); err != nil {
		t.Fatalf("legacy replay rejected: %v", err)
	}
	spent, _, _ = l.Spent("ds")
	if spent != 0.75 {
		t.Fatalf("legacy replay double-charged: spent=%v", spent)
	}
}

func TestLoadSnapshotCorruptRejected(t *testing.T) {
	cases := []struct {
		name string
		data string
		want Reason
	}{
		{"not json", `{oops`, ReasonCorruptSnapshot},
		{"empty dataset name", `{"version":1,"datasets":[{"TotalEpsilon":1}]}`, ReasonCorruptSnapshot},
		{"duplicate dataset", `{"version":1,"datasets":[{"Name":"d","TotalEpsilon":1},{"Name":"d","TotalEpsilon":1}]}`, ReasonCorruptSnapshot},
	}
	for _, tc := range cases {
		_, err := LoadSnapshot(strings.NewReader(tc.data))
		var dpe *Error
		errors.As(err, &dpe)
		t.Logf("input: %s -> reason=%q", tc.name, dpe.Reason)
		if dpe == nil || dpe.Reason != tc.want {
			t.Errorf("%s: want reason %q, got %v", tc.name, tc.want, err)
		}
	}
}

func TestSnapshotConcurrentConsumeThenSave(t *testing.T) {
	// A snapshot taken after concurrent consumption must reflect exactly
	// the spent amount, never more than the budget.
	l := mustLedger(t, "ds", 5.0)
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(w int) {
			for i := 0; i < 20; i++ {
				l.Consume("ds", string(rune('a'+w))+string(rune('0'+i%10))+string(rune('0'+i/10)), 0.1, SequentialComposition)
			}
			done <- struct{}{}
		}(w)
	}
	for w := 0; w < 8; w++ {
		<-done
	}
	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	restored, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	s1, _, _ := l.Spent("ds")
	s2, _, _ := restored.Spent("ds")
	t.Logf("input: 8 workers x 20 requests eps=0.1 budget=5.0 -> live spent=%.4f restored spent=%.4f", s1, s2)
	if s1 != s2 || s1 > 5.0 {
		t.Fatalf("snapshot inconsistent with live ledger: %v vs %v", s1, s2)
	}
}
