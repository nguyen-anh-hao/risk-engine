package outbox

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEncoderProducesConfluentFrame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":7}`))
	}))
	defer srv.Close()

	enc, err := NewEncoder(srv.URL, "risk.events-value")
	if err != nil {
		t.Fatal(err)
	}
	b, err := enc.Encode(RiskEvent{
		EventID: "e1", EventType: "BalanceReserved", AccountID: 1, Asset: "CASH",
		Total: "100", Available: "90", Frozen: "10", OccurredAt: 1, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 6 || b[0] != 0 {
		t.Fatalf("bad frame: %x", b)
	}
}
