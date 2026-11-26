package client

import (
	"errors"
	"testing"

	"little-db/internal/wire"
)

// TestClientBatchGetOK drives a mock server that verifies the request shape
// and answers with a mix of found / missing / present-empty results.
func TestClientBatchGetOK(t *testing.T) {
	var gotReq *wire.BatchGetRequest
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		gotReq = req.(*wire.BatchGetRequest)
		body, err := wire.EncodeBatchGetOK([]wire.BatchGetEntry{
			{Found: true, Value: []byte("v1")},
			{Found: false, Value: nil},
			{Found: true, Value: []byte{}},
		})
		if err != nil {
			t.Fatalf("EncodeBatchGetOK: %v", err)
		}
		return [][2]any{okFrame(body)}
	})
	c := newTestClient(t, conn)

	keys := [][]byte{[]byte("a"), []byte("missing"), []byte("empty")}
	got, err := c.BatchGet(keys)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	// The request that hit the wire must preserve order and duplicates.
	if len(gotReq.Keys) != 3 || string(gotReq.Keys[1]) != "missing" {
		t.Fatalf("wire request keys = %#v", gotReq.Keys)
	}
	if len(got) != 3 {
		t.Fatalf("results = %d, want 3", len(got))
	}
	if !got[0].Found || string(got[0].Value) != "v1" {
		t.Errorf("entry 0 = %+v, want found v1", got[0])
	}
	if got[1].Found || got[1].Value != nil {
		t.Errorf("entry 1 = %+v, want missing/nil", got[1])
	}
	if !got[2].Found || got[2].Value == nil || len(got[2].Value) != 0 {
		t.Errorf("entry 2 = %+v, want found non-nil empty", got[2])
	}
}

// TestClientBatchGetEmptyRejectedLocally verifies the client refuses an
// empty key list without touching the connection (the wire would reject a
// zero-count request anyway).
func TestClientBatchGetEmptyRejectedLocally(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		t.Fatalf("no request should be sent, got %T", req)
		return nil
	})
	c := newTestClient(t, conn)
	if _, err := c.BatchGet(nil); err == nil {
		t.Fatal("empty BatchGet: expected local error")
	}
}

// TestClientBatchGetServerError ensures a non-OK status (e.g. BAD_REQUEST)
// surfaces as *wire.RemoteError rather than a decoded partial result.
func TestClientBatchGetServerError(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		return [][2]any{errFrame(wire.StatusBadRequest, "bad count")}
	})
	c := newTestClient(t, conn)
	_, err := c.BatchGet([][]byte{[]byte("k")})
	var re *wire.RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("err = %T %v, want *RemoteError", err, err)
	}
	if re.Status != wire.StatusBadRequest {
		t.Fatalf("status = %v, want BAD_REQUEST", re.Status)
	}
}
