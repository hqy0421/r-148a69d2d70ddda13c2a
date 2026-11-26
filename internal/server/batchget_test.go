package server

import (
	"bytes"
	"net"
	"testing"
	"time"

	"little-db/internal/wire"
)

// roundTripRaw writes a raw opcode+body frame and reads one response. It
// exists for deliberately malformed requests that EncodeRequest refuses to
// build (e.g. a zero-count BATCHGET).
func roundTripRaw(t *testing.T, conn net.Conn, op uint8, body []byte) (wire.Status, []byte) {
	t.Helper()
	frame, err := wire.EncodeFrame(op, body)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("raw write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, respBody, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("raw ReadFrame: %v", err)
	}
	return wire.Status(tag), respBody
}

// TestServerBatchGetEndToEnd exercises the full request→engine→response
// path over a real TCP connection: positional alignment, missing vs empty,
// and duplicate keys.
func TestServerBatchGetEndToEnd(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Seed: one normal value, one empty value.
	if st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("a"), Value: []byte("alpha")}); st != wire.StatusOK {
		t.Fatalf("seed put a: status %v", st)
	}
	if st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("e"), Value: []byte{}}); st != wire.StatusOK {
		t.Fatalf("seed put e: status %v", st)
	}

	st, body := roundTrip(t, conn, &wire.BatchGetRequest{Keys: [][]byte{
		[]byte("a"),
		[]byte("a"), // repeated
		[]byte("missing"),
		[]byte("e"), // present empty
	}})
	if st != wire.StatusOK {
		t.Fatalf("BATCHGET status = %v, want OK", st)
	}
	entries, err := wire.DecodeBatchGetOK(body)
	if err != nil {
		t.Fatalf("DecodeBatchGetOK: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("entries = %d, want 4", len(entries))
	}
	if !entries[0].Found || !bytes.Equal(entries[0].Value, []byte("alpha")) {
		t.Errorf("entry 0 = %+v", entries[0])
	}
	if !entries[1].Found || !bytes.Equal(entries[1].Value, []byte("alpha")) {
		t.Errorf("duplicate entry 1 = %+v", entries[1])
	}
	if entries[2].Found {
		t.Errorf("missing key reported found: %+v", entries[2])
	}
	if !entries[3].Found || len(entries[3].Value) != 0 || entries[3].Value == nil {
		t.Errorf("present-empty entry 3 = %+v", entries[3])
	}
}

// TestServerBatchGetBadRequest verifies a malformed BATCHGET (zero keys)
// is answered BAD_REQUEST and the connection stays usable afterwards — the
// standard desync-resistance guarantee.
func TestServerBatchGetBadRequest(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Hand-craft a zero-count BATCHGET body, which EncodeRequest refuses.
	st, _ := roundTripRaw(t, conn, uint8(wire.OpBatchGet), []byte{0, 0, 0, 0})
	if st != wire.StatusBadRequest {
		t.Fatalf("zero-count status = %v, want BAD_REQUEST", st)
	}

	// Connection must still serve a healthy request.
	if st, _ := roundTrip(t, conn, &wire.PingRequest{}); st != wire.StatusOK {
		t.Fatalf("ping after bad batch-get: status %v", st)
	}
}

// TestServerBatchGetSnapshotConsistency is the network-facing snapshot
// check: a BATCHGET of a whole keyset racing atomic BatchPuts must never
// observe a mixed generation. It mirrors the engine-level isolation test
// but goes through the TCP codec and server dispatch.
func TestServerBatchGetSnapshotConsistency(t *testing.T) {
	_, addr := startServer(t, nil)

	// Use two connections: a writer and a reader, since one client is
	// serial.
	writerConn := dial(t, addr)
	readerConn := dial(t, addr)

	const n = 6
	keys := make([][]byte, n)
	seedEntries := make([]wire.BatchEntry, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte{byte('k'), byte('0' + i)}
		seedEntries[i] = wire.BatchEntry{Key: keys[i], Value: []byte("g0")}
	}
	if st, _ := roundTrip(t, writerConn, &wire.BatchRequest{Entries: seedEntries}); st != wire.StatusOK {
		t.Fatalf("seed batch: %v", st)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for gen := 1; gen < 300; gen++ {
			entries := make([]wire.BatchEntry, n)
			val := []byte{'g', byte('0' + gen%10)}
			for i := range keys {
				entries[i] = wire.BatchEntry{Key: keys[i], Value: val}
			}
			if st, _ := roundTrip(t, writerConn, &wire.BatchRequest{Entries: entries}); st != wire.StatusOK {
				t.Errorf("writer batch gen %d: status %v", gen, st)
				return
			}
		}
	}()

	for round := 0; round < 1000; round++ {
		st, body := roundTrip(t, readerConn, &wire.BatchGetRequest{Keys: keys})
		if st != wire.StatusOK {
			t.Fatalf("reader batch-get round %d: status %v", round, st)
		}
		entries, err := wire.DecodeBatchGetOK(body)
		if err != nil {
			t.Fatalf("decode round %d: %v", round, err)
		}
		if len(entries) != n {
			t.Fatalf("entries = %d, want %d", len(entries), n)
		}
		first := string(entries[0].Value)
		for i := 1; i < n; i++ {
			if !entries[i].Found || string(entries[i].Value) != first {
				t.Fatalf("round %d: mixed snapshot %q vs %q", round, first, entries[i].Value)
			}
		}
	}
	<-done
}

// TestServerBatchGetOversizeResponseIsWholeFailure pins the all-or-nothing
// response-size contract: when the combined result cannot fit in one wire
// frame, the server must NOT stream a partial answer. It replies OVERLOAD
// for the whole call and the connection stays usable.
func TestServerBatchGetOversizeResponseIsWholeFailure(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Three 12 MiB values: each is within the 16 MiB per-value cap, but the
	// combined BATCHGET body (~36 MiB + headers) exceeds the 32 MiB
	// MaxFramePayload a single response may carry.
	big := bytes.Repeat([]byte{'z'}, 12*1024*1024)
	keys := []string{"big1", "big2", "big3"}
	for _, k := range keys {
		if st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte(k), Value: big}); st != wire.StatusOK {
			t.Fatalf("seed %s: status %v", k, st)
		}
	}

	reqKeys := [][]byte{[]byte(keys[0]), []byte(keys[1]), []byte(keys[2])}
	st, body := roundTrip(t, conn, &wire.BatchGetRequest{Keys: reqKeys})
	if st != wire.StatusOverload {
		t.Fatalf("oversize batch-get status = %v, want OVERLOAD", st)
	}
	if msg, derr := wire.DecodeError(body); derr != nil || msg == "" {
		t.Fatalf("OVERLOAD body: msg=%q err=%v", msg, derr)
	}

	// The connection must remain healthy after the rejected batch.
	if st, _ := roundTrip(t, conn, &wire.PingRequest{}); st != wire.StatusOK {
		t.Fatalf("ping after overloaded batch-get: status %v", st)
	}

	// A single big key on its own still fits one frame and answers OK.
	st, body = roundTrip(t, conn, &wire.BatchGetRequest{Keys: [][]byte{[]byte("big1")}})
	if st != wire.StatusOK {
		t.Fatalf("single big key batch-get status = %v, want OK", st)
	}
	entries, err := wire.DecodeBatchGetOK(body)
	if err != nil || len(entries) != 1 || !entries[0].Found || !bytes.Equal(entries[0].Value, big) {
		t.Fatalf("single big key result: entries=%d err=%v", len(entries), err)
	}
}
