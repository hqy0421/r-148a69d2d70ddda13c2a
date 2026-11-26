package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatchGetHappyPath covers the basic positional contract: every
// requested key comes back in order, values round-trip, and the result
// slice length always matches the request length.
func TestBatchGetHappyPath(t *testing.T) {
	db := openTestDB(t, false)

	seed := map[string]string{
		"alpha": "A",
		"beta":  "BEE",
		"gamma": "G",
	}
	for k, v := range seed {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("seed Put %s: %v", k, err)
		}
	}

	results, err := db.BatchGet([][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}
	want := []struct {
		key     string
		missing bool
		value   string
	}{
		{"alpha", false, "A"},
		{"beta", false, "BEE"},
		{"gamma", false, "G"},
	}
	for i, w := range want {
		if string(results[i].Key) != w.key {
			t.Errorf("result %d key = %q, want %q", i, results[i].Key, w.key)
		}
		if results[i].Missing != w.missing {
			t.Errorf("result %d Missing = %v, want %v", i, results[i].Missing, w.missing)
		}
		if string(results[i].Value) != w.value {
			t.Errorf("result %d value = %q, want %q", i, results[i].Value, w.value)
		}
	}
}

// TestBatchGetMissingVsEmpty is the explicit distinction the spec calls
// out: a never-written/deleted key (Missing=true, nil value) must not
// collapse into a present key with a zero-length value (Missing=false,
// non-nil empty slice).
func TestBatchGetMissingVsEmpty(t *testing.T) {
	db := openTestDB(t, false)

	if err := db.Put([]byte("present-empty"), []byte{}); err != nil {
		t.Fatalf("Put empty value: %v", err)
	}
	if err := db.Put([]byte("will-delete"), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Delete([]byte("will-delete")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	results, err := db.BatchGet([][]byte{
		[]byte("present-empty"),
		[]byte("never-written"),
		[]byte("will-delete"),
	})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}

	// Present empty value: found, non-nil zero-length slice.
	if results[0].Missing {
		t.Errorf("present-empty reported Missing")
	}
	if results[0].Value == nil {
		t.Errorf("present-empty value is nil; want non-nil empty slice")
	}
	if len(results[0].Value) != 0 {
		t.Errorf("present-empty value len = %d, want 0", len(results[0].Value))
	}

	// Never written: missing, nil value.
	if !results[1].Missing {
		t.Errorf("never-written reported present: value=%q", results[1].Value)
	}
	if results[1].Value != nil {
		t.Errorf("missing entry value = %v, want nil", results[1].Value)
	}

	// Deleted: indistinguishable from never written.
	if !results[2].Missing {
		t.Errorf("deleted key reported present: value=%q", results[2].Value)
	}
}

// TestBatchGetDuplicateKeys verifies repeated keys each occupy their own
// output slot, and both slots carry identical values.
func TestBatchGetDuplicateKeys(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.Put([]byte("dup"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	results, err := db.BatchGet([][]byte{
		[]byte("dup"),
		[]byte("absent"),
		[]byte("dup"),
	})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}
	for i, wantMissing := range []bool{false, true, false} {
		if results[i].Missing != wantMissing {
			t.Errorf("result %d Missing = %v, want %v", i, results[i].Missing, wantMissing)
		}
	}
	if !bytes.Equal(results[0].Value, results[2].Value) {
		t.Errorf("duplicate slots differ: %q vs %q", results[0].Value, results[2].Value)
	}
}

// TestBatchGetValidation covers the cheap input checks: empty keys fail the
// whole call, an empty batch is a no-op, and a closed DB returns
// ErrDBClosed.
func TestBatchGetValidation(t *testing.T) {
	db := openTestDB(t, false)
	if err := db.Put([]byte("ok"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Empty key anywhere fails the entire call (all-or-nothing).
	_, err := db.BatchGet([][]byte{[]byte("ok"), {}, []byte("ok")})
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: got %v, want ErrEmptyKey", err)
	}

	// Empty batch: no-op returning an empty, non-nil slice.
	results, err := db.BatchGet(nil)
	if err != nil {
		t.Fatalf("empty BatchGet: %v", err)
	}
	if results == nil || len(results) != 0 {
		t.Fatalf("empty BatchGet = %#v, want non-nil zero-length slice", results)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := db.BatchGet([][]byte{[]byte("ok")}); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("after Close: got %v, want ErrDBClosed", err)
	}
}

// TestBatchGetSnapshotAcrossBatchPut is the core isolation guarantee: a
// BatchGet racing an atomic BatchPut must observe either the entire
// pre-batch state or the entire post-batch state for every key the batch
// touches — never a mix. We run many iterations so an interleaving that
// violated single-lock snapshotting would be extremely likely to surface.
func TestBatchGetSnapshotAcrossBatchPut(t *testing.T) {
	db := openTestDB(t, false)

	const n = 8
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k%02d", i))
		if err := db.Put(keys[i], []byte("v0")); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	var (
		wg       sync.WaitGroup
		violates atomic.Int64
		stop     atomic.Bool
	)

	// Writer flips the whole keyset between two generations atomically.
	wg.Add(1)
	go func() {
		defer wg.Done()
		gen := 1
		for !stop.Load() {
			entries := make([]BatchEntry, n)
			val := []byte(fmt.Sprintf("v%d", gen))
			for i := range keys {
				entries[i] = BatchEntry{Key: keys[i], Value: val}
			}
			if err := db.BatchPut(entries); err != nil {
				t.Errorf("BatchPut gen %d: %v", gen, err)
				return
			}
			gen++
		}
	}()

	// Reader snapshots all keys and demands a single generation per call.
	for i := 0; i < 2000; i++ {
		results, err := db.BatchGet(keys)
		if err != nil {
			t.Fatalf("BatchGet: %v", err)
		}
		if len(results) != n {
			t.Fatalf("results len = %d, want %d", len(results), n)
		}
		first := string(results[0].Value)
		for j := 1; j < n; j++ {
			if string(results[j].Value) != first {
				violates.Add(1)
				break
			}
		}
	}

	stop.Store(true)
	wg.Wait()
	if v := violates.Load(); v != 0 {
		t.Fatalf("BatchGet observed %d mixed-generation snapshots", v)
	}
}

// TestBatchGetSnapshotAcrossCompaction holds many immutable segments and
// hammers BatchGet while a manual compaction rewrites and retires them.
// Because BatchGet keeps segmentsMu.RLock across the whole snapshot, no
// result may fail with a missing segment / spurious not-found, and every
// value must remain byte-correct.
func TestBatchGetSnapshotAcrossCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 << 10})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const n = 400
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%05d", i))
		if err := db.Put(keys[i], []byte(fmt.Sprintf("val-%05d", i))); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	var (
		wg   sync.WaitGroup
		bad  atomic.Int64
		stop atomic.Bool
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := db.Compact(); err != nil {
				t.Errorf("Compact: %v", err)
				return
			}
		}
	}()

	for round := 0; round < 200; round++ {
		// Read a scattered subset each round, including repeats.
		req := [][]byte{keys[3], keys[399], keys[17], keys[3], keys[250]}
		results, err := db.BatchGet(req)
		if err != nil {
			t.Fatalf("BatchGet round %d: %v", round, err)
		}
		wantIdx := []int{3, 399, 17, 3, 250}
		for i, idx := range wantIdx {
			want := fmt.Sprintf("val-%05d", idx)
			if results[i].Missing || string(results[i].Value) != want {
				bad.Add(1)
			}
		}
	}

	stop.Store(true)
	wg.Wait()
	if v := bad.Load(); v != 0 {
		t.Fatalf("%d incorrect results across compaction", v)
	}
}

// TestBatchGetCloseRace mirrors TestCloseRacingWithGet for the batch path:
// concurrent BatchGet calls against seeded keys may return the correct
// values or ErrDBClosed, but never a spurious miss or a panic.
func TestBatchGetCloseRace(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 << 10})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 500
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k%05d", i))
		if err := db.Put(keys[i], []byte(fmt.Sprintf("v%05d", i))); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	var (
		wg   sync.WaitGroup
		bad  atomic.Int64
		stop atomic.Bool
	)
	for r := 0; r < 16; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for !stop.Load() {
				idx := [3]int{i % n, (i + 1) % n, (i + 2) % n}
				req := [][]byte{keys[idx[0]], keys[idx[1]], keys[idx[2]]}
				results, gerr := db.BatchGet(req)
				switch {
				case gerr == nil:
					for j := range req {
						if results[j].Missing {
							bad.Add(1)
							return
						}
						want := fmt.Sprintf("v%05d", idx[j])
						if string(results[j].Value) != want {
							bad.Add(1)
							return
						}
					}
				case errors.Is(gerr, ErrDBClosed):
					return
				default:
					t.Errorf("unexpected BatchGet error: %v", gerr)
					bad.Add(1)
					return
				}
				i += 3
			}
		}(r)
	}

	// Let readers warm up, then close while they're mid-flight (mirrors
	// TestCloseRacingWithGet).
	time.Sleep(5 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stop.Store(true)
	wg.Wait()
	if v := bad.Load(); v != 0 {
		t.Fatalf("close-vs-batchget race: %d bad results", v)
	}
}
