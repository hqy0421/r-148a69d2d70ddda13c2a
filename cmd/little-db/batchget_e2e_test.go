package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// TestE2EBatchGetPositional covers the positional-args form: order is
// preserved, repeated keys repeat, missing vs present-empty stay distinct,
// and a partial miss exits with exitNotFound while still printing every
// result.
func TestE2EBatchGetPositional(t *testing.T) {
	addr := startTestServer(t)

	// Seed: a normal value and a present-but-empty value.
	if _, errb, code := invoke([]string{"put", "--addr", addr, "alpha", "AAA"}, ""); code != exitOK {
		t.Fatalf("put alpha: code=%d stderr=%q", code, errb)
	}
	if _, errb, code := invoke([]string{"put", "--addr", addr, "empty", ""}, ""); code != exitOK {
		t.Fatalf("put empty: code=%d stderr=%q", code, errb)
	}

	// All-present call exits 0.
	out, errb, code := invoke([]string{
		"batch-get", "--addr", addr, "alpha", "alpha", "empty",
	}, "")
	if code != exitOK {
		t.Fatalf("all-present: code=%d stderr=%q", code, errb)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines=%d want 3: %q", len(lines), out)
	}
	type rec struct {
		Found bool   `json:"found"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	parse := func(line string) rec {
		var r rec
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return r
	}
	r0 := parse(lines[0])
	r1 := parse(lines[1])
	r2 := parse(lines[2])
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	if !r0.Found || r0.Key != b64("alpha") || r0.Value != b64("AAA") {
		t.Errorf("line0 = %+v", r0)
	}
	if r1.Key != r0.Key || r1.Value != r0.Value {
		t.Errorf("duplicate key not repeated: %+v vs %+v", r1, r0)
	}
	// Present-empty: found=true, value omitted (omitempty on empty string).
	if !r2.Found || r2.Key != b64("empty") || r2.Value != "" {
		t.Errorf("empty line = %+v", r2)
	}

	// Include a missing key: full output still printed, exit 4.
	out, errb, code = invoke([]string{
		"batch-get", "--addr", addr, "alpha", "ghost",
	}, "")
	if code != exitNotFound {
		t.Fatalf("partial miss: code=%d want %d stderr=%q", code, exitNotFound, errb)
	}
	lines = strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("partial-miss lines=%d want 2: %q", len(lines), out)
	}
	miss := parse(lines[1])
	if miss.Found || miss.Key != b64("ghost") {
		t.Errorf("missing line = %+v", miss)
	}
	if !strings.Contains(errb, "not found") {
		t.Errorf("stderr=%q want not-found diagnostic", errb)
	}
}

// TestE2EBatchGetRawFormat checks the raw KEY\tVALUE\n shape, including a
// missing key's blank value field.
func TestE2EBatchGetRawFormat(t *testing.T) {
	addr := startTestServer(t)
	if _, _, code := invoke([]string{"put", "--addr", addr, "k", "v"}, ""); code != exitOK {
		t.Fatalf("put: code=%d", code)
	}
	out, _, code := invoke([]string{
		"batch-get", "--addr", addr, "--format", "raw", "k", "absent",
	}, "")
	// Partial miss → exit 4, but raw output is complete.
	if code != exitNotFound {
		t.Fatalf("code=%d want %d", code, exitNotFound)
	}
	if want := "k\tv\nabsent\t\n"; out != want {
		t.Fatalf("raw stdout = %q want %q", out, want)
	}
}

// TestE2EBatchGetStdinNDJSON covers the "-" NDJSON input path and --plain.
func TestE2EBatchGetStdinNDJSON(t *testing.T) {
	addr := startTestServer(t)
	if _, _, code := invoke([]string{"put", "--addr", addr, "one", "1"}, ""); code != exitOK {
		t.Fatalf("put: code=%d", code)
	}

	// Base64 stdin (default).
	stdin := fmt.Sprintf(
		`{"key":%q}`+"\n"+`{"key":%q}`+"\n",
		base64.StdEncoding.EncodeToString([]byte("one")),
		base64.StdEncoding.EncodeToString([]byte("nope")),
	)
	out, errb, code := invoke([]string{"batch-get", "--addr", addr, "-"}, stdin)
	if code != exitNotFound {
		t.Fatalf("b64 stdin: code=%d stderr=%q", code, errb)
	}
	if !strings.Contains(out, `"found":true`) || !strings.Contains(out, `"found":false`) {
		t.Fatalf("stdin stdout = %q", out)
	}

	// --plain literal keys, all present → exit 0.
	plainStdin := `{"key":"one"}` + "\n"
	out, _, code = invoke([]string{"batch-get", "--addr", addr, "--plain", "-"}, plainStdin)
	if code != exitOK {
		t.Fatalf("plain stdin: code=%d out=%q", code, out)
	}
	if !strings.Contains(out, base64.StdEncoding.EncodeToString([]byte("1"))) {
		t.Fatalf("plain stdout = %q", out)
	}
}

// TestE2EBatchGetUsageErrors pins local validation: no keys, bad format,
// empty positional key, and over-cap key count all fail with exitUsage
// without dialing.
func TestE2EBatchGetUsageErrors(t *testing.T) {
	const badAddr = "127.0.0.1:1"
	cases := []struct {
		name  string
		args  []string
		stdin string
	}{
		{"no-keys", []string{"batch-get", "--addr", badAddr}, ""},
		{"bad-format", []string{"batch-get", "--addr", badAddr, "--format", "xml", "k"}, ""},
		{"empty-positional-key", []string{"batch-get", "--addr", badAddr, ""}, ""},
		{"empty-stdin", []string{"batch-get", "--addr", badAddr, "-"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, code := invoke(tc.args, tc.stdin)
			if code != exitUsage {
				t.Fatalf("code=%d want exitUsage=%d", code, exitUsage)
			}
		})
	}
}

// TestE2EBatchGetKeyHex verifies the --key-hex positional decoding path.
func TestE2EBatchGetKeyHex(t *testing.T) {
	addr := startTestServer(t)
	// Put under a binary key (hex 0102).
	if _, _, code := invoke([]string{"put", "--addr", addr, "--key-hex", "0102", "bin"}, ""); code != exitOK {
		t.Fatalf("put binary key: code=%d", code)
	}
	out, _, code := invoke([]string{"batch-get", "--addr", addr, "--key-hex", "0102"}, "")
	if code != exitOK {
		t.Fatalf("batch-get hex: code=%d", code)
	}
	if !strings.Contains(out, `"found":true`) {
		t.Fatalf("hex stdout = %q", out)
	}
}
