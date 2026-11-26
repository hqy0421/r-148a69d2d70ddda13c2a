package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// TestBatchGetRequestRoundTrip verifies encode→decode preserves key order,
// duplicate keys, and binary key bytes.
func TestBatchGetRequestRoundTrip(t *testing.T) {
	cases := [][][]byte{
		{[]byte("a")},
		{[]byte("a"), []byte("b"), []byte("c")},
		{[]byte("dup"), []byte("dup")}, // repeated key stays repeated
		{{0x00, 0xff, 0x0a}, {}},       // binary bytes; empty key allowed at this level? no — see below
	}
	// The last case contains an empty key, which encode must reject. Split
	// it out rather than feeding it to the round-trip loop.
	_, err := EncodeRequest(&BatchGetRequest{Keys: cases[len(cases)-1]})
	if err == nil {
		t.Fatalf("encode with empty key: expected ProtocolError")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("empty key error = %T %v, want ProtocolError", err, err)
	}
	cases = cases[:len(cases)-1]

	for i, keys := range cases {
		frame, err := EncodeRequest(&BatchGetRequest{Keys: keys})
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		tag, body, err := ReadFrame(bytes.NewReader(frame))
		if err != nil {
			t.Fatalf("case %d ReadFrame: %v", i, err)
		}
		if Op(tag) != OpBatchGet {
			t.Fatalf("case %d tag = 0x%02x, want OpBatchGet", i, tag)
		}
		got, err := DecodeRequest(OpBatchGet, body)
		if err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		bg, ok := got.(*BatchGetRequest)
		if !ok {
			t.Fatalf("case %d type = %T, want *BatchGetRequest", i, got)
		}
		if len(bg.Keys) != len(keys) {
			t.Fatalf("case %d key count = %d, want %d", i, len(bg.Keys), len(keys))
		}
		for j := range keys {
			if !bytes.Equal(bg.Keys[j], keys[j]) {
				t.Fatalf("case %d key %d = %x, want %x", i, j, bg.Keys[j], keys[j])
			}
		}
	}
}

// TestEncodeBatchGetRejectsEmptyAndOversize guards the request-side
// validation: an empty key list is invalid, an over-cap count is rejected,
// and a body that would exceed the frame budget is caught before
// allocation.
func TestEncodeBatchGetRejectsEmptyAndOversize(t *testing.T) {
	if _, err := encodeRequestBody(&BatchGetRequest{Keys: nil}); err == nil {
		t.Fatal("empty keys: expected error")
	}
	if _, err := encodeRequestBody(&BatchGetRequest{Keys: make([][]byte, MaxBatchGetKeys+1)}); err == nil {
		t.Fatal("oversize count: expected error")
	}

	// Frame-budget guard: each max-length key costs 4 + MaxKeyLen bytes.
	// Enough of them to overflow MaxFramePayload must be rejected. The
	// count itself stays under MaxBatchGetKeys so only the byte cap can
	// trigger.
	needed := (MaxFramePayload / (MaxKeyLen + 4)) + 2
	big := make([][]byte, needed)
	for i := range big {
		big[i] = bytes.Repeat([]byte{'k'}, MaxKeyLen)
	}
	if needed > MaxBatchGetKeys {
		t.Skipf("frame-budget case needs %d keys, over count cap", needed)
	}
	if _, err := encodeRequestBody(&BatchGetRequest{Keys: big}); err == nil {
		t.Fatalf("%d max-length keys: expected frame-size error", needed)
	}
}

// TestBatchGetOKRoundTrip covers the response codec, including the three
// states the spec requires to stay distinct: missing, present-empty, and
// present-with-bytes.
func TestBatchGetOKRoundTrip(t *testing.T) {
	entries := []BatchGetEntry{
		{Found: true, Value: []byte("hello")},
		{Found: false, Value: nil},           // missing
		{Found: true, Value: []byte{}},       // present but empty
		{Found: true, Value: []byte{0, 255}}, // binary
		{Found: false, Value: nil},
	}
	body, err := EncodeBatchGetOK(entries)
	if err != nil {
		t.Fatalf("EncodeBatchGetOK: %v", err)
	}
	got, err := DecodeBatchGetOK(body)
	if err != nil {
		t.Fatalf("DecodeBatchGetOK: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("entry count = %d, want %d", len(got), len(entries))
	}
	for i, want := range entries {
		if got[i].Found != want.Found {
			t.Errorf("entry %d Found = %v, want %v", i, got[i].Found, want.Found)
		}
		if want.Found {
			if !bytes.Equal(got[i].Value, want.Value) {
				t.Errorf("entry %d value = %x, want %x", i, got[i].Value, want.Value)
			}
			// Present-empty must decode to a non-nil slice.
			if len(want.Value) == 0 && got[i].Value == nil {
				t.Errorf("entry %d present-empty decoded to nil", i)
			}
		} else {
			if got[i].Value != nil {
				t.Errorf("entry %d missing decoded with non-nil value %x", i, got[i].Value)
			}
		}
	}
}

// TestDecodeBatchGetOKRejectsMalformed exercises the response decoder's
// guards: truncated header, over-cap count, pre-allocation check, unknown
// found flag, missing entry smuggling a value, truncated value, trailing
// bytes.
func TestDecodeBatchGetOKRejectsMalformed(t *testing.T) {
	packU32 := func(v uint32) []byte {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, v)
		return b
	}
	cases := map[string][]byte{
		"truncated header":        {0x00, 0x01},
		"zero count":              packU32(0),
		"oversize count":          packU32(MaxBatchGetKeys + 1),
		"pre-alloc count too big": append(packU32(MaxBatchGetKeys), 0, 0, 0, 0, 0),
		"truncated entry header":  append(packU32(1), BatchGetFound),
		"truncated value": func() []byte {
			b := packU32(1)
			b = append(b, BatchGetFound)
			b = append(b, packU32(4)...)
			b = append(b, 'o', 'n') // 2 of 4
			return b
		}(),
		"unknown found flag": func() []byte {
			b := packU32(1)
			b = append(b, 0x77)
			b = append(b, packU32(0)...)
			return b
		}(),
		"missing entry with value bytes": func() []byte {
			b := packU32(1)
			b = append(b, BatchGetMissing)
			b = append(b, packU32(1)...)
			b = append(b, 'x')
			return b
		}(),
		"trailing bytes": func() []byte {
			b := packU32(1)
			b = append(b, BatchGetFound)
			b = append(b, packU32(1)...)
			b = append(b, 'x')
			b = append(b, 0xff)
			return b
		}(),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeBatchGetOK(body)
			if err == nil {
				t.Fatalf("expected ProtocolError")
			}
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

// TestEncodeBatchGetOKFrameBoundary pins the exact frame-size contract:
// a body of MaxFramePayload-1 bytes (the largest that still leaves room
// for the 1-byte frame tag) must encode and survive EncodeFrame, while one
// more byte must be rejected by EncodeBatchGetOK itself — not slip through
// and fail later at the WriteFrame layer.
func TestEncodeBatchGetOKFrameBoundary(t *testing.T) {
	const maxBody = uint64(MaxFramePayload) - 1
	// Two found entries: u32 count header + 2*(u8 found + u32 vlen) fixed
	// overhead. Both values share one backing array so the test footprint
	// stays ~16 MiB instead of ~32 MiB.
	const overhead = 4 + 2*5
	v1 := MaxValLen
	v2 := int(maxBody - overhead - uint64(v1))
	if v2 <= 0 || v2 > MaxValLen {
		t.Fatalf("test setup: v2=%d outside [1, MaxValLen]", v2)
	}
	buf := make([]byte, v1)
	entries := []BatchGetEntry{
		{Found: true, Value: buf[:v1]},
		{Found: true, Value: buf[:v2]},
	}
	body, err := EncodeBatchGetOK(entries)
	if err != nil {
		t.Fatalf("largest legal body rejected: %v", err)
	}
	if uint64(len(body)) != maxBody {
		t.Fatalf("body = %d bytes, want %d", len(body), maxBody)
	}
	if _, err := EncodeFrame(uint8(StatusOK), body); err != nil {
		t.Fatalf("largest legal body must survive EncodeFrame: %v", err)
	}

	// One extra value byte pushes the body to MaxFramePayload exactly,
	// leaving no room for the tag byte: the encoder must refuse up front.
	entries[1].Value = buf[:v2+1]
	if _, err := EncodeBatchGetOK(entries); err == nil {
		t.Fatal("body of MaxFramePayload bytes: expected frame-size error")
	}
}

// TestEncodeBatchGetOKRejectsEmpty verifies the encoder refuses an empty
// result set (the wire always carries >=1 result) and a missing entry
// incorrectly carrying value bytes.
func TestEncodeBatchGetOKRejectsEmpty(t *testing.T) {
	if _, err := EncodeBatchGetOK(nil); err == nil {
		t.Fatal("empty entries: expected error")
	}
	// Missing entry with non-nil value is silently normalised to vlen=0 by
	// the encoder, not rejected — assert the normalisation explicitly.
	body, err := EncodeBatchGetOK([]BatchGetEntry{{Found: false, Value: []byte("ignored")}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeBatchGetOK(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got[0].Found || got[0].Value != nil {
		t.Fatalf("missing entry not normalised: %+v", got[0])
	}
}
