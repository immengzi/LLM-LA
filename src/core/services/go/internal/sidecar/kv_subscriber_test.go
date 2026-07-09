package sidecar

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestDecodeIntString covers signed, unsigned, and >int64 block hashes.
func TestDecodeIntString(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
		want string
	}{
		{"small", uint64(42), "42"},
		{"zero", uint64(0), "0"},
		{"max_int64", uint64(9223372036854775807), "9223372036854775807"},
		{"above_int64", uint64(18446744073709551615), "18446744073709551615"},
	}
	for _, c := range cases {
		raw, err := msgpack.Marshal(c.val)
		if err != nil {
			t.Fatalf("%s: marshal: %v", c.name, err)
		}
		if got := decodeIntString(raw); got != c.want {
			t.Fatalf("%s: decodeIntString = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestDecodeHashList verifies a msgpack list of block hashes decodes to
// canonical decimal strings, preserving order and large values.
func TestDecodeHashList(t *testing.T) {
	in := []uint64{1, 2, 9223372036854775808, 18446744073709551615}
	raw, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := decodeHashList(raw)
	want := []string{"1", "2", "9223372036854775808", "18446744073709551615"}
	if len(got) != len(want) {
		t.Fatalf("decodeHashList len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decodeHashList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestDecodeHashListInvalid returns nil on non-list input.
func TestDecodeHashListInvalid(t *testing.T) {
	raw, _ := msgpack.Marshal("not-a-list")
	if got := decodeHashList(raw); got != nil {
		t.Fatalf("decodeHashList(non-list) = %v, want nil", got)
	}
}
