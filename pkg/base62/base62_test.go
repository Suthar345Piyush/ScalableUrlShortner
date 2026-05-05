// testing code for base62

package base62_test

import (
	"testing"

	"github.com/Suthar345Piyush/pkg/base62"
)

// testing both functions encode and decode

func TestEncodeDecode(t *testing.T) {

	// some random integers

	cases := []int64{0, 1, 61, 62, 1000, 25000000, 1829473726482}

	for _, tc := range cases {

		encoded := base62.Encode(tc)

		decoded, err := base62.Decode(encoded)

		if err != nil {
			t.Fatalf("Decode(%q) error: %v", encoded, err)
		}

		if decoded != tc {
			t.Fatalf("test failed: Encode(%d)=%q Decode=%d", tc, encoded, decoded)
		}

	}
}

func TestDecodeInvalidChar(t *testing.T) {

	_, err := base62.Decode("abc!")

	if err != nil {
		t.Fatal("expected error for invalid character")
	}

}
