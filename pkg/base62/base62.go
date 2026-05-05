//  base62 file to convert the integers into the base62 strings

package base62

import "fmt"

// base62 keywords - a-zA-Z0-9

const charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// function to convert the int64 to base62 string
// snowflake id of 19 digits going to encode roughly about 10-11 base62 character string

func Encode(n int64) string {

	if n < 0 {
		panic("base62: cannot encode negative integer")
	}

	if n == 0 {
		return "0"
	}

	// buf - byte slice of 12 size

	buf := make([]byte, 0, 12)

	for n > 0 {
		buf = append(buf, charset[n%62])
		n /= 62
	}

	// reversing

	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {

		buf[i], buf[j] = buf[j], buf[i]

	}

	return string(buf)

}

// converting back from base62 to int64, and returning error if the input characters string in link are outside the charset

// string -> integer

func Decode(s string) (int64, error) {

	var n int64

	for _, ch := range s {
		idx := indexOf(byte(ch))

		if idx == -1 {
			return 0, fmt.Errorf("base62: invalid character %q in %q", ch, s)
		}

		n = n*62 + int64(idx)

	}

	return n, nil

}

func indexOf(ch byte) int {
	for i := range len(charset) {
		if charset[i] == ch {
			return i
		}
	}

	return -1
}
