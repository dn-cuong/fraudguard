package velocity

import "testing"

func TestShouldExpireOnlyOnFirstIncr(t *testing.T) {
	cases := []struct {
		n    int64
		want bool
	}{
		{1, true},
		{2, false},
		{100, false},
	}
	for _, tc := range cases {
		if got := shouldExpire(tc.n); got != tc.want {
			t.Fatalf("n=%d: want %v got %v", tc.n, tc.want, got)
		}
	}
}
