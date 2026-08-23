package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateRight(t *testing.T) {
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"empty string keeps empty", "", 5, ""},
		{"n zero returns empty", "hello", 0, ""},
		{"n negative returns empty", "hello", -1, ""},
		{"n larger than text returns full text", "hi", 10, "hi"},
		{"n equal to text length returns full text", "hello", 5, "hello"},
		{"ASCII truncates at n runes", "abcdef", 3, "abc"},
		{
			// regression: TruncateRight used to compare byte-index to rune-count,
			// so multibyte input truncated at the wrong place.
			name: "multibyte truncates at rune count, not byte count",
			in:   "日本語テスト",
			n:    3,
			want: "日本語",
		},
		{
			// regression: same bug — a 6-rune 18-byte input with n=15 used to
			// stop early at byte-index 15 (5 runes) instead of returning all 6.
			name: "multibyte n larger than rune count returns whole string",
			in:   "日本語テスト",
			n:    15,
			want: "日本語テスト",
		},
		{"emoji truncates by rune", "🐘🐴🐕🐈🐟", 2, "🐘🐴"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TruncateRight(tt.in, tt.n))
		})
	}
}

func TestTruncateRightWithSuffix(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		n      int
		suffix string
		want   string
	}{
		{"no truncation, no suffix appended", "hi", 10, "...", "hi"},
		{"exact length, no suffix appended", "hello", 5, "...", "hello"},
		{"truncation appends suffix", "hello world", 5, "...", "hello..."},
		{"n zero returns just suffix", "hello", 0, "...", "..."},
		{"empty text and n>0 returns empty (no truncation)", "", 3, "...", ""},
		{
			// multibyte + suffix: truncate at 3 runes, then ellipsis.
			name:   "multibyte truncation with suffix",
			in:     "日本語テスト",
			n:      3,
			suffix: "...",
			want:   "日本語...",
		},
		{
			// multibyte fits: no suffix.
			name:   "multibyte no truncation, no suffix",
			in:     "日本語",
			n:      3,
			suffix: "...",
			want:   "日本語",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TruncateRightWithSuffix(tt.in, tt.n, tt.suffix))
		})
	}
}
