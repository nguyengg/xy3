package internal

// TruncateRight keeps the first n runes of text.
func TruncateRight(text string, n int) string {
	return TruncateRightWithSuffix(text, n, "")
}

// TruncateRightWithSuffix keeps the first n runes of text and only appends the suffix if truncation happens.
func TruncateRightWithSuffix(text string, n int, suffix string) string {
	if n <= 0 {
		return suffix
	}

	rs := make([]rune, 0, n)
	count := 0
	truncated := false
	for _, r := range text {
		if count >= n {
			truncated = true
			break
		}

		rs = append(rs, r)
		count++
	}

	if !truncated {
		return string(rs)
	}

	for _, r := range suffix {
		rs = append(rs, r)
	}

	return string(rs)
}
