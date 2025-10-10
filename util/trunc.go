package util

// TruncateRight keeps the first len number of runes of text.
func TruncateRight(text string, len int) string {
	return TruncateRightWithSuffix(text, len, "")
}

// TruncateRightWithSuffix keeps the first len number of runes of text and only append the suffix if truncation happens.
func TruncateRightWithSuffix(text string, len int, suffix string) string {
	if len <= 0 {
		return suffix
	}

	rs := make([]rune, 0, len)
	for i, r := range text {
		if i >= len {
			break
		}

		rs = append(rs, r)
	}

	for _, r := range suffix {
		rs = append(rs, r)
	}

	return string(rs)
}
