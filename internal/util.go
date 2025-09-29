package internal

func FirstNonNilPtr[T any](a *T, b *T) *T {
	if a != nil {
		return a
	}
	if b != nil {
		return b
	}
	return nil
}
