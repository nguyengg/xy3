package internal

// Sizer implements io.Writer that tallies that number of bytes written.
type Sizer struct {
	Size int64
}

func (s *Sizer) Write(p []byte) (n int, err error) {
	n = len(p)
	s.Size += int64(n)
	return
}
