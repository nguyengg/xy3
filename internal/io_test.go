package internal

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingCloser records how many times it was invoked and lets tests inject an error.
type countingCloser struct {
	called int
	err    error
}

func (c *countingCloser) Close() error {
	c.called++
	return c.err
}

func TestChainCloser_AllInvokedOnHappyPath(t *testing.T) {
	c1, c2, c3 := &countingCloser{}, &countingCloser{}, &countingCloser{}
	chain := ChainCloser(c1.Close, c2.Close, c3.Close)

	assert.NoError(t, chain())
	assert.Equal(t, 1, c1.called)
	assert.Equal(t, 1, c2.called)
	assert.Equal(t, 1, c3.called)
}

func TestChainCloser_AllInvokedEvenWhenFirstFails(t *testing.T) {
	// contract: every closer runs, even after an earlier one errored. otherwise a file handle
	// or encoder leaks whenever the top of the chain reports a problem.
	first := errors.New("first")
	c1 := &countingCloser{err: first}
	c2, c3 := &countingCloser{}, &countingCloser{}
	chain := ChainCloser(c1.Close, c2.Close, c3.Close)

	err := chain()
	assert.Same(t, first, err)
	assert.Equal(t, 1, c1.called)
	assert.Equal(t, 1, c2.called)
	assert.Equal(t, 1, c3.called)
}

func TestChainCloser_FirstErrorWins(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")
	c1 := &countingCloser{err: first}
	c2 := &countingCloser{err: second}
	chain := ChainCloser(c1.Close, c2.Close)

	err := chain()
	assert.Same(t, first, err)
}

func TestChainCloser_LaterErrorSurfacesWhenFirstSucceeds(t *testing.T) {
	second := errors.New("second")
	c1 := &countingCloser{}
	c2 := &countingCloser{err: second}
	chain := ChainCloser(c1.Close, c2.Close)

	err := chain()
	assert.Same(t, second, err)
}

func TestResetOnCloseReadSeeker_RestoresOffset(t *testing.T) {
	src := strings.NewReader("0123456789")
	_, err := src.Seek(3, io.SeekStart)
	require.NoError(t, err)

	rsc := ResetOnCloseReadSeeker(src)

	// drain past the captured offset.
	buf := make([]byte, 4)
	n, err := io.ReadFull(rsc, buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "3456", string(buf))

	// close must rewind to the original offset (3), not 0.
	require.NoError(t, rsc.Close())
	off, err := src.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(3), off, "expected offset restored to the value captured at construction")
}

func TestResetOnCloseReadSeeker_ErrorLatch(t *testing.T) {
	// once a Seek fails, subsequent Read/Seek/Close all return the latched error and do not
	// touch the underlying reader.
	src := &alwaysFailSeeker{}
	rsc := ResetOnCloseReadSeeker(src)

	// Read latches the same error.
	_, err := rsc.Read(make([]byte, 4))
	assert.Error(t, err)

	// so does Seek.
	_, err = rsc.Seek(0, io.SeekStart)
	assert.Error(t, err)

	// and Close.
	err = rsc.Close()
	assert.Error(t, err)
}

// alwaysFailSeeker fails every Seek — used to force ResetOnCloseReadSeeker to latch an error
// at construction and stay in that state.
type alwaysFailSeeker struct{}

func (alwaysFailSeeker) Read(p []byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (alwaysFailSeeker) Seek(int64, int) (int64, error) {
	return 0, errors.New("seek always fails")
}

func TestWriteNoopCloser(t *testing.T) {
	var buf strings.Builder
	w := &WriteNoopCloser{Writer: &buf}

	n, err := w.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", buf.String())
	assert.NoError(t, w.Close())
}
