package internal

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeChecksum_MatchesKnownValue(t *testing.T) {
	// SRI-format sha-256 of "hello". The sri package emits base64 without padding.
	src := strings.NewReader("hello")
	got, err := ComputeChecksum(t.Context(), src)
	require.NoError(t, err)

	sum := sha256.Sum256([]byte("hello"))
	want := "sha256-" + base64.RawStdEncoding.EncodeToString(sum[:])
	assert.Equal(t, want, got)
}

func TestComputeChecksum_RewindsSource(t *testing.T) {
	// the entire contract of ComputeChecksum is: read to compute, then seek to 0 so callers can re-read.
	// a regression here silently truncates uploads (S3 body starts mid-stream).
	src := strings.NewReader("some non-empty payload")
	_, err := ComputeChecksum(t.Context(), src)
	require.NoError(t, err)

	off, err := src.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off, "expected reader to be rewound to offset 0")

	// and a second read returns the full payload.
	buf, err := io.ReadAll(src)
	require.NoError(t, err)
	assert.Equal(t, "some non-empty payload", string(buf))
}

func TestComputeChecksum_EmptyInput(t *testing.T) {
	got, err := ComputeChecksum(t.Context(), bytes.NewReader(nil))
	require.NoError(t, err)

	sum := sha256.Sum256(nil)
	want := "sha256-" + base64.RawStdEncoding.EncodeToString(sum[:])
	assert.Equal(t, want, got)
}

func TestAlwaysTrueVerifier(t *testing.T) {
	v := &AlwaysTrueVerifier{Hash: DefaultChecksum()}
	// SumAndVerify is unconditionally true regardless of the bytes fed.
	_, _ = v.Write([]byte("anything"))
	assert.True(t, v.SumAndVerify(nil))
}
