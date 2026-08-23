package xy3

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nguyengg/xy3/internal"
)

// sriOf returns the sha256 SRI checksum string of the given payload, computed with the very hash the
// library uses (internal.DefaultChecksum), so a value produced here is guaranteed to match what
// Upload/Download compute internally.
func sriOf(t *testing.T, payload []byte) string {
	t.Helper()
	h := internal.DefaultChecksum()
	_, err := h.Write(payload)
	require.NoError(t, err)
	return h.SumToString(nil)
}

// putObjectOK registers a PutObject handler that succeeds. s3writer uses a single PutObject for
// payloads below the 5 MiB multipart threshold, which is the path all these tests exercise.
func putObjectOK(m *mockS3) {
	m.on("PutObject", func(any) (any, error) {
		return &s3.PutObjectOutput{}, nil
	})
}

// smallPayload is a few KB — comfortably under s3writer's MinPartSize (5 MiB) so PutObject is used.
func smallPayload() []byte {
	return bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog\n"), 100)
}

func TestUpload_HappyPath_Seekable(t *testing.T) {
	payload := smallPayload()
	mock := newMockS3()
	putObjectOK(mock)

	man, err := Upload(t.Context(), mock.client(t), bytes.NewReader(payload), "my-bucket", "my/key")
	require.NoError(t, err)

	assert.Equal(t, "my-bucket", man.Bucket)
	assert.Equal(t, "my/key", man.Key)
	assert.Equal(t, int64(len(payload)), man.Size)
	assert.Equal(t, sriOf(t, payload), man.Checksum)
	assert.Truef(t, hasSha256Prefix(man.Checksum), "checksum %q should be an sha256- SRI value", man.Checksum)
	assert.Equal(t, 1, mock.count("PutObject"))
}

func TestUpload_HappyPath_NonSeekable(t *testing.T) {
	payload := smallPayload()
	mock := newMockS3()
	putObjectOK(mock)

	// wrap in a struct exposing only io.Reader so the value does NOT satisfy io.ReadSeeker; this is
	// the regression guard for non-seekable sources, where the checksum is computed during upload
	// rather than precomputed.
	src := struct{ io.Reader }{bytes.NewReader(payload)}

	man, err := Upload(t.Context(), mock.client(t), src, "my-bucket", "my/key")
	require.NoError(t, err)

	assert.Equal(t, int64(len(payload)), man.Size)
	assert.NotEmpty(t, man.Checksum)
	assert.Equal(t, sriOf(t, payload), man.Checksum)
	assert.Equal(t, 1, mock.count("PutObject"))
}

func TestUpload_ExpectedChecksumProvided_SkipsPrecompute(t *testing.T) {
	payload := smallPayload()
	want := sriOf(t, payload)

	mock := newMockS3()
	putObjectOK(mock)

	man, err := Upload(t.Context(), mock.client(t), bytes.NewReader(payload), "my-bucket", "my/key",
		func(o *UploadOptions) { o.ExpectedChecksum = want })
	require.NoError(t, err)

	assert.Equal(t, want, man.Checksum)
	assert.Equal(t, int64(len(payload)), man.Size)
	assert.Equal(t, 1, mock.count("PutObject"))
}

func TestUpload_ExpectedChecksumMismatch_ReturnsErrChecksumMismatch(t *testing.T) {
	payload := smallPayload()
	// a syntactically valid sha256 SRI value that does NOT match the payload.
	wrong := sriOf(t, []byte("some other content entirely"))
	require.NotEqual(t, wrong, sriOf(t, payload))

	mock := newMockS3()
	putObjectOK(mock)

	_, err := Upload(t.Context(), mock.client(t), bytes.NewReader(payload), "my-bucket", "my/key",
		func(o *UploadOptions) { o.ExpectedChecksum = wrong })
	require.Error(t, err)

	var mismatch *ErrChecksumMismatch
	require.Truef(t, errors.As(err, &mismatch), "expected ErrChecksumMismatch, got %v", err)
	// H1 regression: Expected must carry exactly what the caller passed in, not the computed value.
	assert.Equal(t, wrong, mismatch.Expected)
	assert.NotEmpty(t, mismatch.Actual)
}

func TestUpload_UnknownChecksumScheme_ErrorsBeforeUpload(t *testing.T) {
	payload := smallPayload()
	mock := newMockS3()
	putObjectOK(mock)

	_, err := Upload(t.Context(), mock.client(t), bytes.NewReader(payload), "my-bucket", "my/key",
		func(o *UploadOptions) { o.ExpectedChecksum = "foo-bar" })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown expected checksum")
	// the failure must happen before any upload is attempted.
	assert.Equal(t, 0, mock.count("PutObject"))
	assert.Empty(t, mock.recorded())
}

// hasSha256Prefix reports whether the SRI checksum uses the sha256- scheme.
func hasSha256Prefix(checksum string) bool {
	return len(checksum) > len("sha256-") && checksum[:len("sha256-")] == "sha256-"
}
