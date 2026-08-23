package xy3

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrChecksumMismatch_ErrorsAs(t *testing.T) {
	var target *ErrChecksumMismatch
	assert.True(t, errors.As(&ErrChecksumMismatch{
		Expected: "hello",
		Actual:   "world",
	}, &target))
}

// headOK registers a HeadObject handler returning the given content length and metadata.
func headOK(m *mockS3, size int64, metadata map[string]string) {
	m.on("HeadObject", func(any) (any, error) {
		return &s3.HeadObjectOutput{
			ContentLength: aws.Int64(size),
			Metadata:      metadata,
		}, nil
	})
}

// getOK registers a GetObject handler returning the full payload. Because these tests use small
// payloads (below s3reader's threshold), Download issues exactly one GetObject spanning the whole
// object, so returning the complete body for any range is correct.
func getOK(m *mockS3, payload []byte) {
	m.on("GetObject", func(any) (any, error) {
		return &s3.GetObjectOutput{
			Body:          readCloser(string(payload)),
			ContentLength: aws.Int64(int64(len(payload))),
		}, nil
	})
}

func TestDownload_HappyPath_NoChecksum(t *testing.T) {
	payload := smallPayload()
	mock := newMockS3()
	headOK(mock, int64(len(payload)), nil)
	getOK(mock, payload)

	var dst bytes.Buffer
	err := Download(t.Context(), mock.client(t), "my-bucket", "my/key", &dst)
	require.NoError(t, err)
	assert.Equal(t, payload, dst.Bytes())
	assert.Equal(t, 1, mock.count("HeadObject"))
	assert.Equal(t, 1, mock.count("GetObject"))
}

func TestDownload_ChecksumFromMetadata_Matches(t *testing.T) {
	payload := smallPayload()
	mock := newMockS3()
	headOK(mock, int64(len(payload)), map[string]string{"checksum": sriOf(t, payload)})
	getOK(mock, payload)

	var dst bytes.Buffer
	err := Download(t.Context(), mock.client(t), "my-bucket", "my/key", &dst)
	require.NoError(t, err)
	assert.Equal(t, payload, dst.Bytes())
}

func TestDownload_ChecksumFromMetadata_Mismatches(t *testing.T) {
	payload := smallPayload()
	wrong := sriOf(t, []byte("not the payload"))
	require.NotEqual(t, wrong, sriOf(t, payload))

	mock := newMockS3()
	headOK(mock, int64(len(payload)), map[string]string{"checksum": wrong})
	getOK(mock, payload)

	var dst bytes.Buffer
	err := Download(t.Context(), mock.client(t), "my-bucket", "my/key", &dst)
	require.Error(t, err)

	var mismatch *ErrChecksumMismatch
	require.Truef(t, errors.As(err, &mismatch), "expected ErrChecksumMismatch, got %v", err)
	assert.Equal(t, wrong, mismatch.Expected)
	assert.NotEmpty(t, mismatch.Actual)
	assert.Equal(t, sriOf(t, payload), mismatch.Actual)
}

func TestDownload_ExpectedChecksumOverride(t *testing.T) {
	payload := smallPayload()
	correct := sriOf(t, payload)

	mock := newMockS3()
	// S3 metadata carries a WRONG checksum; the caller-supplied ExpectedChecksum (correct) must win.
	headOK(mock, int64(len(payload)), map[string]string{"checksum": sriOf(t, []byte("wrong"))})
	getOK(mock, payload)

	var dst bytes.Buffer
	err := Download(t.Context(), mock.client(t), "my-bucket", "my/key", &dst,
		func(o *DownloadOptions) { o.ExpectedChecksum = correct })
	require.NoError(t, err)
	assert.Equal(t, payload, dst.Bytes())
}

func TestDownload_HeadObjectError(t *testing.T) {
	mock := newMockS3()
	mock.on("HeadObject", func(any) (any, error) {
		return nil, httpStatusError("HeadObject", 403)
	})

	var dst bytes.Buffer
	err := Download(t.Context(), mock.client(t), "my-bucket", "my/key", &dst)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "head object error")
	assert.Equal(t, 0, mock.count("GetObject"))
}

func TestWithExpectedBucketOwner_StampsBothInputs(t *testing.T) {
	payload := smallPayload()
	owner := "123456789012"

	mock := newMockS3()
	// capture the ExpectedBucketOwner off each operation's input to prove the option applied to both.
	var headOwner, getOwner *string
	mock.on("HeadObject", func(in any) (any, error) {
		headOwner = in.(*s3.HeadObjectInput).ExpectedBucketOwner
		return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(payload)))}, nil
	})
	mock.on("GetObject", func(in any) (any, error) {
		getOwner = in.(*s3.GetObjectInput).ExpectedBucketOwner
		return &s3.GetObjectOutput{Body: readCloser(string(payload)), ContentLength: aws.Int64(int64(len(payload)))}, nil
	})

	var dst bytes.Buffer
	err := Download(t.Context(), mock.client(t), "my-bucket", "my/key", &dst, WithExpectedBucketOwner(&owner))
	require.NoError(t, err)
	require.NotNil(t, headOwner)
	require.NotNil(t, getOwner)
	assert.Equal(t, owner, *headOwner)
	assert.Equal(t, owner, *getOwner)
}

func TestWithExpectedBucketOwner_NilIsNoop(t *testing.T) {
	opts := &DownloadOptions{}
	WithExpectedBucketOwner(nil)(opts)
	assert.Nil(t, opts.HeadObjectInputOptions)
	assert.Nil(t, opts.GetObjectInputOptions)
}

func TestErrChecksumMismatch_ErrorAndHelper(t *testing.T) {
	err := &ErrChecksumMismatch{Expected: "sha256-aaa", Actual: "sha256-bbb"}
	assert.Contains(t, err.Error(), "sha256-aaa")
	assert.Contains(t, err.Error(), "sha256-bbb")

	got, ok := IsErrChecksumMismatch(fmt.Errorf("wrapped: %w", err))
	assert.True(t, ok)
	require.NotNil(t, got)
	assert.Equal(t, "sha256-aaa", got.Expected)

	_, ok = IsErrChecksumMismatch(errors.New("unrelated"))
	assert.False(t, ok)
}
