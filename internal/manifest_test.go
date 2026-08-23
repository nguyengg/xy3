package internal

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifest_SaveTo_MinimalRoundTrip(t *testing.T) {
	original := Manifest{
		Bucket: "my-bucket",
		Key:    "path/to/object",
	}

	// SaveTo → temp file → LoadManifestFromFile.
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")

	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, original.SaveTo(f))
	require.NoError(t, f.Close())

	loaded, err := LoadManifestFromFile(path)
	require.NoError(t, err)

	assert.Equal(t, original.Bucket, loaded.Bucket)
	assert.Equal(t, original.Key, loaded.Key)
	assert.Nil(t, loaded.ExpectedBucketOwner)
	assert.Zero(t, loaded.Size)
	assert.Equal(t, "", loaded.Checksum)
}

func TestManifest_SaveTo_FullRoundTrip(t *testing.T) {
	original := Manifest{
		Bucket:              "my-bucket",
		Key:                 "path/to/object",
		ExpectedBucketOwner: aws.String("123456789012"),
		Size:                4096,
		Checksum:            "sha256-abc",
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")

	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, original.SaveTo(f))
	require.NoError(t, f.Close())

	loaded, err := LoadManifestFromFile(path)
	require.NoError(t, err)

	assert.Equal(t, original.Bucket, loaded.Bucket)
	assert.Equal(t, original.Key, loaded.Key)
	require.NotNil(t, loaded.ExpectedBucketOwner)
	assert.Equal(t, aws.ToString(original.ExpectedBucketOwner), aws.ToString(loaded.ExpectedBucketOwner))
	assert.Equal(t, original.Size, loaded.Size)
	assert.Equal(t, original.Checksum, loaded.Checksum)
}

func TestManifest_SaveTo_JSONShape(t *testing.T) {
	// verify Size and Checksum use omitempty when zero-valued.
	m := Manifest{Bucket: "b", Key: "k"}
	var buf bytes.Buffer
	require.NoError(t, m.SaveTo(&buf))

	out := buf.String()
	assert.Contains(t, out, `"bucket": "b"`)
	assert.Contains(t, out, `"key": "k"`)
	assert.NotContains(t, out, `"size"`)
	assert.NotContains(t, out, `"checksum"`)
	assert.NotContains(t, out, `"expectedBucketOwner"`)
}

func TestLoadManifestFromFile_RejectsUnknownField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")
	// unknown "surprise" field must be rejected by DisallowUnknownFields.
	err := os.WriteFile(path, []byte(`{"bucket":"b","key":"k","surprise":42}`), 0o600)
	require.NoError(t, err)

	_, err = LoadManifestFromFile(path)
	require.Error(t, err)
	assert.True(t,
		strings.Contains(err.Error(), "unknown field") ||
			strings.Contains(err.Error(), "surprise"),
		"expected error to mention the unknown field, got: %v", err)
}

func TestLoadManifestFromFile_MissingFile(t *testing.T) {
	_, err := LoadManifestFromFile(filepath.Join(t.TempDir(), "does-not-exist.json"))
	assert.Error(t, err)
}
