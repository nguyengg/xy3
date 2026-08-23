package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadIntoLoader(t *testing.T, contents string) *Loader {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".xy3"), []byte(contents), 0o600))
	chdir(t, dir)

	l := &Loader{}
	_, err := l.Load(t.Context())
	require.NoError(t, err)
	return l
}

func TestForUpload_ReturnsConfiguredBucketAndPrefix(t *testing.T) {
	l := loadIntoLoader(t, "[upload]\nbucket = my-bucket\nprefix = my-prefix/\n")
	got := l.ForUpload()
	assert.Equal(t, "my-bucket", got.Bucket)
	assert.Equal(t, "my-prefix/", got.Prefix)
}

func TestForUpload_MissingSectionReturnsZero(t *testing.T) {
	l := loadIntoLoader(t, "; no upload section\n")
	got := l.ForUpload()
	assert.Equal(t, UploadConfig{}, got)
}

func TestForBucket_ReturnsConfiguredValues(t *testing.T) {
	l := loadIntoLoader(t, "[s3://my-bucket]\naws-profile = prod\nexpected-bucket-owner = 123456789012\nstorage-class = STANDARD_IA\n")
	got := l.ForBucket("my-bucket")

	assert.Equal(t, "my-bucket", got.Bucket)
	assert.Equal(t, "prod", got.AWSProfile)
	require.NotNil(t, got.ExpectedBucketOwner)
	assert.Equal(t, "123456789012", aws.ToString(got.ExpectedBucketOwner))
	assert.Equal(t, types.StorageClass("STANDARD_IA"), got.StorageClass)
}

// TestForBucket_MissingKeysReturnNilPointers is the H3 regression test:
// go-ini's Section.Key() never returns nil — it auto-creates the missing key —
// so the previous code set ExpectedBucketOwner = aws.String(""), which S3 then
// used as an owner check and returned 403 AccessDenied.
func TestForBucket_MissingKeysReturnNilPointers(t *testing.T) {
	// section exists, but no expected-bucket-owner / storage-class keys.
	l := loadIntoLoader(t, "[s3://my-bucket]\naws-profile = prod\n")
	got := l.ForBucket("my-bucket")

	assert.Equal(t, "my-bucket", got.Bucket)
	assert.Equal(t, "prod", got.AWSProfile)
	assert.Nil(t, got.ExpectedBucketOwner, "absent key must yield nil pointer, not aws.String(\"\")")
	assert.Equal(t, types.StorageClass(""), got.StorageClass)
}

func TestForBucket_MissingSectionReturnsZero(t *testing.T) {
	l := loadIntoLoader(t, "[s3://other-bucket]\naws-profile = prod\n")
	got := l.ForBucket("my-bucket")
	assert.Equal(t, BucketConfig{}, got)
}

func TestForBucket_EmptyStringValueTreatedAsAbsent(t *testing.T) {
	// explicitly-empty values should not produce a bogus &"" either.
	l := loadIntoLoader(t, "[s3://my-bucket]\nexpected-bucket-owner =\n")
	got := l.ForBucket("my-bucket")
	assert.Nil(t, got.ExpectedBucketOwner)
}
