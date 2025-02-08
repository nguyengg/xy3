package xy3

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var (
	bucket    = flag.String("bucket", "", "S3 bucket to test uploading file.")
	keyPrefix = flag.String("key-prefix", "", "S3 key prefix to test uploading file.")
)

func TestUploadStream(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	flag.Parse()

	if bucket == nil || aws.ToString(bucket) == "" {
		t.Skip("skipping due to missing bucket")
	}

	// fill random data then upload them to S3. the S3 content is then downloaded to verify that they match the
	// expected data.
	expected := make([]byte, 5*1024*1024)
	_, err := io.ReadFull(rand.Reader, expected)
	assert.NoErrorf(t, err, "fill random data error: %v", err)

	tests := []struct {
		name              string
		checksumAlgorithm types.ChecksumAlgorithm
		checksumType      types.ChecksumType
	}{
		{
			name:              "crc32 full object",
			checksumAlgorithm: types.ChecksumAlgorithmCrc32,
			checksumType:      types.ChecksumTypeFullObject,
		},
		{
			name:              "crc32 composite",
			checksumAlgorithm: types.ChecksumAlgorithmCrc32,
			checksumType:      types.ChecksumTypeComposite,
		},
		{
			name:              "crc32c full object",
			checksumAlgorithm: types.ChecksumAlgorithmCrc32c,
			checksumType:      types.ChecksumTypeFullObject,
		},
		{
			name:              "crc32c composite",
			checksumAlgorithm: types.ChecksumAlgorithmCrc32c,
			checksumType:      types.ChecksumTypeComposite,
		},
		{
			name:              "sha1 composite",
			checksumAlgorithm: types.ChecksumAlgorithmSha1,
			checksumType:      types.ChecksumTypeComposite,
		},
		{
			name:              "sha256 composite",
			checksumAlgorithm: types.ChecksumAlgorithmSha256,
			checksumType:      types.ChecksumTypeComposite,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			cfg, err := config.LoadDefaultConfig(ctx)
			client := s3.NewFromConfig(cfg)
			assert.NoErrorf(t, err, "LoadDefaultConfig() error = %v", err)

			// use a random key to prevent conflict.
			key := aws.ToString(keyPrefix) + uuid.NewString()
			defer func() {
				_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: bucket,
					Key:    &key,
				})
			}()

			_, err = UploadStream(ctx, client, bytes.NewReader(expected), &s3.CreateMultipartUploadInput{
				Bucket:            bucket,
				Key:               &key,
				ChecksumAlgorithm: tt.checksumAlgorithm,
				ChecksumType:      tt.checksumType,
			})
			assert.NoErrorf(t, err, "UploadStream() error = %v", err)

			// download to memory and compare.
			getObjectOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: bucket,
				Key:    &key,
			})
			assert.NoErrorf(t, err, "GetObject() error = %v", err)
			defer getObjectOutput.Body.Close()

			var buf bytes.Buffer
			_, err = io.Copy(&buf, getObjectOutput.Body)
			assert.NoErrorf(t, err, "Copy getObjectOutput.Body error = %v", err)

			assert.Equalf(t, expected, buf.Bytes(), "uploaded content does not match original")
		})
	}
}
