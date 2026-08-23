package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseS3URI(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "bucket and key",
			text:       "s3://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantKey:    "path/to/object",
		},
		{
			name:       "bucket and single-segment key",
			text:       "s3://my-bucket/key",
			wantBucket: "my-bucket",
			wantKey:    "key",
		},
		{
			name:       "bucket only, no trailing slash",
			text:       "s3://my-bucket",
			wantBucket: "my-bucket",
			wantKey:    "",
		},
		{
			name:       "bucket only, trailing slash",
			text:       "s3://my-bucket/",
			wantBucket: "my-bucket",
			wantKey:    "",
		},
		{
			name:       "deep key preserves internal slashes",
			text:       "s3://b/a/b/c/d.txt",
			wantBucket: "b",
			wantKey:    "a/b/c/d.txt",
		},
		{
			name:       "key with special characters",
			text:       "s3://bucket/key with spaces/and+plus.tar.gz",
			wantBucket: "bucket",
			wantKey:    "key with spaces/and+plus.tar.gz",
		},
		{
			name:    "missing s3 scheme",
			text:    "https://example.com/foo",
			wantErr: true,
		},
		{
			name:    "empty string",
			text:    "",
			wantErr: true,
		},
		{
			name:    "s3 without slashes",
			text:    "s3:my-bucket/key",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParseS3URI(tt.text)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantKey, key)
		})
	}
}
