package internal

import (
	"fmt"
	"strings"
)

// ParseS3URI parses S3 URIs in format s3://bucket/key.
//
// The only validation that exists right now is that text must start with s3://.
func ParseS3URI(text string) (bucket, key string, err error) {
	// parse S3 URI with optional key prefix. don't bother validating valid bucket names.
	if !strings.HasPrefix(text, "s3://") {
		return "", "", fmt.Errorf("text does not start with s3://")
	}

	parts := strings.SplitN(strings.TrimPrefix(text, "s3://"), "/", 2)
	bucket = parts[0]
	key = parts[1]
	return
}
