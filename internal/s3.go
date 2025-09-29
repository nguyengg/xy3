package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// FindUnusedS3Key returns an S3 key pointing to a non-existing S3 object.
//
// The returned key will be in format `{prefix}{stem}{ext}`, `{prefix}{stem}-1{ext}`, or `{prefix}{stem}-2{ext}`, and so
// on.
func FindUnusedS3Key(ctx context.Context, client *s3.Client, bucket, prefix, stem, ext string) (string, error) {
	key := prefix + stem + ext
	for i := 0; ; {
		if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			// TODO support ExpectedBucketOwner
		}); err != nil {
			if errors.Is(err, context.Canceled) {
				return "", err
			}

			var re *awshttp.ResponseError
			if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
				break
			}

			return "", fmt.Errorf("find unused S3 key error: %w", err)
		}
		i++
		key = fmt.Sprintf("%s%s-%d%s", prefix, stem, i, ext)
	}

	return key, nil
}

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
	if len(parts) > 1 {
		key = parts[1]
	}
	return
}
