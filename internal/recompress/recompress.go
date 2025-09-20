package recompress

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/internal/manifest"
)

func (c *Command) recompressArchive(ctx context.Context, manifestName string) error {
	srcManifest, err := manifest.UnmarshalFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// download to temp file.
	_, err = c.download(ctx, srcManifest)
	if err != nil {
		return fmt.Errorf("download original archive error: %w", err)
	}

	// TODO whilst decompressing, compress to .tar.zst right away.
	// need an aferos-like way to access archive contents.
	return nil
}

// findUnusedS3Key returns an S3 key pointing to a non-existing S3 object that can be used to upload file.
func (c *Command) findUnusedS3Key(ctx context.Context, src manifest.Manifest, stem, ext string) (string, error) {
	prefix := filepath.Dir(src.Key)
	if prefix == "." {
		prefix = ""
	} else {
		prefix = prefix + "/"
	}

	key := prefix + stem + ext
	for i := 0; ; {
		if _, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:              aws.String(src.Bucket),
			Key:                 aws.String(key),
			ExpectedBucketOwner: c.ExpectedBucketOwner,
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
