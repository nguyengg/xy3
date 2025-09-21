package recompress

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/xy3/internal/manifest"
)

func (c *Command) stream(ctx context.Context, cancel context.CancelCauseFunc, man manifest.Manifest) io.ReadCloser {
	// as a challenge, let's try to do everything in-memory, using a pipe so that a new goroutine is responsible
	// for downloading the file to PipeWriter, while main goroutine reads from PipeReader (similar to download's
	// streaming mode).
	pr, pw := io.Pipe()

	go func() {
		defer func() {
			if err := pw.Close(); err != nil {
				c.logger.Printf("close PipeWriter error: %v", err)
			}
		}()

		r, err := s3reader.New(ctx, c.client, &s3.GetObjectInput{
			Bucket:              aws.String(man.Bucket),
			Key:                 aws.String(man.Key),
			ExpectedBucketOwner: man.ExpectedBucketOwner,
		}, func(options *s3reader.Options) {
			options.Concurrency = c.MaxConcurrency
		})
		if err != nil {
			cancel(fmt.Errorf("create s3 reader error: %w", err))
			return
		}
		defer func(r s3reader.Reader) {
			if err := r.Close(); err != nil {
				c.logger.Printf("close s3reader error: %v", err)
			}
		}(r)

		if _, err = r.WriteTo(pw); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			cancel(fmt.Errorf("download from s3 error: %w", err))
		}
	}()

	return pr
}
