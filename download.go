package xy3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/go-aws-commons/tspb"
)

// DownloadOptions customises Download.
type DownloadOptions struct {
	// S3ReaderOptions customises s3reader.Options.
	S3ReaderOptions func(*s3reader.Options)

	// ExpectedChecksum provides an alternative checksum to verify against.
	//
	// By default, if the S3 object has metadata attribute named "checksum", its value will be used. ExpectedChecksum
	// will override this.
	ExpectedChecksum string
}

// Download downloads S3 object and writes to the given io.Writer.
//
// If the checksum mismatches, ErrChecksumMismatch will be returned.
func Download(ctx context.Context, client *s3.Client, bucket, key string, dst io.Writer, optFns ...func(*DownloadOptions)) error {
	opts := &DownloadOptions{}
	for _, fn := range optFns {
		fn(opts)
	}

	// headObject to see if there's a checksum to be used. the response's size is also used.
	headObjectResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return fmt.Errorf("head object error: %w", err)
	}

	r, err := s3reader.NewReaderWithSize(
		ctx,
		client,
		&s3.GetObjectInput{Bucket: &bucket, Key: &key},
		aws.ToInt64(headObjectResult.ContentLength),
		func(s3readerOpts *s3reader.Options) {
			if opts.S3ReaderOptions != nil {
				opts.S3ReaderOptions(s3readerOpts)
			}
		})
	if err != nil {
		return fmt.Errorf("create s3 reader error: %w", err)
	}

	bar := tspb.DefaultBytes(aws.ToInt64(headObjectResult.ContentLength), fmt.Sprintf(`downloading "%s"`, path.Base(key)))

	var (
		checksum = headObjectResult.Metadata["checksum"]
		verifier sri.Verifier
	)
	if opts.ExpectedChecksum != "" {
		checksum = opts.ExpectedChecksum
	}
	if checksum != "" {
		verifier, _ = sri.NewVerifier(checksum)
	}
	if verifier != nil {
		_, err = r.WriteTo(io.MultiWriter(dst, bar, verifier))
	} else {
		_, err = r.WriteTo(io.MultiWriter(dst, bar))
	}

	if _ = r.Close(); err != nil {
		return fmt.Errorf("download error: %w", err)
	}

	_ = bar.Close()

	if verifier != nil && !verifier.SumAndVerify(nil) {
		return &ErrChecksumMismatch{Expected: checksum, Actual: verifier.SumToString(nil)}
	}

	return nil
}

type ErrChecksumMismatch struct {
	Expected string
	Actual   string
}

func (e *ErrChecksumMismatch) Error() string {
	return fmt.Sprintf("checksum does not match: expect %s, got %s", e.Expected, e.Actual)
}

func IsErrChecksumMismatch(err error) (t *ErrChecksumMismatch, ok bool) {
	ok = errors.As(err, &t)
	return
}
