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

	// HeadObjectInputOptions can be used to modify the initial S3 HeadObject request for metadata.
	//
	// Useful if you need to add ExpectedBucketOwner (see WithExpectedBucketOwner).
	HeadObjectInputOptions func(*s3.HeadObjectInput)

	// GetObjectInputOptions can be used to modify the s3.GetObjectInput passed to s3reader.New.
	//
	// Useful if you need to add ExpectedBucketOwner (see WithExpectedBucketOwner).
	GetObjectInputOptions func(*s3.GetObjectInput)

	// ExpectedChecksum provides an alternative checksum to verify against.
	//
	// By default, if the S3 object has metadata attribute named "checksum", its value will be used.
	// ExpectedChecksum will override this.
	ExpectedChecksum string
}

// Download downloads the S3 object specified by its bucket and key, and writes the contents to the given io.Writer.
//
// If the checksum mismatches, ErrChecksumMismatch will be returned.
func Download(ctx context.Context, client *s3.Client, bucket, key string, dst io.Writer, optFns ...func(*DownloadOptions)) error {
	opts := &DownloadOptions{}
	for _, fn := range optFns {
		fn(opts)
	}

	// headObject to see if there's a checksum to be used. the response's size is also used.
	headObjectInput := &s3.HeadObjectInput{Bucket: &bucket, Key: &key}
	if opts.HeadObjectInputOptions != nil {
		opts.HeadObjectInputOptions(headObjectInput)
	}
	headObjectResult, err := client.HeadObject(ctx, headObjectInput)
	if err != nil {
		return fmt.Errorf("head object error: %w", err)
	}

	getObjectInput := &s3.GetObjectInput{Bucket: &bucket, Key: &key}
	if opts.GetObjectInputOptions != nil {
		opts.GetObjectInputOptions(getObjectInput)
	}
	r, err := s3reader.NewReaderWithSize(
		ctx,
		client,
		getObjectInput,
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
	defer bar.Close()

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

	_ = bar.Finish()

	if verifier != nil && !verifier.SumAndVerify(nil) {
		return &ErrChecksumMismatch{Expected: checksum, Actual: verifier.SumToString(nil)}
	}

	return nil
}

// WithExpectedBucketOwner modifies the download options to include the given expected bucket owner.
//
// For convenience, if the expectedBucketOwner argument is nil, the method does nothing. If
// DownloadOptions.HeadObjectInputOptions and/or DownloadOptions.GetObjectInputOptions were already specified, they will
// be run prior to overriding the ExpectedBucketOwner field.
func WithExpectedBucketOwner(expectedBucketOwner *string) func(*DownloadOptions) {
	if expectedBucketOwner == nil {
		return func(_ *DownloadOptions) {
		}
	}

	return func(opts *DownloadOptions) {
		hfn := opts.HeadObjectInputOptions
		opts.HeadObjectInputOptions = func(input *s3.HeadObjectInput) {
			if hfn != nil {
				hfn(input)
			}
			input.ExpectedBucketOwner = expectedBucketOwner
		}

		gfn := opts.GetObjectInputOptions
		opts.GetObjectInputOptions = func(input *s3.GetObjectInput) {
			if gfn != nil {
				gfn(input)
			}
			input.ExpectedBucketOwner = expectedBucketOwner
		}
	}
}

// ErrChecksumMismatch is returned by Download if object integrity verification fails.
type ErrChecksumMismatch struct {
	// Expected is the expected checksum available from S3's checksum metadata or from DownloadOptions.ExpectedChecksum.
	Expected string
	// Actual is the actual checksum from computing the hash of the contents being downloaded.
	Actual string
}

func (e *ErrChecksumMismatch) Error() string {
	return fmt.Sprintf("checksum does not match: expect %s, got %s", e.Expected, e.Actual)
}

func IsErrChecksumMismatch(err error) (t *ErrChecksumMismatch, ok bool) {
	ok = errors.As(err, &t)
	return
}
