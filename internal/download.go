package internal

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/schollz/progressbar/v3"
)

// Download downloads S3 object and writes to the given io.Writer.
//
// If the checksum mismatches, ErrChecksumMismatch will be returned.
func Download(ctx context.Context, client *s3.Client, bucket, key string, dst io.Writer) error {
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
		s3reader.WithProgressBar(progressbar.OptionSetDescription(fmt.Sprintf(`downloading "%s"`, path.Base(key)))))
	if err != nil {
		return fmt.Errorf("create s3 reader error: %w", err)
	}

	// if the object's metadata contains a checksum, use it during download and writing to file.
	var (
		checksum = headObjectResult.Metadata["checksum"]
		verifier sri.Verifier
	)
	if checksum != "" {
		verifier, _ = sri.NewVerifier(checksum)
	}
	if verifier != nil {
		_, err = r.WriteTo(io.MultiWriter(dst, verifier))
	} else {
		_, err = r.WriteTo(dst)
	}

	if _ = r.Close(); err != nil {
		return fmt.Errorf("download error: %w", err)
	}

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
