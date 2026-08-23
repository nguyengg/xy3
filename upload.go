package xy3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/go-aws-commons/s3writer"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/go-aws-commons/tspb"

	"github.com/nguyengg/xy3/internal"
)

// UploadOptions customises Upload.
type UploadOptions struct {
	// S3WriterOptions customises s3writer.Options.
	S3WriterOptions func(*s3writer.Options)

	// PutObjectInputOptions can be used to modify the s3.PutObjectInput passed to s3writer.New.
	//
	// Useful if you need to add ExpectedBucketOwner or StorageClass.
	PutObjectInputOptions func(*s3.PutObjectInput)

	// ExpectedChecksum can be given to skip precomputing process.
	//
	// However, another checksum is still computed during uploading itself, and if this checksum doesn't match the
	// expected value, the upload will fail with ErrChecksumMismatch.
	ExpectedChecksum string

	// ExpectedSize can be given to provide better progress report.
	//
	// Should be given if ExpectedChecksum is also given, since computing the checksum is going to read the entire
	// file anyway. Nothing happens if the final upload size doesn't match ExpectedSize.
	ExpectedSize int64
}

// Upload uploads the given io.Reader contents to S3 and produces a manifest for the uploaded object.
//
// If src implements io.ReadSeeker, its checksum and size will be precomputed so that the checksum can be added to S3
// metadata while the size will be used during progress report. The checksum and size included in the returned manifest
// are computed during the second pass when uploading to S3. As a result, it is possible for the manifest's checksum to
// be different from the S3 metadata checksum if the src io.Reader is not returning the same bytes for both passes.
//
// If src does not implement io.ReadSeeker, the checksum is only included in the returned manifest.
func Upload(ctx context.Context, client *s3.Client, src io.Reader, bucket, key string, optFns ...func(*UploadOptions)) (man internal.Manifest, err error) {
	opts := &UploadOptions{}
	for _, fn := range optFns {
		fn(opts)
	}

	man.Bucket, man.Key = bucket, key

	var (
		name             string
		size             int64 = -1
		sizer                  = &commons.Sizer{}
		expectedChecksum       = opts.ExpectedChecksum
		verifier         sri.Verifier
		bar              io.WriteCloser
	)

	// if checksum and size are given from opts then use them to skip precomputing.
	// during upload, however, we'll always compute size and checksum again to write to manifest.
	// the progress bar can have known size or not, as well as known name or not.
	if opts.ExpectedSize > 0 {
		size = opts.ExpectedSize
	}
	if expectedChecksum == "" {
		if name, size, expectedChecksum, err = computeChecksum(ctx, src); err != nil {
			return man, fmt.Errorf("precompute checksum error: %w", err)
		}
	}

	// if the caller supplied (or we precomputed) an expected checksum, wire up a verifier that
	// will fail SumAndVerify on mismatch. otherwise the source is non-seekable (documented as
	// supported) — we compute the checksum during upload without pre-verification, so use an
	// always-true verifier that still hashes as bytes flow through.
	if expectedChecksum != "" {
		if verifier, _ = sri.NewVerifier(expectedChecksum); verifier == nil {
			return man, fmt.Errorf("unknown expected checksum: %s", expectedChecksum)
		}
	} else {
		verifier = &internal.AlwaysTrueVerifier{Hash: internal.DefaultChecksum()}
	}

	putObjectInput := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	// only stamp the "checksum" metadata when we actually have a value to store.
	if expectedChecksum != "" {
		putObjectInput.Metadata = map[string]string{"checksum": expectedChecksum}
	}

	if opts.PutObjectInputOptions != nil {
		opts.PutObjectInputOptions(putObjectInput)
	}

	// now upload to s3. wrap the original context so that if verifying checksum fails, we'll cancel the context
	// to force the AbortMultipartUpload to be called.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if name != "" {
		bar = tspb.DefaultBytes(size, fmt.Sprintf(`uploading %q`, internal.TruncateRightWithSuffix(filepath.Base(name), 15, "...")))
	} else {
		bar = tspb.DefaultBytes(size, "uploading")
	}
	defer bar.Close()

	w, err := s3writer.New(ctx, client, putObjectInput, func(s3writerOpts *s3writer.Options) {
		if opts.S3WriterOptions != nil {
			opts.S3WriterOptions(s3writerOpts)
		}
	})
	if err != nil {
		return man, fmt.Errorf("create s3 writer error: %w", err)
	}

	_, err = w.ReadFrom(io.TeeReader(src, io.MultiWriter(bar, sizer, verifier)))
	if err != nil {
		return man, fmt.Errorf("upload to s3 error: %w", err)
	}

	// before closing and finishing multipart upload, let's verify checksum one more time.
	// if this verification fails, don't complete the multipart upload.
	if !verifier.SumAndVerify(nil) {
		cancel()

		mismatch := &ErrChecksumMismatch{
			Expected: expectedChecksum,
			Actual:   verifier.SumToString(nil),
		}

		// surface the abort outcome — s3writer.Close returns MultipartUploadError carrying
		// AbortSuccess / AbortFailure / AbortErr, so a failed AbortMultipartUpload (which
		// would orphan multipart parts in S3) is observable via errors.As on the returned error.
		if cerr := w.Close(); cerr != nil {
			return man, errors.Join(mismatch, fmt.Errorf("multipart abort: %w", cerr))
		}
		return man, mismatch
	}

	if err = w.Close(); err != nil {
		return man, fmt.Errorf("close s3 writer error: %w", err)
	}

	man.Size = sizer.Size
	man.Checksum = verifier.SumToString(nil)
	return
}

func computeChecksum(ctx context.Context, src io.Reader) (name string, size int64, checksum string, err error) {
	rs, ok := src.(io.ReadSeeker)
	if !ok {
		return "", -1, "", nil
	}

	var (
		sizer       = &commons.Sizer{}
		checksummer = internal.DefaultChecksum()
		bar         io.WriteCloser
	)
	size = -1

	if f, ok := rs.(*os.File); ok {
		name = f.Name()

		if fi, err := f.Stat(); err != nil {
			return name, 0, "", fmt.Errorf(`stat file "%s" error: %w`, f.Name(), err)
		} else {
			size = fi.Size()
		}
	}

	if name != "" {
		bar = tspb.DefaultBytes(size, fmt.Sprintf(`computing checksum of %q`, internal.TruncateRightWithSuffix(filepath.Base(name), 15, "...")))
	} else {
		bar = tspb.DefaultBytes(size, "computing checksum")
	}
	defer bar.Close()

	rsc := internal.ResetOnCloseReadSeeker(rs)
	_, err = commons.CopyBufferWithContext(ctx, io.MultiWriter(sizer, checksummer), io.TeeReader(rsc, bar), nil)
	if err == nil {
		err = rsc.Close()
	}
	if err != nil {
		return name, 0, "", err
	}

	return name, sizer.Size, checksummer.SumToString(nil), nil
}
