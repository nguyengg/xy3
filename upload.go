package xy3

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3writer"
	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

// UploadOptions customises Upload.
type UploadOptions struct {
	// S3WriterOptions customises s3writer.Options.
	S3WriterOptions func(*s3writer.Options)

	// PutObjectInputOptions can be used to modify the s3.PutObjectInput passed to s3writer.New.
	//
	// Useful if you need to add ExpectedBucketOwner or StorageClass.
	PutObjectInputOptions func(*s3.PutObjectInput)
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
	putObjectInput := &s3.PutObjectInput{Bucket: &bucket, Key: &key}

	// if src implements io.ReadSeeker then we can compute checksum first to add them as S3 metadata while also
	// computing the size in order to provide better upload progress reporting.
	// if not, we'll add the checksum only to manifest instead.
	name, size, err := computeChecksum(ctx, src, putObjectInput)
	if err != nil {
		return man, fmt.Errorf("precompute checksum error: %w", err)
	}
	if opts.PutObjectInputOptions != nil {
		opts.PutObjectInputOptions(putObjectInput)
	}

	// always compute size and checksum again as part of uploading.
	// the progress bar can have known size or not, as well as known name or not.
	var (
		sizer       = &util.Sizer{}
		checksummer = internal.DefaultChecksum()
		bar         io.WriteCloser
	)

	if name != "" {
		bar = tspb.DefaultBytes(size, fmt.Sprintf(`uploading "%s"`, filepath.Base(name)))
	} else {
		bar = tspb.DefaultBytes(size, "uploading")
	}

	// now upload to s3.
	w, err := s3writer.New(ctx, client, putObjectInput, func(s3writerOpts *s3writer.Options) {
		if opts.S3WriterOptions != nil {
			opts.S3WriterOptions(s3writerOpts)
		}
	})
	if err != nil {
		return man, fmt.Errorf("create s3 writer error: %w", err)
	}

	_, err = w.ReadFrom(io.TeeReader(src, io.MultiWriter(bar, sizer, checksummer)))
	_ = bar.Close()
	if err != nil {
		return man, fmt.Errorf("upload to s3 error: %w", err)
	}
	if err = w.Close(); err != nil {
		return man, fmt.Errorf("close s3 writer error: %w", err)
	}

	man.Size = sizer.Size
	man.Checksum = checksummer.SumToString(nil)
	return
}

func computeChecksum(ctx context.Context, src io.Reader, input *s3.PutObjectInput) (name string, size int64, err error) {
	rs, ok := src.(io.ReadSeeker)
	if !ok {
		return "", -1, nil
	}

	if f, ok := rs.(*os.File); ok {
		name = f.Name()

		if fi, err := f.Stat(); err != nil {
			return name, -1, fmt.Errorf(`stat file "%s" error: %w`, f.Name(), err)
		} else {
			size = fi.Size()
		}
	}

	var (
		sizer       = &util.Sizer{}
		checksummer = internal.DefaultChecksum()
		bar         io.WriteCloser
	)

	if name != "" {
		bar = tspb.DefaultBytes(size, fmt.Sprintf(`computing checksum of "%s"`, filepath.Base(name)))
	} else {
		bar = tspb.DefaultBytes(size, "computing checksum")
	}

	rsc := util.ResetOnCloseReadSeeker(rs)
	_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(sizer, checksummer), io.TeeReader(rsc, bar), nil)
	if err == nil {
		err = rsc.Close()
	}
	if err != nil {
		return name, size, err
	}

	size = sizer.Size
	input.Metadata = map[string]string{"checksum": checksummer.SumToString(nil)}
	return
}
