package xy3

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	// Useful if you need to add ExpectedBucketOwner or other customisations such as StorageClass.
	PutObjectInputOptions func(*s3.PutObjectInput)
}

// Upload uploads the given io.Reader contents to S3.
func Upload(ctx context.Context, client *s3.Client, src io.Reader, bucket, key string, optFns ...func(*UploadOptions)) (man internal.Manifest, err error) {
	opts := &UploadOptions{}
	for _, fn := range optFns {
		fn(opts)
	}

	man.Bucket = bucket
	man.Key = key

	// if src implements io.ReadSeeker then we can compute checksum first to add them as S3 metadata while also
	// computing the size in order to provide better upload progress reporting.
	// if not, we'll add the checksum to manifest instead.
	var (
		name, checksum string
		size           int64 = -1
	)
	if rs, ok := src.(io.ReadSeeker); ok {
		if f, ok := rs.(*os.File); ok {
			name = f.Name()
		}

		if err = util.ResettableReadSeeker(rs, func(r io.ReadSeeker) error {
			checksum, size, err = computeChecksum(ctx, r)
			return err
		}); err != nil {
			return
		}
	}

	// putObjectInput can be customised.
	putObjectInput := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if checksum != "" {
		putObjectInput.Metadata = map[string]string{"checksum": checksum}
	}
	if opts.PutObjectInputOptions != nil {
		opts.PutObjectInputOptions(putObjectInput)
	}

	// the progress bar can have known size or not, as well as known name or not.
	// if checksum and/or size weren't computed back then, let's compute them now too.
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
		_ = bar.Close()
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

	if checksum == "" {
		checksum = checksummer.SumToString(nil)
	}

	man.Size = sizer.Size
	man.Checksum = checksum
	return
}

func computeChecksum(ctx context.Context, src io.Reader) (string, int64, error) {
	sizer := &util.Sizer{}
	checksummer := internal.DefaultChecksum()

	if f, ok := src.(*os.File); ok {
		fi, err := f.Stat()
		if err != nil {
			return "", 0, fmt.Errorf(`stat file "%s" error: %w`, f.Name(), err)
		}

		bar := tspb.DefaultBytes(fi.Size(), fmt.Sprintf(`computing checksum of "%s"`, filepath.Base(f.Name())))
		_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(sizer, checksummer), io.TeeReader(f, bar), nil)
		_ = bar.Close()
		if err != nil {
			return "", 0, fmt.Errorf(`compute checksum of "%s" error: %w`, f.Name(), err)
		}

		return checksummer.SumToString(nil), sizer.Size, nil
	}

	bar := tspb.DefaultBytes(-1, "computing checksum")
	_, err := util.CopyBufferWithContext(ctx, io.MultiWriter(sizer, checksummer), io.TeeReader(src, bar), nil)
	_ = bar.Close()
	if err != nil {
		return "", 0, fmt.Errorf("compute checksum error: %w", err)
	}

	return checksummer.SumToString(nil), sizer.Size, nil
}
