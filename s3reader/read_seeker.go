package s3reader

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ReadSeeker uses ranged GetObject to implement several io.Reader-like methods.
type ReadSeeker interface {
	io.ReadSeeker
	io.ReaderAt
}

// ReadSeekerClient abstracts the APIs that are needed to implement ReadSeeker.
type ReadSeekerClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// NewReaderSeeker returns a ReadSeeker with the given bucket and key.
//
// The client will be used to determine a valid size for the file.
func NewReaderSeeker(client ReadSeekerClient, bucket, key string, optFns ...func(*Options)) (ReadSeeker, error) {
	opts := &Options{
		CtxFn: context.Background,
		ModifyGetObjectInput: func(input *s3.GetObjectInput) *s3.GetObjectInput {
			return input
		},
		ModifyHeadObjectInput: func(input *s3.HeadObjectInput) *s3.HeadObjectInput {
			return input
		},
	}
	for _, fn := range optFns {
		fn(opts)
	}

	headObjectOutput, err := client.HeadObject(opts.CtxFn(), opts.ModifyHeadObjectInput(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}))
	if err != nil {
		return nil, fmt.Errorf("determine file size error: %w", err)
	}

	return &readSeeker{
		reader: reader{
			client:               client,
			bucket:               bucket,
			key:                  key,
			ctxFn:                opts.CtxFn,
			modifyGetObjectInput: opts.ModifyGetObjectInput,
		},
		size: aws.ToInt64(headObjectOutput.ContentLength),
	}, nil
}

// NewReaderWithSize returns a ReadSeeker with the given bucket, key, and size.
func NewReaderWithSize(client ReaderClient, bucket, key string, size int64, optFns ...func(*Options)) ReadSeeker {
	opts := &Options{
		CtxFn: context.Background,
		ModifyGetObjectInput: func(input *s3.GetObjectInput) *s3.GetObjectInput {
			return input
		},
	}
	for _, fn := range optFns {
		fn(opts)
	}

	return &readSeeker{
		reader: reader{
			client:               client,
			bucket:               bucket,
			key:                  key,
			ctxFn:                opts.CtxFn,
			modifyGetObjectInput: opts.ModifyGetObjectInput,
		},
		size: size,
	}
}

// readSeeker implements io.Seeker on top of reader.
type readSeeker struct {
	reader
	size int64
}

func (r *readSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if r.off = offset; offset >= r.size {
			return 0, errors.New("seek would end up past end of file")
		}

		r.buf.Reset()
	case io.SeekCurrent:
		if r.off += offset; offset >= r.size {
			return 0, errors.New("seek would end up past end of file")
		}

		r.buf.Next(int(offset))
	case io.SeekEnd:
		r.off = r.size + offset
		r.buf.Reset()
	}

	return r.off, nil
}
