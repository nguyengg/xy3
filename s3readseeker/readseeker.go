package s3readseeker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ReadSeeker uses ranged GetObject to implement io.ReadSeeker and io.ReaderAt.
type ReadSeeker interface {
	io.ReadSeeker
	io.ReaderAt

	// Size returns the size of the S3 object that was determined from the initial HeadObject.
	Size() int64
}

// ReadSeekerClient abstracts the S3 APIs that are needed to implement ReadSeeker.
type ReadSeekerClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// DefaultBufferSize is the default value for Options.BufferSize.
const DefaultBufferSize = 64 * 1024

// Options customises New.
type Options struct {
	// BufferSize is used to provide buffered read-ahead for every Read all.
	//
	// By default, DefaultBufferSize is used so that consequential small Reads don't end up with several GetObject
	// calls if one bigger GetObject call is more efficient.
	//
	// Pass zero or a negative value to disable this feature.
	BufferSize int

	// CtxFn returns a context.Context to be used with every GetObject or HeadObject call.
	//
	// By default, context.Background is used.
	CtxFn func() context.Context

	// ModifyGetObjectInput can be used to modify the GetObject input parameters such as adding ExpectedBucketOwner.
	//
	// Its return value will be used to make the GetObject call.
	ModifyGetObjectInput func(*s3.GetObjectInput) *s3.GetObjectInput

	// ModifyHeadObjectInput can be used to modify the HeadObject input parameters such as adding
	// ExpectedBucketOwner.
	//
	// Its return value will be used to make the HeadObject call. Used only by New.
	ModifyHeadObjectInput func(input *s3.HeadObjectInput) *s3.HeadObjectInput
}

// New returns a ReadSeeker with the given bucket and key.
//
// The client will be used to determine a valid size for the file.
func New(client ReadSeekerClient, bucket, key string, optFns ...func(*Options)) (ReadSeeker, error) {
	opts := &Options{
		BufferSize: DefaultBufferSize,
		CtxFn:      context.Background,
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
		client:     client,
		bucket:     bucket,
		key:        key,
		ctxFn:      opts.CtxFn,
		goiFn:      opts.ModifyGetObjectInput,
		size:       aws.ToInt64(headObjectOutput.ContentLength),
		bufferSize: opts.BufferSize,
	}, nil
}

// readSeeker implements io.Seeker on top of reader.
type readSeeker struct {
	client      ReadSeekerClient
	bucket, key string
	ctxFn       func() context.Context
	goiFn       func(*s3.GetObjectInput) *s3.GetObjectInput
	off, size   int64
	buf         bytes.Buffer
	bufferSize  int
}

func (r *readSeeker) Size() int64 {
	return r.size
}

func (r *readSeeker) Read(p []byte) (n int, err error) {
	m := len(p)
	if m == 0 {
		return 0, nil
	}

	// always uses from buffer if possible.
	if r.buf.Len() > m {
		n, err = r.buf.Read(p)
		r.off += int64(n)
		return
	}

	// if len(p) is less than bufferSize, we'll fill the buffer with the next batch then read from buffer again.
	rangeStart := r.off + int64(r.buf.Len())
	if rangeStart >= r.size {
		// r.buf contains remaining bytes.
		n, err = r.buf.Read(p)
		r.off += int64(n)
		return n, io.EOF
	}

	rangeEnd := min(r.size-1, r.off+int64(max(m, r.bufferSize)))
	getObjectOutput, err := r.client.GetObject(r.ctxFn(), r.goiFn(&s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
	}))
	if err != nil {
		return 0, err
	}

	_, err = r.buf.ReadFrom(getObjectOutput.Body)
	if _ = getObjectOutput.Body.Close(); err != nil {
		return 0, err
	}

	n, err = r.buf.Read(p)
	r.off += int64(n)
	return
}

func (r *readSeeker) ReadAt(p []byte, off int64) (n int, err error) {
	m := int64(len(p))
	if m == 0 {
		return 0, nil
	}

	getObjectOutput, err := r.client.GetObject(r.ctxFn(), r.goiFn(&s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, min(r.size-1, off+(m-1)))),
	}))
	if err != nil {
		return 0, err
	}

	n, err = getObjectOutput.Body.Read(p)
	_ = getObjectOutput.Body.Close()
	return
}

var ErrSeekBeforeFirstByte = errors.New("seek ends up before first byte")
var ErrSeekPastLastByte = errors.New("seek ends up past of last byte")

func (r *readSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.off = offset
		r.buf.Reset()
	case io.SeekCurrent:
		r.off += offset
		if offset > 0 {
			r.buf.Next(int(offset))
		} else {
			r.buf.Reset()
		}
	case io.SeekEnd:
		r.off = r.size + offset
		r.buf.Reset()
	}

	if r.off < 0 {
		return r.off, ErrSeekBeforeFirstByte
	}
	if r.off >= r.size {
		return r.off, ErrSeekPastLastByte
	}

	return r.off, nil
}
