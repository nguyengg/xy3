package s3reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Reader uses ranged GetObject to implement several io.Reader-like methods.
type Reader interface {
	io.Reader
	io.ReaderAt
}

// ReaderClient abstracts the API that is needed to implement Reader.
type ReaderClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Options customises New.
type Options struct {
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
	// Its return value will be used to make the HeadObject call. This value is only used by NewReaderSeeker.
	ModifyHeadObjectInput func(input *s3.HeadObjectInput) *s3.HeadObjectInput
}

// NewReader returns a Reader with the given bucket and key.
func NewReader(client ReaderClient, bucket, key string, optFns ...func(*Options)) Reader {
	opts := &Options{
		CtxFn: context.Background,
		ModifyGetObjectInput: func(input *s3.GetObjectInput) *s3.GetObjectInput {
			return input
		},
	}
	for _, fn := range optFns {
		fn(opts)
	}

	return &reader{
		client:               client,
		bucket:               bucket,
		key:                  key,
		ctxFn:                opts.CtxFn,
		modifyGetObjectInput: opts.ModifyGetObjectInput,
	}
}

const bufferSize = 64 * 1024

// reader implements only Reader.
type reader struct {
	client               ReaderClient
	bucket, key          string
	ctxFn                func() context.Context
	modifyGetObjectInput func(*s3.GetObjectInput) *s3.GetObjectInput
	off                  int64
	buf                  bytes.Buffer
}

func (o *reader) Read(p []byte) (n int, err error) {
	m := len(p)
	if m == 0 {
		return 0, nil
	}

	// always uses from buffer if possible.
	if o.buf.Len() > m {
		n, err = o.buf.Read(p)
		o.off += int64(n)
		return
	}

	// if len(p) is less than bufferSize, we'll fill the buffer with the next batch then read from buffer again.
	rangeStart := o.off + int64(o.buf.Len())
	rangeEnd := o.off + max(int64(m), bufferSize) - 1
	getObjectOutput, err := o.client.GetObject(o.ctxFn(), o.modifyGetObjectInput(&s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
	}))
	if err != nil {
		return 0, err
	}

	_, err = o.buf.ReadFrom(getObjectOutput.Body)
	if _ = getObjectOutput.Body.Close(); err != nil {
		return 0, err
	}

	n, err = o.buf.Read(p)
	o.off += int64(n)
	return
}

func (o *reader) ReadAt(p []byte, off int64) (n int, err error) {
	m := int64(len(p))
	if m == 0 {
		return 0, nil
	}

	getObjectOutput, err := o.client.GetObject(o.ctxFn(), o.modifyGetObjectInput(&s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+(m-1))),
	}))
	if err != nil {
		return 0, err
	}

	n, err = getObjectOutput.Body.Read(p)
	_ = getObjectOutput.Body.Close()
	return
}
