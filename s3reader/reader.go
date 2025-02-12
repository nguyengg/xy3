package s3reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/internal/executor"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/time/rate"
)

const (
	// DefaultThreshold is the default value for GetOptions.Threshold.
	//
	// S3's [Recommendation] is actually 8MB-16MB.
	//
	// [Recommendation]: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html
	DefaultThreshold = int64(5 * 1024 * 1024)

	// DefaultConcurrency is the default value for Options.Concurrency.
	DefaultConcurrency = 3

	// DefaultPartSize is the default value for Options.PartSize.
	//
	// S3's [Recommendation] is actually 8MB-16MB.
	//
	// [Recommendation]: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html
	DefaultPartSize = int64(5 * 1024 * 1024)

	// DefaultBufferSize is the default value for Options.BufferSize.
	DefaultBufferSize = 1024 * 1024
)

var (
	// ErrSeekBeforeFirstByte is returned by Reader.Seek if the parameters would end up moving the internal read
	// offset to a negative number.
	ErrSeekBeforeFirstByte = errors.New("seek ends up before first byte")

	// ErrSeekPastLastByte is returned by Reader.Seek if the parameters would end up moving the internal read
	// offset past the offset of the last byte (Reader.Size-1).
	ErrSeekPastLastByte = errors.New("seek ends up past of last byte")

	// ErrClosed is returned by all Reader read methods after Close returns.
	ErrClosed = errors.New("reader already closed")
)

// Reader uses ranged GetObject to implement io.ReadSeekCloser, io.ReaderAt, and io.WriterAt.
//
// Each Read, ReadAt, or WriteTo may be done with one GetObject or several smaller GetObject in parallel depending on
// the Options passed to New. Methods from io.ReadSeeker (Read and Seek) and io.WriterAt (WriteTo) will update the
// internal read offset and as a result should not be called concurrently. On the other hand, concurrent calls to ReadAt
// are safe as they do not advance the internal read offset. Once Close returns, however, all subsequent reads will
// return ErrClosed.
type Reader interface {
	// Read reads up to len(p) bytes into p and advances the internal read offset accordingly.
	//
	// Read should not be called concurrently as they share the same internal read offset and buffer. If len(p) is
	// larger than Options.Threshold, Read will use parallel GetObject to retrieve the data with each part
	// downloading up to Options.PartSize in bytes. Otherwise, Read will use the larger of len(p) or
	// Options.BufferSize for one GetObject call to provide buffered read.
	//
	// See io.Reader for more information on the return values.
	Read(p []byte) (int, error)

	// Seek moves the internal read offset for the next Read or WriteTo.
	//
	// See io.Seeker for more information on the return values. This implementation returns either
	// ErrSeekBeforeFirstByte or ErrSeekPastLastByte for invalid seek parameters.
	Seek(offset int64, whence int) (int64, error)

	// Close shuts down the internal goroutine pool that supports parallel GetObject requests.
	//
	// Close does not always have to be called as garbage collection will be able to reclaim the goroutines
	// eventually. If you end up creating a lot of Reader instances, however, it is sensible to Close them as soon
	// as possible.
	Close() error

	// ReadAt reads a specific range of the S3 reader start at offset off and reads no more than len(p) bytes.
	//
	// Concurrent ReadAt calls are safe as they do not advance the internal read offset.
	//
	// See io.ReaderAt for more information on the return values.
	ReadAt(p []byte, off int64) (int, error)

	// WriteTo writes and advances internal read offset until remaining data from S3 is exhausted.
	//
	// See io.WriterTo for more information on the return values.
	WriteTo(dst io.Writer) (int64, error)

	// Size returns the size of the S3 reader that was determined from the initial HeadObject request or given by
	// way of NewReaderWithSize.
	Size() int64

	// Reopen returns a new Reader that with identical settings as this instance but starts at initial state
	// (read offset at first byte).
	//
	// The new instance has its own goroutine pool and, as a result, can Close independently of this instance.
	// Useful if you need to start reading from first byte again but need to keep this instance for some other
	// usage.
	Reopen() Reader
}

// GetObjectClient abstracts the S3 APIs that are needed to implement Reader.
type GetObjectClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// GetAndHeadObjectClient abstracts the S3 APIs that are needed for New to determine the object size.
type GetAndHeadObjectClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Options customises the returned Reader of New and NewReaderWithSize.
type Options struct {
	// Threshold indicates the minimum number of bytes needed for parallel GetObject.
	//
	// If the range is shorter than the threshold, a single GetObject will be used.
	//
	// Default to DefaultThreshold. Must be positive integer.
	Threshold int64

	// Concurrency controls the number of goroutines in the pool that supports parallel GetObject.
	//
	// Default to DefaultConcurrency. Must be a positive integer. Set to 1 to disable the feature.
	//
	// Because a single goroutine pool is shared for all Reader.Read and Reader.ReadAt calls, it is acceptable
	// to set this value to a high number (`runtime.NumCPU()`) and use MaxBytesInSecond instead to add rate
	// limiting.
	Concurrency int

	// MaxBytesInSecond limits the number of bytes that are downloaded in one second.
	//
	// The zero-value indicates no limit. Must be a positive integer otherwise.
	MaxBytesInSecond int64

	// PartSize is the size of each parallel GetObject.
	//
	// Default to DefaultPartSize. Must be a positive integer; unused if Concurrency is 1.
	PartSize int64

	// BufferSize is used to provide buffered read ahead for every Read call.
	//
	// BufferSize provides buffered read to reduce the number of small-range GetObject by making one mid-range
	// GetObject instead, extremely helpful if Reader is being used strictly as an io.Reader.
	//
	// Default to DefaultBufferSize. Must be a non-negative integer. Set to 0 to disable the feature.
	BufferSize int64

	// internal. must use opaque func(*Options) to customise these.
	size   int64
	logger io.WriteCloser
}

// New returns a Reader with the given GetObject input parameters.
//
// The given context will be used for all subsequent S3 calls.
//
// New will call HeadObject using identical input parameters to determine the reader size. If you already know the
// object's size, use NewReaderWithSize instead. New may return a non-nil error from the HeadObject or from
// invalid options.
func New(ctx context.Context, client GetAndHeadObjectClient, input *s3.GetObjectInput, optFns ...func(*Options)) (Reader, error) {
	headObjectOutput, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:               input.Bucket,
		Key:                  input.Key,
		ChecksumMode:         input.ChecksumMode,
		ExpectedBucketOwner:  input.ExpectedBucketOwner,
		IfMatch:              input.IfMatch,
		IfModifiedSince:      input.IfModifiedSince,
		IfNoneMatch:          input.IfNoneMatch,
		IfUnmodifiedSince:    input.IfUnmodifiedSince,
		PartNumber:           input.PartNumber,
		Range:                input.Range,
		RequestPayer:         input.RequestPayer,
		SSECustomerAlgorithm: input.SSECustomerAlgorithm,
		SSECustomerKey:       input.SSECustomerKey,
		SSECustomerKeyMD5:    input.SSECustomerKeyMD5,
		VersionId:            input.VersionId,
	})
	if err != nil {
		return nil, fmt.Errorf("determine file size error: %w", err)
	}

	return NewReaderWithSize(ctx, client, input, aws.ToInt64(headObjectOutput.ContentLength), optFns...)
}

// NewReaderWithSize returns a Reader with the given GetObject input parameters and known size.
//
// The given context will be used for all subsequent S3 calls.
//
// NewReaderWithSize will only return a non-nil error if there are invalid options.
func NewReaderWithSize(ctx context.Context, client GetObjectClient, input *s3.GetObjectInput, size int64, optFns ...func(*Options)) (Reader, error) {
	opts := &Options{
		Threshold:        DefaultThreshold,
		Concurrency:      DefaultConcurrency,
		MaxBytesInSecond: 0,
		PartSize:         DefaultPartSize,
		BufferSize:       DefaultBufferSize,

		// internal.
		size:   size,
		logger: noopLogger{io.Discard},
	}
	for _, fn := range optFns {
		fn(opts)
	}

	if opts.Threshold <= 0 {
		return nil, fmt.Errorf("threshold (%d) must be a positive integer", opts.Threshold)
	}
	if opts.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency (%d) must be a positive integer", opts.Concurrency)
	}
	if opts.PartSize <= 0 && opts.Concurrency != 1 {
		return nil, fmt.Errorf("partSize (%d) must be a positive integer", opts.PartSize)
	}
	if opts.BufferSize < 0 {
		return nil, fmt.Errorf("bufferSize (%d) must be a non-negative integer", opts.PartSize)
	}

	var limiter *rate.Limiter
	if opts.MaxBytesInSecond < 0 {
		return nil, fmt.Errorf("mxBytesInSecond (%d) must be a non-negative integer", opts.MaxBytesInSecond)
	} else if opts.MaxBytesInSecond == 0 {
		limiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		limiter = rate.NewLimiter(rate.Limit(opts.MaxBytesInSecond), int(opts.PartSize))
	}

	return &reader{
		// from options.
		ctx:         ctx,
		client:      client,
		input:       *input,
		threshold:   opts.Threshold,
		concurrency: opts.Concurrency,
		bufferSize:  opts.BufferSize,
		partSize:    opts.PartSize,
		size:        size,
		logger:      opts.logger,

		// internal.
		ex:      executor.NewCallerRunsOnFullExecutor(opts.Concurrency - 1),
		limiter: limiter,
		buf:     &bytes.Buffer{},
		off:     0,
	}, nil
}

// reader implements Reader.
type reader struct {
	// from options.
	ctx                                   context.Context
	client                                GetObjectClient
	input                                 s3.GetObjectInput
	concurrency                           int
	threshold, bufferSize, partSize, size int64
	logger                                io.WriteCloser

	// internal.
	ex      executor.ExecuteCloser
	limiter *rate.Limiter
	buf     *bytes.Buffer
	off     int64
	err     error
}

func (r *reader) Read(p []byte) (n int, err error) {
	if err = r.err; err != nil {
		return
	}

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

	// if r.buf already contains all the remaining bytes then make sure io.EOF is returned here.
	rangeStart := r.off + int64(r.buf.Len())
	if rangeStart >= r.size {
		n, err = r.buf.Read(p)
		r.off += int64(n)
		return n, io.EOF
	}

	// always download either len(p) or bufferSize, whichever is larger, to provide buffered read ahead capability.
	// we do need to clamp rangeEnd to size-1 to prevent reading past EOF.
	rangeEnd := min(r.size-1, r.off+max(int64(m), r.bufferSize))
	if _, err = r.read(r.buf, rangeStart, rangeEnd); err != nil {
		return 0, err
	}

	n, err = r.buf.Read(p)
	r.off += int64(n)
	return
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	if err := r.err; err != nil {
		return 0, err
	}

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

func (r *reader) ReadAt(p []byte, off int64) (int, error) {
	if err := r.err; err != nil {
		return 0, err
	}

	m := len(p)
	if m == 0 {
		return 0, nil
	}

	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	// we do need to clamp rangeEnd to size-1 to prevent reading past EOF.
	rangeEnd := min(r.size-1, off+int64(m)-1)
	if _, err := r.read(bb, off, rangeEnd); err != nil {
		return 0, err
	}

	return copy(p, bb.B), nil
}

func (r *reader) WriteTo(dst io.Writer) (int64, error) {
	if err := r.err; err != nil {
		return 0, err
	}

	// bytes.Buffer.WriteTo never returns EOF.
	n, err := r.buf.WriteTo(dst)
	if err != nil {
		return n, err
	}

	if r.off += n; r.off >= r.size {
		return n, nil
	}

	n, err = r.read(dst, r.off, r.size-1)
	r.off += n
	return n, err
}

func (r *reader) Size() int64 {
	return r.size
}

func (r *reader) Reopen() Reader {
	// logger is only copied over if it implements cloneable.
	// at the moment we don't allow customers to specify their own io.Writer logger so we're in control of this.
	logger := r.logger
	if c, ok := logger.(cloneable); ok {
		logger = c.Clone()
	}

	return &reader{
		// from options.
		ctx:         r.ctx,
		client:      r.client,
		input:       r.input,
		threshold:   r.threshold,
		concurrency: r.concurrency,
		bufferSize:  r.bufferSize,
		partSize:    r.partSize,
		size:        r.size,
		logger:      logger,

		// internal.
		ex:      executor.NewCallerRunsOnFullExecutor(r.concurrency - 1),
		limiter: r.limiter,
		buf:     &bytes.Buffer{},
		off:     0,
	}
}

func (r *reader) Close() error {
	switch {
	case errors.Is(r.err, ErrClosed):
		return r.err
	default:
		_, r.err = r.logger.Close(), ErrClosed
		return r.ex.Close()
	}
}

func (r *reader) read(dst io.Writer, rangeStart, rangeEnd int64) (int64, error) {
	// if the range is smaller than threshold then let's just do everything in one GetObject.
	size := rangeEnd - rangeStart + 1
	if size < r.threshold {
		getObjectOutput, err := r.client.GetObject(r.ctx, copyInput(r.input, rangeStart, rangeEnd))
		if err != nil {
			return 0, err
		}

		written, err := io.Copy(dst, getObjectOutput.Body)
		_ = getObjectOutput.Body.Close()
		return written, err
	}

	partSize := r.partSize
	partCount := int(math.Ceil(float64(size) / float64(partSize)))
	var wg sync.WaitGroup
	wg.Add(partCount)

	w := writer{dst: io.MultiWriter(dst, r.logger)}
	defer w.close()

	ctx, cancel := context.WithCancelCause(r.ctx)
	for partNumber, lastPart := 0, partCount-1; partNumber <= lastPart; partNumber++ {
		select {
		case <-ctx.Done():
			return w.written, ctx.Err()
		default:
			// don't need to copy partNumber and startRange start with go1.22
			// (https://go.dev/blog/loopvar-preview)
			if err := r.ex.Execute(func() {
				defer wg.Done()

				startRange := int64(partNumber) * partSize
				var input *s3.GetObjectInput
				if partNumber == lastPart {
					input = copyInput(r.input, startRange)
				} else {
					input = copyInput(r.input, startRange, startRange+partSize-1)
				}

				output, err := r.client.GetObject(ctx, input)
				if err == nil {
					err = w.write(partNumber, output.Body)
					_ = output.Body.Close()
				}
				if err != nil {
					cancel(err)
				}
			}); err != nil {
				cancel(err)
				return w.written, err
			}
		}
	}

	// wait in a separate goroutine to catch context cancel.
	done := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return w.written, ctx.Err()
	case <-done:
		return w.drain()
	}
}

func copyInput(src s3.GetObjectInput, rangeBytes ...int64) *s3.GetObjectInput {
	input := src

	switch len(rangeBytes) {
	case 1:
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", rangeBytes[0]))
	case 2:
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", rangeBytes[0], rangeBytes[1]))
	case 3:
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d/%d", rangeBytes[0], rangeBytes[1], rangeBytes[2]))
	}

	return &input
}
