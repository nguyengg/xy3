package s3writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nguyengg/xy3/internal/executor"
	"github.com/nguyengg/xy3/internal/hashs3"
	"golang.org/x/time/rate"
)

const (
	// MaxObjectSize is the maximum size of an S3 object.
	//
	// See [Amazon S3 multipart upload limits].
	//
	// [Amazon S3 multipart upload limits]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
	MaxObjectSize = int64(5_497_558_138_880)

	// MaxPartCount is the maximum number of parts per upload.
	//
	// See [Amazon S3 multipart upload limits].
	//
	// [Amazon S3 multipart upload limits]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
	MaxPartCount = 10_000

	// MinPartSize is the minimum number of bytes per part upload.
	//
	// See [Amazon S3 multipart upload limits].
	//
	// [Amazon S3 multipart upload limits]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
	MinPartSize = int64(5_242_880)

	// MaxPartSize is the maximum number of bytes per part upload.
	//
	// See [Amazon S3 multipart upload limits].
	//
	// [Amazon S3 multipart upload limits]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
	MaxPartSize = int64(5_368_709_120)

	// DefaultConcurrency is the default value for Options.Concurrency.
	DefaultConcurrency = 3
)

// ErrClosed is returned by all Writer write methods after Close returns.
var ErrClosed = errors.New("writer already closed")

// Writer uses either a single PutObject or multipart upload to upload content to S3.
//
// Writer is implemented as a buffered partWriter. Close must be called to drain the write buffer and complete the
// multipart upload if multipart upload was used; the return value will be an MultipartUploadError instance in this
// case. If the number of bytes to upload is less than MinPartSize, a single PutObject is used.
//
// Similar to bufio.Writer, if an error occurs writing to Writer, no more data will be accepted and subsequent writes
// including Close will return the initial error.
type Writer interface {
	// Write writes len(p) bytes from p to the write buffer.
	//
	// Every Write may not end up uploading to S3 immediately due to the use of a buffer.
	//
	// See io.Writer for more information on the return values. This method always returns n == len(p) even though
	// the number of bytes uploaded to S3 may be less than n to meet io.Writer requirements for avoiding short
	// writes.
	Write(p []byte) (n int, err error)

	// ReadFrom reads data from src until EOF or error and writes to S3.
	//
	// See io.ReaderFrom for more information on the return values. It returns n as the number of bytes read from
	// src, not the number of bytes uploaded to S3 which can be less than n.
	ReadFrom(src io.Reader) (n int64, err error)

	// Close drains the write buffer and complete the upload if multipart upload is used.
	//
	// After Close completes successfully, subsequent writes including Close will return ErrClosed.
	Close() error
}

// Options customises the returned Writer of NewWriter.
type Options struct {
	// Concurrency controls the number of goroutines in the pool that supports parallel UploadPart.
	//
	// Default to DefaultConcurrency. Must be a positive integer. Set to 1 to disable the feature.
	//
	// Because a single goroutine pool is shared for all Writer.Write calls, it is acceptable to set this value to
	// a high number (`runtime.NumCPU()`) and use MaxBytesInSecond instead to add rate limiting.
	Concurrency int

	// MaxBytesInSecond limits the number of bytes that are uploaded in one second.
	//
	// The zero-value indicates no limit. Must be a positive integer otherwise.
	MaxBytesInSecond int64

	// PartSize is the size of each parallel GetObject.
	//
	// Default to MinPartSize. Must be a positive integer; cannot be lower than MinPartSize.
	//
	// Be mindful of the MaxPartCount limit. Since Writer does not know the size of the content being uploaded,
	// caller must set PartSize to something reasonably large enough so that the number of parts does not exceed
	// limit.
	PartSize int64

	// DisableAbortOnError controls whether AbortMultipartUpload is automatically called on error.
	DisableAbortOnError bool
}

// WriterClient abstracts the S3 APIs that are needed to implement Writer.
type WriterClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// New returns a Writer given the PutObject input parameters.
//
// Unlike s3/manager, you do not pass the content being uploaded via [s3.PutObjectInput.Body] here. Instead, use the
// returned Writer as either an io.Writer or an io.ReaderFrom.
//
// New will only return a non-nil error if there are invalid options.
func New(ctx context.Context, client WriterClient, input *s3.PutObjectInput, optFns ...func(*Options)) (Writer, error) {
	opts := &Options{
		Concurrency:      DefaultConcurrency,
		MaxBytesInSecond: 0,
		PartSize:         MinPartSize,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	if opts.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency (%d) must be a positive integer", opts.Concurrency)
	}
	if opts.PartSize < MinPartSize {
		return nil, fmt.Errorf("partSize (%d) cannot be less than minimum (%d)", opts.PartSize, MinPartSize)
	}

	var limiter *rate.Limiter
	if opts.MaxBytesInSecond < 0 {
		return nil, fmt.Errorf("mxBytesInSecond (%d) must be a non-negative integer", opts.MaxBytesInSecond)
	} else if opts.MaxBytesInSecond == 0 {
		limiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		limiter = rate.NewLimiter(rate.Limit(opts.MaxBytesInSecond), int(opts.PartSize))
	}

	buf := &bytes.Buffer{}
	buf.Grow(int(opts.PartSize))

	// if the input doesn't have a checksum algorithm specified, we'll give one.
	// TODO if the input also has specific checksum values precalculated then use them.
	clonedInput := *input
	hasher := hashs3.NewFromPutObject(&clonedInput)

	return &writer{
		ctx:                 ctx,
		client:              client,
		input:               clonedInput,
		concurrency:         opts.Concurrency,
		partSize:            opts.PartSize,
		disableAbortOnError: opts.DisableAbortOnError,
		ex:                  executor.NewCallerRunsOnFullExecutor(opts.Concurrency - 1),
		limiter:             limiter,
		buf:                 buf,
		hasher:              hasher,
	}, nil
}

// writer implements Writer.
type writer struct {
	// from options.
	ctx                 context.Context
	client              WriterClient
	input               s3.PutObjectInput
	concurrency         int
	partSize            int64
	disableAbortOnError bool

	// internal state.
	ex            executor.ExecuteCloser
	limiter       *rate.Limiter
	buf           *bytes.Buffer
	uploadId      *string
	partNumber    int32
	err           error
	parts         sync.Map
	hasher        hashs3.HashS3
	mupObjectSize int64
}

func (w *writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	n, err := w.write(bytes.NewReader(p), false)
	return int(n), w.setErr(err)
}

func (w *writer) ReadFrom(src io.Reader) (written int64, err error) {
	if w.err != nil {
		return 0, w.err
	}

	written, err = w.write(src, false)
	return written, w.setErr(err)
}

type eofReader struct {
}

func (eofReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (w *writer) Close() (err error) {
	if _, err = w.write(eofReader{}, true); err != nil {
		return w.setErr(err)
	}

	// error from closing the executor is ignored so that we don't end up accidentally aborting the upload.
	_ = w.ex.Close()
	w.err = ErrClosed
	return nil
}

func (w *writer) write(src io.Reader, flush bool) (written int64, err error) {
	ctx, cancel := context.WithCancelCause(w.ctx)
	var wg sync.WaitGroup

	for {
		c := 0
		for limit := w.partSize - int64(w.buf.Len()); limit > 0; {
			c++

			n, err := w.buf.ReadFrom(io.LimitReader(src, limit))
			written += n
			limit -= n

			if err != nil {
				if errors.Is(err, io.EOF) {
					if c > 1 {
						//log.Printf("took %d reads to get EOF", c)
					}
					break
				}

				return written, err
			}
			if n == 0 {
				if c > 1 {
					//log.Printf("took %d reads to get n==0", c)
				}
				break
			}
		}

		if n := int64(w.buf.Len()); n < w.partSize {
			break
		} else {
			w.mupObjectSize += n
		}

		if w.uploadId == nil {
			if err = w.createMultipartUpload(ctx); err != nil {
				return written, fmt.Errorf("create multipart upload error: %w", err)
			}
		}

		// swap the buffers and compute the checksum.
		bb := w.buf
		w.buf = &bytes.Buffer{}
		w.buf.Grow(int(w.partSize))
		if _, err = w.hasher.Write(bb.Bytes()); err != nil {
			return written, fmt.Errorf("compute checksum error: %w", err)
		}

		w.partNumber++
		partNumber := w.partNumber

		wg.Add(1)

		if err = w.ex.Execute(func() {
			defer wg.Done()

			if err = w.uploadPart(ctx, partNumber, bb); err != nil {
				cancel(err)
			}
		}); err != nil {
			cancel(err)
			return written, err
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
		return written, ctx.Err()
	case <-done:
		if flush {
			if w.uploadId == nil {
				return int64(w.buf.Len()), w.putObject()
			}

			if n := int64(w.buf.Len()); n > 0 {
				if _, err = w.hasher.Write(w.buf.Bytes()); err != nil {
					return written, fmt.Errorf("compute checksum error: %w", err)
				}

				w.mupObjectSize += n
				w.partNumber++

				if err = w.uploadPart(w.ctx, w.partNumber, w.buf); err != nil {
					return written, err
				}
			}

			return written, w.completeMultipartUpload()
		}

		return written, nil
	}
}

func (w *writer) createMultipartUpload(ctx context.Context) error {
	input := &s3.CreateMultipartUploadInput{
		Bucket:                    w.input.Bucket,
		Key:                       w.input.Key,
		ACL:                       w.input.ACL,
		BucketKeyEnabled:          w.input.BucketKeyEnabled,
		CacheControl:              w.input.CacheControl,
		ChecksumAlgorithm:         w.input.ChecksumAlgorithm,
		ContentDisposition:        w.input.ContentDisposition,
		ContentEncoding:           w.input.ContentEncoding,
		ContentLanguage:           w.input.ContentLanguage,
		ContentType:               w.input.ContentType,
		ExpectedBucketOwner:       w.input.ExpectedBucketOwner,
		Expires:                   w.input.Expires,
		GrantFullControl:          w.input.GrantFullControl,
		GrantRead:                 w.input.GrantRead,
		GrantReadACP:              w.input.GrantReadACP,
		GrantWriteACP:             w.input.GrantWriteACP,
		Metadata:                  w.input.Metadata,
		ObjectLockLegalHoldStatus: w.input.ObjectLockLegalHoldStatus,
		ObjectLockMode:            w.input.ObjectLockMode,
		ObjectLockRetainUntilDate: w.input.ObjectLockRetainUntilDate,
		RequestPayer:              w.input.RequestPayer,
		SSECustomerAlgorithm:      w.input.SSECustomerAlgorithm,
		SSECustomerKey:            w.input.SSECustomerKey,
		SSECustomerKeyMD5:         w.input.SSECustomerKeyMD5,
		SSEKMSEncryptionContext:   w.input.SSEKMSEncryptionContext,
		SSEKMSKeyId:               w.input.SSEKMSKeyId,
		ServerSideEncryption:      w.input.ServerSideEncryption,
		StorageClass:              w.input.StorageClass,
		Tagging:                   w.input.Tagging,
		WebsiteRedirectLocation:   w.input.WebsiteRedirectLocation,
	}

	w.hasher = hashs3.NewFromCreateMultipartUpload(input)

	createMultipartUploadOutput, err := w.client.CreateMultipartUpload(ctx, input)
	if err == nil {
		w.uploadId = createMultipartUploadOutput.UploadId
	}

	return err
}

func (w *writer) putObject() (err error) {
	w.input.Body = w.buf

	hasher := hashs3.NewFromPutObject(&w.input)
	_, err = hasher.Write(w.buf.Bytes())
	if err == nil {
		_, err = w.client.PutObject(w.ctx, hasher.SumPutObject(&w.input))
	}

	return err
}

func (w *writer) uploadPart(ctx context.Context, partNumber int32, buf *bytes.Buffer) error {
	uploadPartOutput, err := w.client.UploadPart(ctx, w.hasher.HashUploadPart(buf.Bytes(), &s3.UploadPartInput{
		Bucket:               w.input.Bucket,
		Key:                  w.input.Key,
		PartNumber:           aws.Int32(partNumber),
		UploadId:             w.uploadId,
		Body:                 buf,
		ChecksumAlgorithm:    w.input.ChecksumAlgorithm,
		ExpectedBucketOwner:  w.input.ExpectedBucketOwner,
		RequestPayer:         w.input.RequestPayer,
		SSECustomerAlgorithm: w.input.SSECustomerAlgorithm,
		SSECustomerKey:       w.input.SSECustomerKey,
		SSECustomerKeyMD5:    w.input.SSECustomerKeyMD5,
	}))
	if err != nil {
		return err
	}

	w.parts.Store(partNumber, types.CompletedPart{
		ChecksumCRC32:     uploadPartOutput.ChecksumCRC32,
		ChecksumCRC32C:    uploadPartOutput.ChecksumCRC32C,
		ChecksumCRC64NVME: uploadPartOutput.ChecksumCRC64NVME,
		ChecksumSHA1:      uploadPartOutput.ChecksumSHA1,
		ChecksumSHA256:    uploadPartOutput.ChecksumSHA256,
		ETag:              uploadPartOutput.ETag,
		PartNumber:        aws.Int32(partNumber),
	})

	return nil
}

func (w *writer) completeMultipartUpload() (err error) {
	// collect and sort the completed parts because the sorting operation is too complex for S3 to do.
	parts := make([]types.CompletedPart, 0)
	w.parts.Range(func(_, value any) bool {
		parts = append(parts, value.(types.CompletedPart))
		return true
	})
	slices.SortFunc(parts, func(a, b types.CompletedPart) int {
		return int(*a.PartNumber - *b.PartNumber)
	})

	_, err = w.client.CompleteMultipartUpload(w.ctx, w.hasher.SumCompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:               w.input.Bucket,
		Key:                  w.input.Key,
		UploadId:             w.uploadId,
		ExpectedBucketOwner:  w.input.ExpectedBucketOwner,
		IfMatch:              w.input.IfMatch,
		IfNoneMatch:          w.input.IfNoneMatch,
		MpuObjectSize:        &w.mupObjectSize,
		MultipartUpload:      &types.CompletedMultipartUpload{Parts: parts},
		RequestPayer:         w.input.RequestPayer,
		SSECustomerAlgorithm: w.input.SSECustomerAlgorithm,
		SSECustomerKey:       w.input.SSECustomerKey,
		SSECustomerKeyMD5:    w.input.SSECustomerKeyMD5,
	}))

	return
}

func (w *writer) setErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrClosed):
		w.err = err
		return nil
	case errors.As(err, &MultipartUploadError{}), w.uploadId == nil:
		w.err = err
		return err
	}

	muErr := MultipartUploadError{
		Err:      err,
		UploadID: aws.ToString(w.uploadId),
		Abort:    AbortNotAttempted,
		AbortErr: nil,
	}

	if !w.disableAbortOnError {
		if _, muErr.AbortErr = w.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:              w.input.Bucket,
			Key:                 w.input.Key,
			UploadId:            w.uploadId,
			ExpectedBucketOwner: w.input.ExpectedBucketOwner,
			RequestPayer:        w.input.RequestPayer,
		}); muErr.AbortErr == nil {
			muErr.Abort = AbortSuccess
		} else {
			muErr.Abort = AbortFailure
		}
	}

	w.err = muErr
	return muErr
}
