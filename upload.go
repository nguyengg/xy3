package xy3

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"sync"
)

// Amazon S3 multipart upload limits
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const (
	MaxFileSize        = int64(5_497_558_138_880)
	MaxPartCount       = 10_000
	MinPartSize        = int64(5_242_880)
	MaxPartSize        = int64(5_368_709_120)
	DefaultConcurrency = 3
)

// MultipartUploader is used to upload files to S3 using multipart upload with progress report.
type MultipartUploader struct {
	// PartSize is the size of each part.
	//
	// Defaults to MinPartSize which is also the minimum. Cannot exceed MaxPartSize.
	PartSize int64

	// Concurrency is the number of goroutines responsible for uploading the parts in parallel.
	//
	// Defaults to DefaultConcurrency. Cannot be non-positive.
	Concurrency int

	// MaxBytesInSecond is used to rate limit the amount of bytes that are uploaded in one second.
	//
	// The zero-value indicates no limit. Negative values are ignored.
	MaxBytesInSecond int

	// DisableAbortOnFailure controls whether upload failure will result in an attempt to call
	// [s3.Client.AbortMultipartUpload].
	//
	// By default, an abort attempt will be made.
	DisableAbortOnFailure bool

	// PreUploadPart is called before a [s3.ClientUploadPart] attempt.
	//
	// The data slice should not be modified nor retained lest it impacts the actual data uploaded to S3. This hook will
	// only be called from the main goroutine that calls Upload. It can be used to hash the file as there is a
	// guaranteed ordering (ascending part number starting at 1, ending at partCount inclusive) to these calls.
	PreUploadPart func(partNumber int32, data []byte)

	// PostUploadPart is called after every successful [s3.Client.UploadPart].
	//
	// By default, `log.Printf` will be used to print messages in format `uploaded %d/%d parts`. This hook will only be
	// called from the main goroutine that calls Upload. Unlike PreUploadPart, there is no guarantee to the ordering of
	// the parts being completed.
	PostUploadPart func(part s3types.CompletedPart, partCount int32)

	client UploadAPIClient
}

func newMultipartUploader(client UploadAPIClient, optFns ...func(*MultipartUploader)) (*MultipartUploader, error) {
	var partUploadCount int32

	u := &MultipartUploader{
		PartSize:    MinPartSize,
		Concurrency: DefaultConcurrency,
		PostUploadPart: func(part s3types.CompletedPart, partCount int32) {
			partUploadCount++
			log.Printf("uploaded %d/%d parts", partUploadCount, partCount)
		},
		client: client,
	}
	for _, fn := range optFns {
		fn(u)
	}

	if u.PartSize < MinPartSize {
		return nil, fmt.Errorf("partSize (%d) cannot be less than %d", u.PartSize, MinPartSize)
	}
	if u.PartSize > MaxPartSize {
		return nil, fmt.Errorf("partSize (%d) cannot be greater than %d", u.PartSize, MaxPartSize)
	}
	if u.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency (%d) must be greater than 0", u.Concurrency)
	}

	return u, nil
}

type uploadInput struct {
	partNumber int32
	buf        *bytes.Buffer
}

type uploadOutput struct {
	part s3types.CompletedPart
	err  error
}

func (u MultipartUploader) upload(ctx context.Context, name string, input *s3.CreateMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	// because input is a name of file, we know the file's size which can be used to compute the number of parts.
	f, size, partSize, partCount, err := u.validate(name)
	if err != nil {
		return nil, err
	}
	cf := sync.OnceFunc(func() {
		_ = f.Close()
	})
	defer cf()

	var uploadId string
	if output, err := u.client.CreateMultipartUpload(ctx, input); err != nil {
		return nil, err
	} else {
		uploadId = aws.ToString(output.UploadId)
	}

	// wrapErr is responsible for attempting to call abort.
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}

		mErr := MultipartUploadError{
			Err:      err,
			UploadID: uploadId,
			Abort:    AbortNotAttempted,
			AbortErr: nil,
		}
		if !u.DisableAbortOnFailure {
			if _, mErr.AbortErr = u.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:              input.Bucket,
				Key:                 input.Key,
				UploadId:            &uploadId,
				ExpectedBucketOwner: input.ExpectedBucketOwner,
				RequestPayer:        input.RequestPayer,
			}); mErr.AbortErr == nil {
				mErr.Abort = AbortSuccess
			} else {
				mErr.Abort = AbortFailure
			}
		}

		return mErr
	}

	// start the workers here. they will share a pool of buffers.
	bufPool := &sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
	inputs := make(chan uploadInput, u.Concurrency)
	outputs := make(chan uploadOutput, u.Concurrency)
	var limiter *rate.Limiter
	if u.MaxBytesInSecond <= 0 {
		limiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		limiter = rate.NewLimiter(rate.Limit(u.MaxBytesInSecond), u.MaxBytesInSecond)
	}
	for range u.Concurrency {
		go u.newWorker(input, uploadId, partCount, bufPool, limiter).do(ctx, inputs, outputs)
	}

	// main goroutine is responsible for:
	//	1. divvy up the file into parts.
	//	2. send each part to the inputs channel.
	//	3. simultaneously and afterward read from outputs channel to report progress.
	parts := make([]s3types.CompletedPart, 0, partCount)
	r := &io.LimitedReader{R: f}
	for partNumber, n, remain := int32(1), int64(0), size; partNumber <= partCount; partNumber++ {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		r.N = partSize
		n, err = buf.ReadFrom(r)
		if err != nil {
			close(inputs)
			return nil, wrapErr(err)
		}

		if remain -= n; n != partSize {
			// the last part might be truncated but never 0.
			if remain != 0 {
				close(inputs)
				return nil, wrapErr(fmt.Errorf("read only %d/%d bytes", n, partSize))
			}
		}

		if u.PreUploadPart != nil {
			u.PreUploadPart(partNumber, buf.Bytes())
		}

	sendInputLoop:
		for {
			select {
			case inputs <- uploadInput{
				partNumber: partNumber,
				buf:        buf,
			}:
				break sendInputLoop
			case result := <-outputs:
				if err = result.err; err != nil {
					return nil, wrapErr(err)
				}

				parts = append(parts, result.part)
				if u.PostUploadPart != nil {
					u.PostUploadPart(result.part, partCount)
				}
			case <-ctx.Done():
				close(inputs)
				return nil, wrapErr(ctx.Err())
			}
		}
	}

	// close inputs channel to allow workers to close, as well as close file as early as possible. don't worry about
	// closing outputs as gc can take care of reclaiming it.
	close(inputs)
	cf()

	// now wait for remaining parts to finish uploading.
	for n := int32(len(parts)); n < partCount; {
		select {
		case result := <-outputs:
			if err = result.err; err != nil {
				return nil, wrapErr(err)
			}

			parts = append(parts, result.part)
			n++
			if u.PostUploadPart != nil {
				u.PostUploadPart(result.part, partCount)
			}
		case <-ctx.Done():
			return nil, wrapErr(ctx.Err())
		}
	}

	// need to sort the parts because the sorting operation is too complex for S3 to do.
	slices.SortFunc(parts, func(a, b s3types.CompletedPart) int {
		return int(*a.PartNumber - *b.PartNumber)
	})

	output, err := u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:               input.Bucket,
		Key:                  input.Key,
		UploadId:             &uploadId,
		ExpectedBucketOwner:  input.ExpectedBucketOwner,
		MultipartUpload:      &s3types.CompletedMultipartUpload{Parts: parts},
		RequestPayer:         input.RequestPayer,
		SSECustomerAlgorithm: input.SSECustomerAlgorithm,
		SSECustomerKey:       input.SSECustomerKey,
		SSECustomerKeyMD5:    input.SSECustomerKeyMD5,
	})
	return output, wrapErr(err)
}

// validate checks that file is valid for upload, and return an opened file on success.
func (u MultipartUploader) validate(name string) (f *os.File, size, partSize int64, partCount int32, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return
	}
	if fi.IsDir() {
		err = fmt.Errorf("file is directory")
		return
	}

	size = fi.Size()
	if size > MaxFileSize {
		err = fmt.Errorf("file's size (%s) exceeds S3 limit (%s)", humanize.Bytes(uint64(size)), humanize.Bytes(uint64(MaxFileSize)))
		return
	}
	if size == 0 {
		err = fmt.Errorf("empty file")
		return
	}

	partSize = u.PartSize
	partCount = int32(math.Ceil(float64(size) / float64(partSize)))
	if partCount > MaxPartCount {
		err = fmt.Errorf("partSize too small as it results in part count (%d) exceeding limit (%d)", partCount, MaxPartCount)
		return
	}

	f, err = os.Open(name)
	return
}

type uploadWorker struct {
	client    UploadAPIClient
	input     *s3.UploadPartInput
	partCount int32
	bufPool   *sync.Pool
	limiter   *rate.Limiter
}

func (u MultipartUploader) newWorker(input *s3.CreateMultipartUploadInput, uploadId string, partCount int32, bufPool *sync.Pool, limiter *rate.Limiter) *uploadWorker {
	return &uploadWorker{
		client: u.client,
		input: &s3.UploadPartInput{
			Bucket:               input.Bucket,
			Key:                  input.Key,
			UploadId:             aws.String(uploadId),
			ChecksumAlgorithm:    input.ChecksumAlgorithm,
			ExpectedBucketOwner:  input.ExpectedBucketOwner,
			RequestPayer:         input.RequestPayer,
			SSECustomerAlgorithm: input.SSECustomerAlgorithm,
			SSECustomerKey:       input.SSECustomerKey,
			SSECustomerKeyMD5:    input.SSECustomerKeyMD5,
		},
		partCount: partCount,
		bufPool:   bufPool,
		limiter:   limiter,
	}
}

func (w *uploadWorker) do(ctx context.Context, inputs <-chan uploadInput, outputs chan<- uploadOutput) {
	var (
		part uploadInput
		ok   bool
	)

	for {
		select {
		case part, ok = <-inputs:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		w.input.PartNumber = aws.Int32(part.partNumber)

		// for retry to work, Body needs to implement io.Seeker so we wrap the bytes.Buffer here.
		// here is also where rate limiting happens.
		data := part.buf.Bytes()
		if err := w.limiter.WaitN(ctx, len(data)); err != nil {
			outputs <- uploadOutput{
				err: fmt.Errorf("wait to upload part %d error: %w", part.partNumber, err),
			}
			return
		}
		w.input.Body = bytes.NewReader(data)

		output, err := w.client.UploadPart(ctx, w.input)
		w.bufPool.Put(part.buf)
		if err != nil {
			outputs <- uploadOutput{
				err: fmt.Errorf("upload part %d error: %w", part.partNumber, err),
			}
			return
		}

		select {
		case outputs <- uploadOutput{
			part: s3types.CompletedPart{
				ChecksumCRC32:  output.ChecksumCRC32,
				ChecksumCRC32C: output.ChecksumCRC32C,
				ChecksumSHA1:   output.ChecksumSHA1,
				ChecksumSHA256: output.ChecksumSHA256,
				ETag:           output.ETag,
				PartNumber:     aws.Int32(part.partNumber),
			},
		}:
		case <-ctx.Done():
			return
		}
	}
}
