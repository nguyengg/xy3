package xy3

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"
)

// UploadOptions customises various aspects of an Upload operation.
type UploadOptions struct {
	// PartSize is the size of each part.
	//
	// Defaults to MinPartSize which is also the minimum. Cannot exceed MaxPartSize.
	PartSize int64

	// Concurrency is the number of goroutines responsible for uploading the parts in parallel.
	//
	// Defaults to DefaultConcurrency. Must be a positive integer.
	Concurrency int

	// MaxBytesInSecond is used to rate limit the amount of bytes that are uploaded in one second.
	//
	// The zero-value indicates no limit. Must be positive integer.
	MaxBytesInSecond int64

	// DisableAbortOnFailure controls whether upload failure will result in an attempt to call
	// [s3.Client.AbortMultipartUpload].
	//
	// By default, an abort attempt will be made.
	DisableAbortOnFailure bool

	// PreUploadPart is called before a [s3.ClientUploadPart] attempt.
	//
	// The data slice should not be modified nor retained lest it impacts the actual data uploaded to S3. This hook
	// will only be called from the main goroutine that calls Upload. It can be used to hash the file as there is a
	// guaranteed ordering (ascending part number starting at 1, ending at partCount inclusive) to these calls.
	PreUploadPart func(partNumber int32, data []byte)

	// PostUploadPart is called after every successful [s3.Client.UploadPart].
	//
	// By default, `log.Printf` will be used to print messages in format `uploaded %d/%d parts`. This hook will only
	// be called from the main goroutine that calls Upload. Unlike PreUploadPart, there is no guarantee to the
	// ordering of the parts being completed.
	PostUploadPart func(part types.CompletedPart, partCount int32)
}

type uploader struct {
	UploadOptions

	client                      UploadAPIClient
	bufPool                     *sync.Pool
	limiter                     *rate.Limiter
	createMultipartUploadInput  *s3.CreateMultipartUploadInput
	createMultipartUploadOutput *s3.CreateMultipartUploadOutput
	lev                         hasher
}

type uploadInput struct {
	input *s3.UploadPartInput
	buf   *bytes.Buffer
}

type uploadOutput struct {
	part types.CompletedPart
	err  error
}

func newMultipartUploader(client UploadAPIClient, optFns ...func(options *UploadOptions)) (*uploader, error) {
	var partUploadCount int32

	opts := UploadOptions{
		PartSize:         MinPartSize,
		Concurrency:      DefaultConcurrency,
		MaxBytesInSecond: 0,
		PostUploadPart: func(part types.CompletedPart, partCount int32) {
			partUploadCount++
			log.Printf("uploaded %d/%d parts", partUploadCount, partCount)
		},
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	u := &uploader{
		UploadOptions: opts,
		client:        client,
		bufPool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}

	if u.PartSize < MinPartSize {
		return nil, fmt.Errorf("partSize (%d) cannot be less than %d", u.PartSize, MinPartSize)
	}
	if u.PartSize > MaxPartSize {
		return nil, fmt.Errorf("partSize (%d) cannot be greater than %d", u.PartSize, MaxPartSize)
	}
	if u.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency (%d) must be positive", opts.Concurrency)
	}
	if u.MaxBytesInSecond < 0 {
		return nil, fmt.Errorf("maxBytesInSecond (%d) cannot be negative", u.MaxBytesInSecond)
	} else if u.MaxBytesInSecond == 0 {
		u.limiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		u.limiter = rate.NewLimiter(rate.Limit(u.MaxBytesInSecond), int(u.PartSize))
	}

	return u, nil
}

func (u *uploader) upload(ctx context.Context, name string, input *s3.CreateMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	// because input is a name of file, we know the file's size which can be used to compute the number of parts.
	f, size, partSize, partCount, err := u.validate(name)
	if err != nil {
		return nil, err
	}
	cf := sync.OnceFunc(func() {
		_ = f.Close()
	})
	defer cf()

	u.lev = hashFrom(input)
	u.createMultipartUploadInput = input
	if u.createMultipartUploadOutput, err = u.client.CreateMultipartUpload(ctx, input); err != nil {
		return nil, err
	}

	// wrapErr is responsible for attempting to call abort.
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}

		mErr := MultipartUploadError{
			Err:      err,
			UploadID: aws.ToString(u.createMultipartUploadOutput.UploadId),
			Abort:    AbortNotAttempted,
			AbortErr: nil,
		}
		if !u.DisableAbortOnFailure {
			if _, mErr.AbortErr = u.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:              input.Bucket,
				Key:                 input.Key,
				UploadId:            u.createMultipartUploadOutput.UploadId,
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

	// start the workers here.
	inputs := make(chan uploadInput, u.Concurrency)
	outputs := make(chan uploadOutput, u.Concurrency)
	for range u.Concurrency {
		go u.poll(ctx, inputs, outputs)
	}

	// main goroutine is responsible for:
	//	1. divvy up the file into parts.
	//	2. send each part to the inputs channel.
	//	3. simultaneously and afterward read from outputs channel to report progress.
	parts := make([]types.CompletedPart, 0, partCount)
	r := &io.LimitedReader{R: f}
	for partNumber, n, remain := int32(1), int64(0), size; partNumber <= partCount; partNumber++ {
		buf := u.bufPool.Get().(*bytes.Buffer)
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

		uploadPartInput := &s3.UploadPartInput{
			Bucket:               u.createMultipartUploadInput.Bucket,
			Key:                  u.createMultipartUploadInput.Key,
			PartNumber:           aws.Int32(partNumber),
			UploadId:             u.createMultipartUploadOutput.UploadId,
			ChecksumAlgorithm:    u.createMultipartUploadInput.ChecksumAlgorithm,
			ExpectedBucketOwner:  u.createMultipartUploadInput.ExpectedBucketOwner,
			RequestPayer:         u.createMultipartUploadInput.RequestPayer,
			SSECustomerAlgorithm: u.createMultipartUploadInput.SSECustomerAlgorithm,
			SSECustomerKey:       u.createMultipartUploadInput.SSECustomerKey,
			SSECustomerKeyMD5:    u.createMultipartUploadInput.SSECustomerKeyMD5,
		}
		data := buf.Bytes()
		u.lev.uploadPart(data, uploadPartInput)
		if u.PreUploadPart != nil {
			u.PreUploadPart(partNumber, data)
		}

	sendInputLoop:
		for {
			select {
			case inputs <- uploadInput{input: uploadPartInput, buf: buf}:
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

	// need to sort the parts because the sorting operation is too complex for S3 to poll.
	slices.SortFunc(parts, func(a, b types.CompletedPart) int {
		return int(*a.PartNumber - *b.PartNumber)
	})

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:               input.Bucket,
		Key:                  input.Key,
		UploadId:             u.createMultipartUploadOutput.UploadId,
		ExpectedBucketOwner:  input.ExpectedBucketOwner,
		MultipartUpload:      &types.CompletedMultipartUpload{Parts: parts},
		RequestPayer:         input.RequestPayer,
		ChecksumType:         input.ChecksumType,
		MpuObjectSize:        aws.Int64(size),
		SSECustomerAlgorithm: input.SSECustomerAlgorithm,
		SSECustomerKey:       input.SSECustomerKey,
		SSECustomerKeyMD5:    input.SSECustomerKeyMD5,
	}
	u.lev.complete(completeMultipartUploadInput)

	output, err := u.client.CompleteMultipartUpload(ctx, completeMultipartUploadInput)
	return output, wrapErr(err)
}

// validate checks that file is valid for upload, and return an opened file on success.
func (u *uploader) validate(name string) (f *os.File, size, partSize int64, partCount int32, err error) {
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

func (u *uploader) poll(ctx context.Context, inputs <-chan uploadInput, outputs chan<- uploadOutput) {
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

		// for retry to work, UploadPartInput.Body needs to implement io.Seeker.
		// we wrap bytes.Buffer in bytes.NewReader to get that for free.
		// here is also where rate limiting happens.
		data := part.buf.Bytes()
		part.input.Body = bytes.NewReader(data)
		if err := u.limiter.WaitN(ctx, len(data)); err != nil {
			outputs <- uploadOutput{
				err: fmt.Errorf("wait to upload part %d error: %w", aws.ToInt32(part.input.PartNumber), err),
			}
			return
		}

		output, err := u.client.UploadPart(ctx, part.input)
		u.bufPool.Put(part.buf)

		if err != nil {
			outputs <- uploadOutput{
				err: fmt.Errorf("upload part %d error: %w", aws.ToInt32(part.input.PartNumber), err),
			}
			return
		}

		select {
		case outputs <- uploadOutput{
			part: types.CompletedPart{
				ChecksumCRC32:     output.ChecksumCRC32,
				ChecksumCRC32C:    output.ChecksumCRC32C,
				ChecksumCRC64NVME: output.ChecksumCRC64NVME,
				ChecksumSHA1:      output.ChecksumSHA1,
				ChecksumSHA256:    output.ChecksumSHA256,
				ETag:              output.ETag,
				PartNumber:        part.input.PartNumber,
			},
		}:
		case <-ctx.Done():
			return
		}
	}
}

type hasher struct {
	uploadPart func([]byte, *s3.UploadPartInput)
	complete   func(*s3.CompleteMultipartUploadInput)
}

func hashFrom(input *s3.CreateMultipartUploadInput) hasher {
	switch checksumType := input.ChecksumType; input.ChecksumAlgorithm {
	case types.ChecksumAlgorithmCrc32:
		fo := crc32.NewIEEE()
		co := crc32.NewIEEE()
		return hasher{
			func(data []byte, input *s3.UploadPartInput) {
				_, _ = fo.Write(data)
				co.Reset()
				_, _ = co.Write(data)
				input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(co.Sum(nil)))
			},
			func(input *s3.CompleteMultipartUploadInput) {
				if checksumType == types.ChecksumTypeFullObject {
					input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(fo.Sum(nil)))
				}
			},
		}
	case types.ChecksumAlgorithmCrc32c:
		fo := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		co := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		return hasher{
			func(data []byte, input *s3.UploadPartInput) {
				_, _ = fo.Write(data)
				co.Reset()
				_, _ = co.Write(data)
				input.ChecksumCRC32C = aws.String(base64.StdEncoding.EncodeToString(co.Sum(nil)))
			},
			func(input *s3.CompleteMultipartUploadInput) {
				if checksumType == types.ChecksumTypeFullObject {
					input.ChecksumCRC32C = aws.String(base64.StdEncoding.EncodeToString(fo.Sum(nil)))
				}
			},
		}
	case types.ChecksumAlgorithmCrc64nvme:
		fo := crc64.New(crc64.MakeTable(0xAD93D23594C93659))
		return hasher{
			func(data []byte, input *s3.UploadPartInput) {
				// crc64-nvme does not support full object.
				// however, there is a bug (?) right now that causes
				// need to empty this due to a bug that causes: failed to parse algorithm, unknown checksum algorithm, CRC64NVME
				//input.ChecksumCRC64NVME = aws.String(base64.StdEncoding.EncodeToString(co.Sum(nil)))
				input.ChecksumAlgorithm = ""
			},
			func(input *s3.CompleteMultipartUploadInput) {
				input.ChecksumCRC64NVME = aws.String(base64.StdEncoding.EncodeToString(fo.Sum(nil)))
			},
		}
	case types.ChecksumAlgorithmSha1:
		co := sha1.New()
		return hasher{
			func(data []byte, input *s3.UploadPartInput) {
				co.Reset()
				co.Write(data)
				input.ChecksumSHA1 = aws.String(base64.StdEncoding.EncodeToString(co.Sum(nil)))
			},
			func(input *s3.CompleteMultipartUploadInput) {
				// sha1 does not support full object.
			},
		}
	case types.ChecksumAlgorithmSha256:
		co := sha256.New()
		return hasher{
			func(data []byte, input *s3.UploadPartInput) {
				co.Reset()
				co.Write(data)
				input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(co.Sum(nil)))
			},
			func(input *s3.CompleteMultipartUploadInput) {
				// sha256 does not support full object.
			},
		}
	default:
		// right now if nothing is specified default to crc32 full-object.
		// otherwise, it will fail, see https://github.com/nguyengg/xy3/issues/1.
		input.ChecksumAlgorithm = types.ChecksumAlgorithmCrc32
		input.ChecksumType = types.ChecksumTypeFullObject

		fo := crc32.NewIEEE()
		co := crc32.NewIEEE()
		return hasher{
			uploadPart: func(data []byte, input *s3.UploadPartInput) {
				_, _ = fo.Write(data)
				co.Reset()
				_, _ = co.Write(data)
				input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(co.Sum(nil)))
			},
			complete: func(input *s3.CompleteMultipartUploadInput) {
				input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(fo.Sum(nil)))
			},
		}
	}
}
