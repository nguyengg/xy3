package xy3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3/internal/hashs3"
	"github.com/valyala/bytebufferpool"
	_ "github.com/valyala/bytebufferpool"
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
	// will only be called from the main goroutine that calls Upload or UploadStream. It can be used to hash the
	// file as there is a guaranteed ordering (ascending part number starting at 1, ending at partCount inclusive)
	// to these calls.
	PreUploadPart func(partNumber int32, data []byte)

	// PostUploadPart is called after every successful [s3.Client.UploadPart].
	//
	// This hook will only be called from the main goroutine that calls Upload or UploadStream. Unlike
	// PreUploadPart, there is no guarantee to the ordering of the parts being completed.
	PostUploadPart func(part types.CompletedPart, partCount int32)
}

type uploader struct {
	UploadOptions

	client                      UploadAPIClient
	limiter                     *rate.Limiter
	lev                         hashs3.HashS3
	createMultipartUploadInput  *s3.CreateMultipartUploadInput
	createMultipartUploadOutput *s3.CreateMultipartUploadOutput
}

type uploadInput struct {
	partNumber int32
	bb         *bytebufferpool.ByteBuffer
}

type uploadOutput struct {
	part types.CompletedPart
	err  error
}

type uploadReporter struct {
	size, expectedSize uint64
	expectedPartCount  int32
	parts              map[int32]int
}

func (r *uploadReporter) preUploadPart(partNumber int32, data []byte) {
	r.parts[partNumber] = len(data)
}

func (r *uploadReporter) postUploadPart(part types.CompletedPart, partCount int32) {
	partNumber := aws.ToInt32(part.PartNumber)
	r.size += uint64(r.parts[partNumber])

	if r.expectedPartCount != 0 {
		log.Printf("uploaded %d/%d parts (%s/%s)", partCount, r.expectedPartCount, humanize.IBytes(r.size), humanize.IBytes(r.expectedSize))
	} else {
		log.Printf("uploaded %d parts (%s)", partCount, humanize.IBytes(r.size))
	}

	delete(r.parts, partNumber)
}

func newMultipartUploader(client UploadAPIClient, optFns ...func(options *UploadOptions)) (*uploader, error) {
	opts := UploadOptions{
		PartSize:         MinPartSize,
		Concurrency:      DefaultConcurrency,
		MaxBytesInSecond: 0,
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	u := &uploader{
		UploadOptions: opts,
		client:        client,
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
	fi, err := os.Stat(name)
	if err != nil {
		return nil, fmt.Errorf("stat file error: %w", err)
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("file is directory")
	}

	size := fi.Size()
	if size > MaxFileSize {
		return nil, fmt.Errorf("file's size (%s) exceeds S3 limit (%s)", humanize.Bytes(uint64(size)), humanize.Bytes(uint64(MaxFileSize)))
	}
	if size == 0 {
		return nil, fmt.Errorf("empty file")
	}

	expectedPartCount := int32(math.Ceil(float64(size) / float64(u.PartSize)))
	if expectedPartCount > MaxPartCount {
		return nil, fmt.Errorf("partSize too small as it results in part count (%d) exceeding limit (%d)", expectedPartCount, MaxPartCount)
	} else if u.PreUploadPart == nil && u.PostUploadPart == nil {
		rep := uploadReporter{
			expectedSize:      uint64(size),
			expectedPartCount: int32(math.Ceil(float64(size) / float64(u.PartSize))),
		}
		u.PreUploadPart = rep.preUploadPart
		u.PostUploadPart = rep.postUploadPart
	}

	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open file for read error: %w", err)
	}
	defer f.Close()

	return u.uploadStream(ctx, f, input, expectedPartCount)
}

func (u *uploader) uploadStream(ctx context.Context, body io.Reader, input *s3.CreateMultipartUploadInput, expectedPartCount int32) (_ *s3.CompleteMultipartUploadOutput, err error) {
	// while streaming from body, we'll also compute its hash to write it to the manifest file later.
	u.lev, input.ChecksumAlgorithm, input.ChecksumType = hashs3.NewOrDefault(input.ChecksumAlgorithm, input.ChecksumType)
	u.createMultipartUploadInput = input
	u.createMultipartUploadOutput, err = u.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, err
	}
	body = io.TeeReader(body, u.lev)

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
	var (
		size       int64
		partNumber int32
		parts      = make([]types.CompletedPart, 0, expectedPartCount)
	)

	data := make([]byte, u.PartSize)

ingBad:
	for n := 0; err == nil; {
		switch n, err = body.Read(data); err {
		case nil:
		case io.EOF:
			// use of io.TeeReader will cause an additional read that will return 0, io.EOF.
			// if we were reading directly from io.Reader, this may not happen.
			if n == 0 {
				break ingBad
			}
		default:
			close(inputs)
			return nil, wrapErr(err)
		}

		partNumber++
		size += int64(n)
		part := data[:n]

		bb := bytebufferpool.Get()
		_, _ = bb.Write(part)

		// call PreUploadPart here to provide ordering guarantee.
		if u.PreUploadPart != nil {
			u.PreUploadPart(partNumber, part)
		}

	sendInputLoop:
		for {
			select {
			case inputs <- uploadInput{partNumber, bb}:
				break sendInputLoop
			case result := <-outputs:
				if result.err != nil {
					close(inputs)
					return nil, wrapErr(result.err)
				}

				parts = append(parts, result.part)

				if u.PostUploadPart != nil {
					u.PostUploadPart(result.part, expectedPartCount)
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

	// now wait for remaining parts to finish uploading.
	for n := int32(len(parts)); n < partNumber; n++ {
		select {
		case result := <-outputs:
			if result.err != nil {
				return nil, wrapErr(result.err)
			}

			parts = append(parts, result.part)

			if u.PostUploadPart != nil {
				u.PostUploadPart(result.part, partNumber)
			}
		case <-ctx.Done():
			return nil, wrapErr(ctx.Err())
		}
	}

	// need to sort the parts because the sorting operation is too complex for S3 to do.
	slices.SortFunc(parts, func(a, b types.CompletedPart) int {
		return int(*a.PartNumber - *b.PartNumber)
	})

	completeMultipartUploadOutput, err := u.client.CompleteMultipartUpload(
		ctx,
		// this will compute the hash and modify the CompleteMultipartUploadInput with the relevant checksums.
		u.lev.SumCompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:               u.createMultipartUploadInput.Bucket,
			Key:                  u.createMultipartUploadInput.Key,
			UploadId:             u.createMultipartUploadOutput.UploadId,
			ExpectedBucketOwner:  u.createMultipartUploadInput.ExpectedBucketOwner,
			MultipartUpload:      &types.CompletedMultipartUpload{Parts: parts},
			RequestPayer:         u.createMultipartUploadInput.RequestPayer,
			ChecksumType:         u.createMultipartUploadInput.ChecksumType,
			MpuObjectSize:        &size,
			SSECustomerAlgorithm: u.createMultipartUploadInput.SSECustomerAlgorithm,
			SSECustomerKey:       u.createMultipartUploadInput.SSECustomerKey,
			SSECustomerKeyMD5:    u.createMultipartUploadInput.SSECustomerKeyMD5,
		}))
	return completeMultipartUploadOutput, wrapErr(err)
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
		partNumber := part.partNumber
		data := part.bb.B
		uploadPartInput := &s3.UploadPartInput{
			Bucket:               u.createMultipartUploadInput.Bucket,
			Key:                  u.createMultipartUploadInput.Key,
			Body:                 bytes.NewReader(data),
			PartNumber:           &partNumber,
			UploadId:             u.createMultipartUploadOutput.UploadId,
			ChecksumAlgorithm:    u.createMultipartUploadInput.ChecksumAlgorithm,
			ExpectedBucketOwner:  u.createMultipartUploadInput.ExpectedBucketOwner,
			RequestPayer:         u.createMultipartUploadInput.RequestPayer,
			SSECustomerAlgorithm: u.createMultipartUploadInput.SSECustomerAlgorithm,
			SSECustomerKey:       u.createMultipartUploadInput.SSECustomerKey,
			SSECustomerKeyMD5:    u.createMultipartUploadInput.SSECustomerKeyMD5,
		}
		if err := u.limiter.WaitN(ctx, len(data)); err != nil {
			outputs <- uploadOutput{
				err: fmt.Errorf("wait to upload part %d error: %w", partNumber, err),
			}
			return
		}

		uploadPartOutput, err := u.client.UploadPart(ctx, u.lev.HashUploadPart(data, uploadPartInput))
		bytebufferpool.Put(part.bb)

		if err != nil {
			outputs <- uploadOutput{
				err: fmt.Errorf("upload part %d error: %w", partNumber, err),
			}
			return
		}

		select {
		case outputs <- uploadOutput{
			part: types.CompletedPart{
				ChecksumCRC32:     uploadPartOutput.ChecksumCRC32,
				ChecksumCRC32C:    uploadPartOutput.ChecksumCRC32C,
				ChecksumCRC64NVME: uploadPartOutput.ChecksumCRC64NVME,
				ChecksumSHA1:      uploadPartOutput.ChecksumSHA1,
				ChecksumSHA256:    uploadPartOutput.ChecksumSHA256,
				ETag:              uploadPartOutput.ETag,
				PartNumber:        &partNumber,
			},
		}:
		case <-ctx.Done():
			return
		}
	}
}
