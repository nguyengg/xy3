package xy3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/time/rate"
)

// DownloadOptions customises various aspects of a Download operation.
type DownloadOptions struct {
	// PartSize is the size of each part.
	//
	// Defaults to MinPartSize. There is no technical limit to this number so be reasonable.
	PartSize int64

	// Concurrency is the number of goroutines responsible for uploading the parts in parallel.
	//
	// Defaults to DefaultConcurrency. Must be a positive integer.
	Concurrency int

	// MaxBytesInSecond is used to rate limit the amount of bytes that are downloaded in one second.
	//
	// The zero-value indicates no limit. Must be positive integer.
	MaxBytesInSecond int64

	// PostGetPart is called after every successful ranged [s3.Client.GetObject].
	//
	// By default, `log.Printf` will be used to print messages in format `downloaded %d/%d parts`. This hook will
	// only be called from the main goroutine that calls Download; the hook will be called right before the data
	// slice is written to file. It can be used to hash the file as there is a guaranteed ordering (ascending
	// part number starting at 1, ending at partCount inclusive) to these callbacks, though it would be preferable
	// to wrap the io.Writer passed into Download as an io.MultiWriter instead.
	//
	// Implementations must not retain the data slice. Size is the file's Content-Length determined from the S3
	// HeadObject request (total size to be downloaded) for progress reporting.
	PostGetPart func(data []byte, size int64, partNumber, partCount int)

	// ModifyHeadObjectInput provides ways to customise the initial S3 HeadObject call to retrieve the size.
	//
	// See WithExpectedBucketOwner for an example.
	ModifyHeadObjectInput func(*s3.HeadObjectInput)

	// ModifyGetObjectInput provides ways to customise the S3 GetObject calls to download each part.
	//
	// See WithExpectedBucketOwner for an example.
	ModifyGetObjectInput func(*s3.GetObjectInput)
}

// WithExpectedBucketOwner overrides [DownloadOptions.ModifyHeadObjectInput] and [DownloadOptions.ModifyGetObjectInput]
// to attach the given expectedBucketOwner to the HeadObject and ranged GetObject requests.
func WithExpectedBucketOwner(expectedBucketOwner string) func(*DownloadOptions) {
	return func(options *DownloadOptions) {
		options.ModifyHeadObjectInput = func(input *s3.HeadObjectInput) {
			input.ExpectedBucketOwner = &expectedBucketOwner
		}
		options.ModifyGetObjectInput = func(input *s3.GetObjectInput) {
			input.ExpectedBucketOwner = &expectedBucketOwner
		}
	}
}

type downloader struct {
	DownloadOptions

	client  DownloadAPIClient
	bufPool *sync.Pool
	limiter *rate.Limiter
}

type downloadInput struct {
	partNumber int
	partRange  string
}

type downloadOutput struct {
	partNumber int
	buf        *bytes.Buffer
	err        error
}

func newDownloader(client DownloadAPIClient, optFns ...func(*DownloadOptions)) (*downloader, error) {
	opts := DownloadOptions{
		PartSize:         MinPartSize,
		Concurrency:      DefaultConcurrency,
		MaxBytesInSecond: 0,
		PostGetPart: func(_ []byte, _ int64, partNumber, partCount int) {
			log.Printf("downloaded %d/%d parts", partNumber, partCount)
		},
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	d := &downloader{
		DownloadOptions: opts,
		client:          client,
		bufPool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}

	if d.PartSize <= 0 {
		return nil, fmt.Errorf("partSize (%d) must be positive", d.PartSize)
	}
	if d.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency (%d) must be positive", d.Concurrency)
	}
	if d.MaxBytesInSecond < 0 {
		return nil, fmt.Errorf("maxBytesInSecond (%d) cannot be negative", d.MaxBytesInSecond)
	} else if d.MaxBytesInSecond == 0 {
		d.limiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		d.limiter = rate.NewLimiter(rate.Limit(d.MaxBytesInSecond), int(d.PartSize))
	}

	return d, nil
}

func (d *downloader) download(ctx context.Context, bucket, key string, w io.Writer) error {
	// the S3 HeadObject request will give the size of the file which can be used to do range GETs.
	headObjectInput := &s3.HeadObjectInput{Bucket: &bucket, Key: &key}
	if d.ModifyHeadObjectInput != nil {
		d.ModifyHeadObjectInput(headObjectInput)
	}

	headObjectOutput, err := d.client.HeadObject(ctx, headObjectInput)
	if err != nil {
		return err
	}
	size := *headObjectOutput.ContentLength
	partSize := d.PartSize
	partCount := int(math.Ceil(float64(size) / float64(partSize)))

	// start the workers here.
	inputs := make(chan downloadInput, d.Concurrency)
	outputs := make(chan downloadOutput, d.Concurrency)
	for range d.Concurrency {
		go d.poll(ctx, inputs, outputs, bucket, key)
	}

	// main goroutine is responsible for:
	//	1. sending each part to the inputs channel.
	//	2. simultaneously and afterward read from outputs channel to report progress and write to file.
	//
	// the downloaded parts are stored in this map, and if the next part is available for writing to file then perform
	// the write right away to keep as little data in memory as possible.
	parts := make(map[int]*downloadOutput, partCount)
	downloadedPartCount := 0
	nextPartToWrite := 1
partLoop:
	for partNumber, startRange := 1, int64(0); ; {
		if partNumber == partCount {
			inputs <- downloadInput{
				partNumber: partNumber,
				partRange:  fmt.Sprintf("bytes=%d-", startRange),
			}
			break
		}

		for {
			select {
			case inputs <- downloadInput{
				partNumber: partNumber,
				partRange:  fmt.Sprintf("bytes=%d-%d", startRange, startRange+partSize-1),
			}:
				partNumber++
				startRange += partSize
				continue partLoop
			case result := <-outputs:
				if result.err != nil {
					return result.err
				}

				parts[result.partNumber] = &result
				downloadedPartCount++

				for part, ok := parts[nextPartToWrite]; ok; {
					if d.PostGetPart != nil {
						d.PostGetPart(part.buf.Bytes(), size, nextPartToWrite, partCount)
					}

					_, err = part.buf.WriteTo(w)
					d.bufPool.Put(part.buf)

					if err != nil {
						close(inputs)
						return fmt.Errorf("write part %d/%d to file error: %w", nextPartToWrite, partCount, err)
					}

					delete(parts, nextPartToWrite)

					nextPartToWrite++
					part, ok = parts[nextPartToWrite]
				}
			case <-ctx.Done():
				close(inputs)
				return ctx.Err()
			}
		}
	}

	// close inputs channel to allow workers to wind down. don't worry about closing outputs as gc can take care of it.
	close(inputs)

	// now continue receiving and writing remaining download parts.
	for nextPartToWrite <= partCount {
		select {
		case result := <-outputs:
			if result.err != nil {
				return result.err
			}

			parts[result.partNumber] = &result
			downloadedPartCount++

			for part, ok := parts[nextPartToWrite]; ok; {
				if d.PostGetPart != nil {
					d.PostGetPart(part.buf.Bytes(), size, nextPartToWrite, partCount)
				}

				_, err = part.buf.WriteTo(w)
				d.bufPool.Put(part.buf)

				if err != nil {
					return fmt.Errorf("write part %d/%d to file error: %w", nextPartToWrite, partCount, err)
				}

				delete(parts, nextPartToWrite)

				nextPartToWrite++
				part, ok = parts[nextPartToWrite]
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (d *downloader) poll(ctx context.Context, inputs <-chan downloadInput, outputs chan<- downloadOutput, bucket, key string) {
	for {
		select {
		case part, ok := <-inputs:
			if !ok {
				return
			}

			getObjectInput := &s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &key,
				Range:  &part.partRange,
			}
			if d.ModifyGetObjectInput != nil {
				d.ModifyGetObjectInput(getObjectInput)
			}

			// rate limiting happens here.
			if err := d.limiter.WaitN(ctx, int(d.PartSize)); err != nil {
				outputs <- downloadOutput{
					partNumber: part.partNumber,
					err:        fmt.Errorf("wait to download part %d (%s) error: %w", part.partNumber, part.partRange, err),
				}
				return
			}

			getObjectOutput, err := d.client.GetObject(ctx, getObjectInput)
			if err != nil {
				outputs <- downloadOutput{
					partNumber: part.partNumber,
					err:        fmt.Errorf("get part %d (%s) error: %w", part.partNumber, part.partRange, err),
				}
				return
			}

			buf := d.bufPool.Get().(*bytes.Buffer)
			buf.Reset()
			_, err = buf.ReadFrom(getObjectOutput.Body)
			_ = getObjectOutput.Body.Close()
			if err != nil {
				outputs <- downloadOutput{
					partNumber: part.partNumber,
					err:        fmt.Errorf("read part %d (%s) error: %w", part.partNumber, part.partRange, err),
				}
				return
			}

			outputs <- downloadOutput{
				partNumber: part.partNumber,
				buf:        buf,
			}
		case <-ctx.Done():
			return
		}
	}
}
