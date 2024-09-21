package xy3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"math"
)

// Downloader is used to download files from S3 using ranged get with progress report.
type Downloader struct {
	// PartSize is the size of each part.
	//
	// Defaults to MinPartSize. Cannot be non-positive.
	PartSize int64

	// Concurrency is the number of goroutines responsible for uploading the parts in parallel.
	//
	// Defaults to DefaultConcurrency. Cannot be non-positive.
	Concurrency int

	// PostGetPart is called after every successful ranged [s3.Client.GetObject].
	//
	// By default, `log.Printf` will be used to print messages in format `downloaded %d/%d parts`. This hook will only
	// be called from the main goroutine that calls Download; the hook will be called right after the data slice have
	// been written to file. It can be used to hash the file as there is a guaranteed ordering (ascending part number
	// starting at 1, ending at partCount inclusive) to these callbacks, though it would be preferable to wrap the
	// io.Writer passed into Download as an io.MultiWriter instead.
	//
	// Implementations must not retain the data slice.
	PostGetPart func(data []byte, partNumber, partCount int)

	// ModifyHeadObjectInput provides ways to customise the initial S3 HeadObject call to retrieve the size.
	//
	// See AddExpectedBucketOwnerToHeadObject for an example.
	ModifyHeadObjectInput func(*s3.HeadObjectInput)

	// ModifyGetObjectInput provides ways to customise the S3 GetObject calls to download each part.
	//
	// See AddExpectedBucketOwnerToGetObject for an example.
	ModifyGetObjectInput func(*s3.GetObjectInput)

	client DownloadAPIClient
}

// AddExpectedBucketOwnerToHeadObject modifies the s3.HeadObjectInput by adding the expected bucket owner.
func AddExpectedBucketOwnerToHeadObject(expectedBucketOwner string) func(*s3.HeadObjectInput) {
	return func(input *s3.HeadObjectInput) {
		input.ExpectedBucketOwner = &expectedBucketOwner
	}
}

// AddExpectedBucketOwnerToGetObject modifies the s3.GetObjectInput by adding the expected bucket owner.
func AddExpectedBucketOwnerToGetObject(expectedBucketOwner string) func(*s3.GetObjectInput) {
	return func(input *s3.GetObjectInput) {
		input.ExpectedBucketOwner = &expectedBucketOwner
	}
}

func newDownloader(client DownloadAPIClient, optFns ...func(*Downloader)) (*Downloader, error) {
	d := &Downloader{
		PartSize:    MinPartSize,
		Concurrency: DefaultConcurrency,
		PostGetPart: func(data []byte, partNumber, partCount int) {
			log.Printf("downloaded %d/%d parts", partNumber, partCount)
		},
		client: client,
	}
	for _, fn := range optFns {
		fn(d)
	}

	if d.PartSize <= 0 {
		return nil, fmt.Errorf("partSize (%d) must be greater than 0", d.PartSize)
	}
	if d.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency (%d) must be greater than 0", d.Concurrency)
	}

	return d, nil
}

func (d Downloader) download(ctx context.Context, bucket, key string, w io.Writer) error {
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
		go d.newWorker(bucket, key, partCount).do(ctx, inputs, outputs)
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
				PartNumber: partNumber,
				Range:      fmt.Sprintf("bytes=%d-", startRange),
			}
			break
		}

		for {
			select {
			case inputs <- downloadInput{
				PartNumber: partNumber,
				Range:      fmt.Sprintf("bytes=%d-%d", startRange, startRange+partSize-1),
			}:
				partNumber++
				startRange += partSize
				continue partLoop
			case result := <-outputs:
				if result.Err != nil {
					return result.Err
				}

				parts[result.PartNumber] = &result
				downloadedPartCount++

				for part, ok := parts[nextPartToWrite]; ok; {
					if _, err = w.Write(part.Data); err != nil {
						close(inputs)
						return fmt.Errorf("write part %d/%d to file error: %w", nextPartToWrite, partCount, err)
					}
					if d.PostGetPart != nil {
						d.PostGetPart(part.Data, nextPartToWrite, partCount)
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
			if result.Err != nil {
				return result.Err
			}

			parts[result.PartNumber] = &result
			downloadedPartCount++

			for part, ok := parts[nextPartToWrite]; ok; {
				if _, err = w.Write(part.Data); err != nil {
					return fmt.Errorf("write part %d/%d to file error: %w", nextPartToWrite, partCount, err)
				}
				if d.PostGetPart != nil {
					d.PostGetPart(part.Data, nextPartToWrite, partCount)
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

func (d Downloader) newWorker(bucket, key string, partCount int) *downloadWorker {
	return &downloadWorker{d, bucket, key, partCount}
}

type downloadInput struct {
	PartNumber int
	Range      string
}

type downloadOutput struct {
	PartNumber int
	Data       []byte
	Err        error
}

type downloadWorker struct {
	Downloader
	bucket    string
	key       string
	partCount int
}

func (w *downloadWorker) do(ctx context.Context, inputs <-chan downloadInput, outputs chan<- downloadOutput) {
	for {
		select {
		case part, ok := <-inputs:
			if !ok {
				return
			}

			getObjectInput := &s3.GetObjectInput{Bucket: &w.bucket, Key: &w.key, Range: &part.Range}
			if w.ModifyGetObjectInput != nil {
				w.ModifyGetObjectInput(getObjectInput)
			}

			getObjectOutput, err := w.client.GetObject(ctx, getObjectInput)
			if err != nil {
				outputs <- downloadOutput{
					PartNumber: part.PartNumber,
					Err:        fmt.Errorf("get part %d/%d (%s) error: %w", part.PartNumber, w.partCount, part.Range, err),
				}
				return
			}

			data, err := io.ReadAll(getObjectOutput.Body)
			_ = getObjectOutput.Body.Close()
			if err != nil {
				outputs <- downloadOutput{
					PartNumber: part.PartNumber,
					Err:        fmt.Errorf("read part %d/%d (%s) error: %w", part.PartNumber, w.partCount, part.Range, err),
				}
				return
			}

			outputs <- downloadOutput{
				PartNumber: part.PartNumber,
				Data:       data,
			}
		case <-ctx.Done():
			return
		}
	}
}
