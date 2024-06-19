package download

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/cksum"
	"github.com/nguyengg/xy3/internal/manifest"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// defaultPartSize is the size in bytes of each part.
const defaultPartSize = xy3.MinPartSize // 8_388_608

type downloadInput struct {
	PartNumber int
	Range      string
}

type downloadOutput struct {
	PartNumber int
	Data       []byte
	Err        error
}

func (c *Command) download(ctx context.Context, name string) error {
	file, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("open file error: %w", err)
	}
	man, err := manifest.UnmarshalFrom(file)
	if _ = file.Close(); err != nil {
		return err
	}
	basename := filepath.Base(man.Key)
	ext := filepath.Ext(basename)

	// while downloading, also computes checksum to verify against the downloaded content.
	h, err := cksum.NewFromChecksumString(man.Checksum)
	if err != nil {
		return err
	}

	// the S3 HeadObject request will give the size of the file which can be used to do range GETs.
	headObjectOutput, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		var re *awshttp.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
			return fmt.Errorf("s3 object does not exist")
		}

	}
	size := *headObjectOutput.ContentLength
	partSize := defaultPartSize
	partCount := int(math.Ceil(float64(size) / float64(partSize)))

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file complete, clean up by deleting the local file.
	file, err = internal.OpenExclFile(strings.TrimSuffix(basename, ext), ext)
	success := false
	defer func(file *os.File) {
		if name, _ = file.Name(), file.Close(); !success {
			c.logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				c.logger.Printf("delete file error: %v", err)
			}
		}
	}(file)

	bar := internal.DefaultBytes(size, "downloading")

	var w io.Writer
	if h != nil {
		w = io.MultiWriter(file, bar, h)
	} else {
		w = io.MultiWriter(file, bar)
	}

	c.logger.Printf(`downloading %s from "s3://%s/%s" to "%s"`, humanize.Bytes(uint64(size)), man.Bucket, man.Key, file.Name())

	// first loop starts all the goroutines that are responsible for downloading the parts concurrently.
	inputs := make(chan downloadInput, c.MaxConcurrency)
	outputs := make(chan downloadOutput, c.MaxConcurrency)
	closeInputs := sync.OnceFunc(func() { close(inputs) })
	closeOutputs := sync.OnceFunc(func() { close(outputs) })
	defer func() {
		closeInputs()
		closeOutputs()
	}()
	for i := 0; i < c.MaxConcurrency; i++ {
		go c.do(ctx, s3.GetObjectInput{
			Bucket:              &man.Bucket,
			Key:                 &man.Key,
			ExpectedBucketOwner: man.ExpectedBucketOwner,
		}, partCount, inputs, outputs)
	}

	// main goroutine is responsible for:
	//	1. sending each part to the inputs channel.
	//	2. simultaneously and afterward read from outputs channel to report progress and write to file.
	//
	// the downloaded parts are stored in this map, and if the next part is available for writing to file then do
	// right away to keep as little as data in memory as possible.
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
						return fmt.Errorf("write part %d/%d to file error: %w", nextPartToWrite-1, partCount, err)
					}

					delete(parts, nextPartToWrite)

					nextPartToWrite++
					part, ok = parts[nextPartToWrite]
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	closeInputs()

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

				delete(parts, nextPartToWrite)

				nextPartToWrite++
				part, ok = parts[nextPartToWrite]
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	closeOutputs()
	_ = bar.Close()
	success = true

	if h == nil {
		c.logger.Printf("done downloading; no checksum to verify")
		return nil
	}

	if actual := h.SumToChecksumString(nil); man.Checksum != actual {
		c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, actual)
	} else {
		c.logger.Printf("done downloading; checksum matches")
	}

	return nil
}

// do is supposed to be run in a goroutine to poll from inputs channel and sends results to outputs channel.
//
// The method returns only upon inputs being closed, or if the download of any part fails.
func (c *Command) do(ctx context.Context, input s3.GetObjectInput, partCount int, inputs <-chan downloadInput, outputs chan<- downloadOutput) {
	for {
		select {
		case part, ok := <-inputs:
			if !ok {
				return
			}

			input.Range = aws.String(part.Range)
			getObjectOutput, err := c.client.GetObject(ctx, &input)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					err = fmt.Errorf("get part %d/%d (%s) error: %w", part.PartNumber, partCount, part.Range, err)
				}

				outputs <- downloadOutput{
					PartNumber: part.PartNumber,
					Err:        err,
				}
				return
			}

			data, err := io.ReadAll(getObjectOutput.Body)
			_ = getObjectOutput.Body.Close()
			if err != nil {
				outputs <- downloadOutput{
					PartNumber: part.PartNumber,
					Err:        fmt.Errorf("read part %d/%d (%s) error: %w", part.PartNumber, partCount, part.Range, err),
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
