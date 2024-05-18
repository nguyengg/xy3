package download

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"golang.org/x/time/rate"
	"hash"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// defaultPartSize is the size in bytes of each part.
const defaultPartSize = 8_388_608

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
	logger := log.New(os.Stderr, `"`+filepath.Base(name)+`" `, log.LstdFlags|log.Lmsgprefix)

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
	var h hash.Hash
	switch {
	case strings.HasPrefix(man.Checksum, "sha384-"):
		h = sha512.New384()
	case strings.HasPrefix(man.Checksum, "sha256-"):
		h = sha256.New()
	case man.Checksum == "":
		break
	default:
		return fmt.Errorf("unknown checksum algorithm: %v", man.Checksum)
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
			logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				logger.Printf("delete file error: %v", err)
			}
		}
	}(file)

	logger.Printf(`start downloading %d parts from "s3://%s/%s" to "%s"`, partCount, man.Bucket, man.Key, file.Name())

	// for download progress, only log every few seconds.
	sometimes := rate.Sometimes{Interval: 3 * time.Second}
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// first loop starts all the goroutines that are responsible for downloading the parts concurrently.
	inputs := make(chan downloadInput, partCount)
	outputs := make(chan downloadOutput, partCount)
	for i := 0; i < c.MaxConcurrency; i++ {
		go c.do(ctx, s3.GetObjectInput{
			Bucket:              &man.Bucket,
			Key:                 &man.Key,
			ExpectedBucketOwner: man.ExpectedBucketOwner,
		}, partCount, inputs, outputs)
	}

	// main goroutine is responsible for sending each part to the inputCh, then reading outputCh to report progress and
	// assemble file. we know main goroutine will never block because the capacities of inputCh and outputCh equal to
	// exact number of parts.
	for partNumber, start := 1, 0; ; {
		if partNumber == partCount {
			inputs <- downloadInput{
				PartNumber: partNumber,
				Range:      fmt.Sprintf("bytes=%d-", start),
			}
			break
		}

		inputs <- downloadInput{
			PartNumber: partNumber,
			Range:      fmt.Sprintf("bytes=%d-%d", start, start+partSize-1),
		}

		partNumber++
		start += partSize
	}
	close(inputs)

	// now wait for all downloads to complete.
	// store the parts but if the next part is available for writing to file then do so right away to keep the part
	// mappings small.
	parts := make(map[int]*downloadOutput, partCount)
	n := 0
	for i := 1; i <= partCount; {
		select {
		case result := <-outputs:
			if result.Err != nil {
				return result.Err
			}

			parts[result.PartNumber] = &result
			n++

			for part, ok := parts[i]; ok; {
				if _, err = file.Write(part.Data); err != nil {
					return fmt.Errorf("write part %d/%d to file error: %w", i, partCount, err)
				}
				if h != nil {
					if _, err = h.Write(part.Data); err != nil {
						return fmt.Errorf("compute file checksum error: %w", err)
					}
				}

				delete(parts, i)

				i++
				part, ok = parts[i]
			}

			sometimes.Do(func() {
				logger.Printf("downloaded %d/%d and wrote %d parts so far", n, partCount, i)
			})
		case <-ctx.Done():
			logger.Printf("cancelled")
			return nil
		case <-ticker.C:
			sometimes.Do(func() {
				logger.Printf("no new part; downloaded %d/%d and wrote %d parts so far", n, partCount, i)
			})
		}

	}
	close(outputs)

	logger.Printf("done downloading")
	success = true

	if h == nil {
		logger.Printf("no checksum to verify")
		return nil
	}

	expected := strings.SplitN(man.Checksum, "-", 2)[1]
	actual := base64.StdEncoding.EncodeToString(h.Sum(nil))
	if expected != actual {
		logger.Printf("checksum does not match: expect %s, got %s", expected, actual)
	} else {
		logger.Printf("checksum matches")
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
