package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/cksum"
	"github.com/nguyengg/xy3/internal/manifest"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const (
	maxUploadSize = int64(1_099_511_627_776)
	maxPartCount  = 10_000
	minPartSize   = 5_242_880
	maxPartSize   = 5_368_709_120
)

// defaultPartSize is the size in bytes of each part.
const defaultPartSize = int(8_388_608)

type uploadInput struct {
	PartNumber int32
	Data       []byte
}

type uploadOutput struct {
	Part s3types.CompletedPart
	Err  error
}

func (c *Command) upload(ctx context.Context, name string) error {
	basename := filepath.Base(name)
	logger := log.New(os.Stderr, `"`+basename+`" `, log.LstdFlags|log.Lmsgprefix)

	// preflight involves validation and possibly compressing a directory.
	filename, ext, size, contentType, err := c.preflight(ctx, logger, name)
	if size > maxUploadSize {
		return fmt.Errorf("upload size (%d - %s) is larger than limit (%d - %s)",
			size, humanize.Bytes(uint64(size)),
			maxUploadSize, humanize.Bytes(uint64(maxUploadSize)))
	}
	if size == 0 {
		return fmt.Errorf("file is empty")
	}

	// we do know the exact number of parts since we know the file's size and the size of each part.
	partSize := defaultPartSize
	partCount := int(math.Ceil(float64(size) / float64(partSize)))
	if partCount > maxPartCount {
		return fmt.Errorf("part size too small as it result in part count (%d) exceeding limit (%d)", partCount, maxPartCount)
	}

	// for upload progress, only log every few seconds.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// find an unused S3 key that can be used for the CreateMultipartUpload call.
	key, err := c.findUnusedS3Key(ctx, strings.TrimSuffix(filename, ext), ext)
	if err != nil {
		return err
	}

	man := manifest.Manifest{
		Bucket:              c.Bucket,
		Key:                 key,
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		Size:                size,
	}

	logger.Printf(`start uploading %d parts to "s3://%s/%s"`, partCount, c.Bucket, key)

	// start the multipart upload, and if the operation fails then use a deferred function to abort the multipart upload.
	createMultipartUploadOutput, err := c.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:              aws.String(c.Bucket),
		Key:                 aws.String(key),
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		ContentType:         contentType,
		Metadata:            map[string]string{"name": filename},
		StorageClass:        s3types.StorageClassIntelligentTiering,
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			err = fmt.Errorf("create multipart upload error: %w", err)
		}
		return err
	}

	success := false
	defer func() {
		if !success {
			logger.Printf(`aborting multipart upload "%s"`, aws.ToString(createMultipartUploadOutput.UploadId))
			if _, err := c.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:              aws.String(c.Bucket),
				Key:                 aws.String(key),
				UploadId:            createMultipartUploadOutput.UploadId,
				ExpectedBucketOwner: c.ExpectedBucketOwner,
			}); err != nil {
				logger.Printf(`abort multipart upload "%s" error: %v"`, aws.ToString(createMultipartUploadOutput.UploadId), err)
			}
		}
	}()

	// first loop starts all the goroutines that are responsible for uploading the parts concurrently.
	inputs := make(chan uploadInput, c.MaxConcurrency)
	outputs := make(chan uploadOutput, c.MaxConcurrency)
	closeInputs := sync.OnceFunc(func() { close(inputs) })
	closeOutputs := sync.OnceFunc(func() { close(outputs) })
	defer func() {
		closeInputs()
		closeOutputs()
	}()

	for i := 0; i < c.MaxConcurrency; i++ {
		go func() {
			c.do(ctx, s3.UploadPartInput{
				Bucket:              &c.Bucket,
				Key:                 &key,
				UploadId:            createMultipartUploadOutput.UploadId,
				ExpectedBucketOwner: c.ExpectedBucketOwner,
			}, partCount, inputs, outputs)
		}()
	}

	// main goroutine is responsible for:
	//	1. divvy up the file into parts.
	//	2. send each part to the inputs channel. also compute the hash checksum of the file.
	//	3. simultaneously and afterward read from outputs channel to report progress.
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open file error: %w", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	parts := make([]s3types.CompletedPart, 0, partCount)
	remainingSize := size
	hash := cksum.NewHasher()
partLoop:
	for partNumber, n := 1, 0; partNumber <= partCount; partNumber++ {
		data := make([]byte, partSize)

		n, err = file.Read(data)
		if err != nil {
			return fmt.Errorf("read file error: %w", err)
		}

		if remainingSize -= int64(n); n != partSize {
			// the last part might be truncated but never 0.
			if remainingSize != 0 {
				return fmt.Errorf("read only %d/%d bytes", n, partSize)
			}
			data = data[:n]
		}
		if _, err = hash.Write(data); err != nil {
			return fmt.Errorf("hash data error: %w", err)
		}

		for {
			select {
			case inputs <- uploadInput{
				PartNumber: int32(partNumber),
				Data:       data,
			}:
				continue partLoop
			case result := <-outputs:
				if result.Err != nil {
					return result.Err
				}

				parts = append(parts, result.Part)
			case <-ctx.Done():
				logger.Printf("cancelled")
				return nil
			case <-ticker.C:
				logger.Printf("uploading %d/%d parts so far", partNumber, partCount)
			}
		}
	}
	if remainingSize != 0 {
		return fmt.Errorf("expected remaining size to be 0, got %d", remainingSize)
	}
	closeInputs()

	// now wait for all uploads to complete.
	for i := len(parts); i < partCount; {
		select {
		case result := <-outputs:
			if result.Err != nil {
				return result.Err
			}

			i++
			parts = append(parts, result.Part)
		case <-ctx.Done():
			logger.Printf("cancelled")
			return nil
		case <-ticker.C:
			logger.Printf("uploaded %d/%d parts so far", i+1, partCount)
		}
	}
	closeOutputs()

	// must sort all parts by PartNumber because S3 can't be bothered to do this.
	slices.SortFunc(parts, func(a, b s3types.CompletedPart) int {
		return int(*a.PartNumber - *b.PartNumber)
	})

	// only mark the upload operation successful if CompleteMultipartUpload also succeeds.
	// deleting file can fail but that won't count as an upload failure.
	if _, err = c.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:              aws.String(c.Bucket),
		Key:                 aws.String(key),
		UploadId:            createMultipartUploadOutput.UploadId,
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		MultipartUpload:     &s3types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		if !errors.Is(err, context.Canceled) {
			err = fmt.Errorf("complete multipart upload error: %w", err)
		}
		return err
	}

	logger.Printf("done uploading")
	success = true

	// now generate the local .s3 file that contains the S3 URI. if writing to file fails, prints the JSON content to
	// standard output so that they can be saved manually later.
	man.Checksum = hash.SumToChecksumString(nil)
	if file, err = internal.OpenExclFile(strings.TrimSuffix(filename, ext), ext+".s3"); err == nil {
		err = man.MarshalTo(file)
	}
	if _ = file.Close(); err != nil {
		return err
	}
	logger.Printf(`wrote to manifest "%s"`, file.Name())

	if c.Delete {
		logger.Printf(`deleting file "%s"`, filename)
		if err = os.Remove(filename); err != nil {
			logger.Printf("delete file error: %v", err)
		}
	}

	return nil
}

// findUnusedS3Key returns an S3 key pointing to a non-existing S3 object that can be used to upload file.
func (c *Command) findUnusedS3Key(ctx context.Context, stem, ext string) (string, error) {
	key := c.Prefix + stem + ext
	for i := 0; ; {
		if _, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:              aws.String(c.Bucket),
			Key:                 aws.String(key),
			ExpectedBucketOwner: c.ExpectedBucketOwner,
		}); err != nil {
			if errors.Is(err, context.Canceled) {
				return "", err
			}

			var re *awshttp.ResponseError
			if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
				break
			}

			return "", fmt.Errorf("find unused S3 key error: %w", err)
		}
		i++
		key = c.Prefix + stem + "-" + strconv.Itoa(i) + ext
	}

	return key, nil
}

// do is supposed to be run in a goroutine to poll from inputs channel and sends results to outputs channel.
//
// The method returns only upon inputs being closed, or if the upload of any part fails.
func (c *Command) do(ctx context.Context, input s3.UploadPartInput, partCount int, inputs <-chan uploadInput, outputs chan<- uploadOutput) {
	for {
		select {
		case part, ok := <-inputs:
			if !ok {
				return
			}

			input.PartNumber = aws.Int32(part.PartNumber)
			input.Body = bytes.NewReader(part.Data)
			uploadPartOutput, err := c.client.UploadPart(ctx, &input)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					err = fmt.Errorf("upload part %d/%d error: %w", part.PartNumber, partCount, err)
				}

				outputs <- uploadOutput{Err: err}
				return
			}

			outputs <- uploadOutput{
				Part: s3types.CompletedPart{
					ETag:       uploadPartOutput.ETag,
					PartNumber: aws.Int32(part.PartNumber),
				},
			}
		case <-ctx.Done():
			return
		}
	}
}
