package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/cksum"
	"github.com/nguyengg/xy3/internal/manifest"
)

type uploadInput struct {
	PartNumber int32
	Data       []byte
}

type uploadOutput struct {
	Part types.CompletedPart
	Err  error
}

func (c *Command) upload(ctx context.Context, name string) error {
	// preflight involves validation and possibly compressing a directory.
	filename, size, contentType, err := c.preflight(ctx, name)

	// find an unused S3 key that can be used for the CreateMultipartUpload call.
	stem, ext := internal.SplitStemAndExt(filename)
	key, err := c.findUnusedS3Key(ctx, stem, ext)
	if err != nil {
		return err
	}
	m := manifest.Manifest{
		Bucket:              c.Bucket,
		Key:                 key,
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		Size:                size,
	}

	hash := cksum.NewHasher()

	c.logger.Printf(`uploading %s to "s3://%s/%s"`, humanize.Bytes(uint64(size)), c.Bucket, key)

	if _, err = xy3.Upload(ctx, c.client, filename, &s3.CreateMultipartUploadInput{
		Bucket:              aws.String(c.Bucket),
		Key:                 aws.String(key),
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		ContentType:         contentType,
		Metadata:            map[string]string{"name": filename},
		StorageClass:        types.StorageClassIntelligentTiering,
	}, func(options *xy3.UploadOptions) {
		options.Concurrency = c.MaxConcurrency

		bar := internal.DefaultBytes(size, "uploading")

		var completedPartCount int32
		parts := make(map[int32]int)
		options.PreUploadPart = func(partNumber int32, data []byte) {
			n, _ := hash.Write(data)
			parts[partNumber] = n
		}

		options.PostUploadPart = func(part types.CompletedPart, partCount int32) {
			if completedPartCount++; completedPartCount == partCount {
				_ = bar.Close()
			} else {
				_ = bar.Add64(int64(parts[aws.ToInt32(part.PartNumber)]))
			}
		}
	}); err != nil {
		return err
	}

	c.logger.Printf("done uploading")

	// now generate the local .s3 file that contains the S3 URI. if writing to file fails, prints the JSON content to
	// standard output so that they can be saved manually later.
	m.Checksum = hash.SumToChecksumString(nil)
	f, err := internal.OpenExclFile(stem, ext+".s3")
	if err != nil {
		return err
	}
	if err, _ = m.MarshalTo(f), f.Close(); err != nil {
		return err
	}

	c.logger.Printf(`wrote to manifest "%s"`, f.Name())

	if c.Delete {
		c.logger.Printf(`deleting file "%s"`, filename)
		if err = os.Remove(filename); err != nil {
			c.logger.Printf("delete file error: %v", err)
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
				Part: types.CompletedPart{
					ETag:       uploadPartOutput.ETag,
					PartNumber: aws.Int32(part.PartNumber),
				},
			}
		case <-ctx.Done():
			return
		}
	}
}
