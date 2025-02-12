package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/nguyengg/xy3/s3writer"
)

func (c *Command) upload(ctx context.Context, name string) error {
	// prepare validates and possibly performs compression to file if name is a directory.
	f, size, contentType, err := c.prepare(ctx, name)
	if err != nil {
		return err
	}

	defer f.Close()

	// find an unused S3 key that can be used for the CreateMultipartUpload call.
	filename := f.Name()
	stem, ext := xy3.StemAndExt(filename)
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

	c.logger.Printf(`uploading to "s3://%s/%s"`, c.Bucket, key)

	w, err := s3writer.New(ctx, c.client, &s3.PutObjectInput{
		Bucket:              aws.String(c.Bucket),
		Key:                 aws.String(key),
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		ContentType:         contentType,
		Metadata:            map[string]string{"name": filename},
		StorageClass:        types.StorageClass(c.StorageClass),
	}, func(options *s3writer.Options) {
		options.Concurrency = c.MaxConcurrency
	})
	if err != nil {
		return fmt.Errorf("create s3 writer error: %w", err)
	}

	// while reading from f, also write to hash and progress bar.
	hash, bar := namedhash.New(), internal.DefaultBytes(size, "uploading")
	if _, err = f.WriteTo(io.MultiWriter(w, hash, bar)); err != nil {
		return fmt.Errorf("read from file error: %w", err)
	}
	if _, err = bar.Close(), w.Close(); err != nil {
		return fmt.Errorf("upload to s3 error: %w", err)
	}

	c.logger.Printf("done uploading")

	// now generate the local .s3 file that contains the S3 URI. if writing to file fails, prints the JSON content
	// to standard output so that they can be saved manually later.
	m.Checksum = hash.SumToString(nil)
	mf, err := xy3.OpenExclFile(".", stem, ext+".s3")
	if err != nil {
		return err
	}
	if err, _ = m.MarshalTo(mf), mf.Close(); err != nil {
		return err
	}

	c.logger.Printf(`wrote to manifest "%s"`, mf.Name())

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
		key = fmt.Sprintf("%s%s-%d%s", c.Prefix, stem, i, ext)
	}

	return key, nil
}
