package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/cmd/awsconfig"
	"github.com/nguyengg/xy3/internal/manifest"
)

type Metadata struct {
	Args struct {
		S3Locations []string `positional-arg-name:"S3_LOCATION" description:"the S3 bucket names and optional key prefixes in format s3://bucket/prefix" required:"yes"`
	} `positional-args:"yes"`

	awsconfig.ConfigLoaderMixin

	client *s3.Client
	logger *log.Logger
}

func (c *Metadata) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := c.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load default config error:%w", err)
	}

	c.client = s3.NewFromConfig(cfg, func(options *s3.Options) {
		// without this, getting a bunch of WARN message below:
		// WARN Response has no supported checksum. Not validating response payload.
		options.DisableLogOutputChecksumValidationSkipped = true
	})

	success := 0
	n := len(c.Args.S3Locations)
	for i, s3location := range c.Args.S3Locations {
		bucket, key, err := internal.ParseS3URI(s3location)
		if err != nil {
			return fmt.Errorf(`invalid s3 uri "%s": %w`, s3location, err)
		}

		var prefix *string
		if key != "" {
			prefix = &key
		}

		c.logger = log.New(os.Stderr, fmt.Sprintf(`[%d/%d] s3://%s - `, i+1, n, bucket), 0)

		if err = c.retrieve(ctx, bucket, prefix); err == nil {
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("retrieve manifests error: %v", err)
	}

	return nil
}

func (c *Metadata) retrieve(ctx context.Context, bucket string, prefix *string) error {
	for paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: prefix,
	}); paginator.HasMorePages(); {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects error: %w", err)
		}

		for _, obj := range page.Contents {
			headObjectResult, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return fmt.Errorf(`get metadata about "%s" error: %w`, aws.ToString(obj.Key), err)
			}

			m := manifest.Manifest{
				Bucket:   bucket,
				Key:      aws.ToString(obj.Key),
				Size:     aws.ToInt64(obj.Size),
				Checksum: headObjectResult.Metadata["checksum"],
			}

			f, err := os.OpenFile(path.Base(m.Key)+".s3", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return fmt.Errorf("create manifest file error: %w", err)
			}
			name := f.Name()

			if err, _ = m.MarshalTo(f), f.Close(); err != nil {
				return fmt.Errorf(`write manifest to "%s" error: %w`, name, err)
			}

			if err = os.Chtimes(f.Name(), time.Time{}, aws.ToTime(headObjectResult.LastModified)); err != nil {
				c.logger.Printf(`wrote "%s"; change modify time error: %v`, name, err)
			} else {
				c.logger.Printf(`wrote "%s"`, name)
			}
		}
	}

	return nil
}
