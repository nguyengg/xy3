package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

type Metadata struct {
	ExpectedBucketOwner *string `long:"expected-bucket-owner" description:"optional ExpectedBucketOwner field to apply when the manifest does not have its own expectedBucketOwner"`

	Args struct {
		S3Location string `positional-arg-name:"s3://bucket/prefix" description:"the S3 bucket name and optional prefix" required:"yes"`
	} `positional-args:"yes"`

	client *s3.Client
	logger *log.Logger
}

func (c *Metadata) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load default config error:%w", err)
	}

	c.client = s3.NewFromConfig(cfg, func(options *s3.Options) {
		// without this, getting a bunch of WARN message below:
		// WARN Response has no supported checksum. Not validating response payload.
		options.DisableLogOutputChecksumValidationSkipped = true
	})

	bucket, key, err := internal.ParseS3URI(c.Args.S3Location)
	if err != nil {
		return fmt.Errorf("invalid s3 uri: %w", err)
	}
	var prefix *string
	if key != "" {
		prefix = &key
	}

	for paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket:              aws.String(bucket),
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		Prefix:              prefix,
	}); paginator.HasMorePages(); {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects error: %w", err)
		}

		for _, obj := range page.Contents {
			// HeadObject to get metadata.
			headObjectResult, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:              aws.String(bucket),
				Key:                 obj.Key,
				ExpectedBucketOwner: c.ExpectedBucketOwner,
			})
			if err != nil {
				return fmt.Errorf(`get metadata about "%s" error: %w`, aws.ToString(obj.Key), err)
			}

			m := manifest.Manifest{
				Bucket:              bucket,
				Key:                 aws.ToString(obj.Key),
				ExpectedBucketOwner: c.ExpectedBucketOwner,
				Size:                aws.ToInt64(obj.Size),
				Checksum:            headObjectResult.Metadata["checksum"],
			}

			stem, ext := util.StemAndExt(m.Key)
			f, err := util.OpenExclFile(".", stem, ext+".s3", 0666)
			if err != nil {
				return fmt.Errorf("create file error: %w", err)
			}

			if err, _ = m.MarshalTo(f), f.Close(); err != nil {
				return fmt.Errorf("write manifest error: %w", err)
			}

			log.Printf("wrote %s", f.Name())
		}
	}

	return nil
}
