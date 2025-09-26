package upload

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/go-aws-commons/s3writer"
	"github.com/nguyengg/xy3/internal"
)

type Command struct {
	S3Location       string `short:"u" long:"s3-location" description:"name of the S3 bucket and optional key prefix in format s3://bucket/prefix" value-name:"S3_LOCATION" required:"true"`
	MaxBytesInSecond int64  `long:"throttle" description:"limits the number of bytes that are uploaded in one second; the zero-value indicates no limit."`
	Args             struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local directories to be uploaded to S3 as archives." required:"yes"`
	} `positional-args:"yes"`

	bucket, prefix string
	cfg            internal.BucketConfig
	client         *s3.Client
	logger         *log.Logger
}

func (c *Command) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	if c.MaxBytesInSecond <= 0 {
		return fmt.Errorf("--throttle must be non-negative")
	}

	c.bucket, c.prefix, err = internal.ParseS3URI(c.S3Location)
	if err != nil {
		return fmt.Errorf(`invalid s3 uri "%s": %w`, c.S3Location, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	c.cfg = internal.ConfigForBucket(c.bucket)
	c.client, err = internal.NewS3ClientFromProfile(ctx, c.cfg.AWSProfile)
	if err != nil {
		return fmt.Errorf("create s3 client error: %w", err)
	}

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)

		if err = c.upload(ctx, string(file)); err == nil {
			success++
			continue
		}

		// if an error happens due to context being cancelled (interrupt signal), manually log about whether the
		// multipart upload was successfully aborted.
		if errors.Is(err, context.Canceled) {
			var mErr = s3writer.MultipartUploadError{}
			if errors.As(err, &mErr) {
				switch mErr.Abort {
				case s3writer.AbortSuccess:
					c.logger.Printf("upload was interrupted and its multipart upload was aborted successfully")
				case s3writer.AbortFailure:
					c.logger.Printf("upload was interrupted and its multipart upload (upload Id %s) was not aborted successfully: %v", mErr.UploadID, mErr.AbortErr)
				default:
					c.logger.Printf("upload was interrupted without an attempt to abort its multipart upload (upload Id %s)", mErr.UploadID)
				}
				break
			}

			c.logger.Printf("upload was interrupted without having started a multipart upload")
			break
		}

		c.logger.Printf("upload error: %v", err)
	}

	log.Printf("successfully uploaded %d/%d files", success, n)
	return nil
}
