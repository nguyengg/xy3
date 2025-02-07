package upload

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
)

type Command struct {
	Bucket              string  `short:"b" long:"bucket" description:"name of the S3 bucket containing the files" required:"true"`
	Prefix              string  `short:"k" long:"key-prefix" description:"key prefix to apply to all S3 operations"`
	ExpectedBucketOwner *string `long:"expected-bucket-owner" description:"optional ExpectedBucketOwner field to apply to all S3 operations"`
	Delete              bool    `short:"d" long:"delete" description:"if given, the local files will be deleted only upon successful upload. If compressing a directory, the directory will not be deleted but the intermediate archive will be."`
	XZ                  bool    `long:"xz" description:"if given, local directories will be tar-ed and compressed instead of using zip by default"`
	MaxConcurrency      int     `short:"P" long:"max-concurrency" description:"use up to max-concurrency number of goroutines at a time for parallel uploads." default:"5"`
	Args                struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files or directories (after compressing the contents of the directory) to be uploaded to S3. By default, zip will be used to compress directory's contents.'" required:"yes"`
	} `positional-args:"yes"`

	client *s3.Client
	logger *log.Logger
}

func (c *Command) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	if c.MaxConcurrency <= 0 {
		return fmt.Errorf("max-concurrency must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load default config error:%w", err)
	}

	c.client = s3.NewFromConfig(cfg)

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
			var mErr = xy3.MultipartUploadError{}
			if errors.As(err, &mErr) {
				switch mErr.Abort {
				case xy3.AbortSuccess:
					c.logger.Printf("upload was interrupted and its multipart upload was aborted successfully")
				case xy3.AbortFailure:
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
