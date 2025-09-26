package download

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
	"github.com/nguyengg/xy3/internal"
)

type Command struct {
	DownloadManifests bool  `long:"manifests" description:"if specified, the positional arguments must be come S3 locations in format s3://bucket/prefix (optional prefix) in order to download manifests of files found in those S3 location"`
	NoExtract         bool  `long:"no-extract" description:"if specified, the downloaded archives will not be automatically decompressed and extracted if it's an archive"`
	MaxBytesInSecond  int64 `long:"throttle" description:"limits the number of bytes that are downloaded per second; the zero-value indicates no limit."`
	Args              struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI; or S3 URI in format s3://bucket/key to download directly from S3; or S3 locations in format s3://bucket/prefix to download manifests (with --manifests)"`
	} `positional-args:"yes"`

	logger *log.Logger
}

func (c *Command) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	if c.MaxBytesInSecond < 0 {
		return fmt.Errorf("--throttle must be non-negative")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	if c.DownloadManifests {
		var count, n int
		for _, s3Location := range c.Args.Files {
			if n, err = c.downloadManifests(ctx, string(s3Location)); err == nil {
				count += n
				continue
			}

			if errors.Is(err, context.Canceled) {
				break
			}

			log.Printf(`download manifests from "%s" error: %v`, s3Location, err)
		}

		log.Printf("successfully downloaded %d manifests", count)
		return nil
	}

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start downloading")

		name := string(file)
		if strings.HasPrefix(name, "s3://") {
			if err = c.downloadFromS3(ctx, name); err == nil {
				c.logger.Printf("done downloading")
				success++
				continue
			}
		} else if err = c.downloadFromManifest(ctx, name); err == nil {
			c.logger.Printf("done downloading")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("download error: %v", err)
	}

	log.Printf("successfully downloaded %d/%d files", success, n)
	return nil
}

func (c *Command) createClient(ctx context.Context, bucket string) (cfg internal.BucketConfig, client *s3.Client, err error) {
	cfg = internal.ConfigForBucket(bucket)
	client, err = internal.NewS3ClientFromProfile(ctx, cfg.AWSProfile, func(opts *s3.Options) {
		// without this, getting a bunch of WARN message below:
		// WARN Response has no supported checksum. Not validating response payload.
		opts.DisableLogOutputChecksumValidationSkipped = true
	})
	if err != nil {
		return cfg, nil, fmt.Errorf("create s3 client error: %w", err)
	}

	return
}
