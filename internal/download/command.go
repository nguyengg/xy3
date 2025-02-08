package download

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
	"github.com/nguyengg/xy3/internal"
)

type Command struct {
	StreamAndExtract bool `long:"stream-and-extract" description:"experimental: if the S3 file is a ZIP file (application/zip), this mode will attempt to decompress and extract the contents as they are downloaded"`
	MaxConcurrency   int  `short:"P" long:"max-concurrency" description:"use up to max-concurrency number of goroutines at a time for range downloads." default:"5"`
	Args             struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
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

	c.client = s3.NewFromConfig(cfg, func(options *s3.Options) {
		// without this, getting a bunch of WARN message below:
		// WARN Response has no supported checksum. Not validating response payload.
		options.DisableLogOutputChecksumValidationSkipped = true
	})

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)

		if err = c.download(ctx, string(file)); err == nil {
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
