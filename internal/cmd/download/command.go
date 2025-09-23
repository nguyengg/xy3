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
	"github.com/nguyengg/xy3/internal/cmd/awsconfig"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

type Command struct {
	Extract        bool `long:"extract" description:"if specified, the downloaded file will automatically be decompressed and extracted if it's an archive'"`
	MaxConcurrency int  `short:"P" long:"max-concurrency" description:"use up to max-concurrency number of goroutines at a time for range downloads."`
	Args           struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
	} `positional-args:"yes"`

	awsconfig.ConfigLoaderMixin
	client *s3.Client
	logger *log.Logger
}

func (c *Command) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	if c.MaxConcurrency < 0 {
		return fmt.Errorf("max-concurrency must be non-negative")
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
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start downloading")

		if err = c.download(ctx, string(file)); err == nil {
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

func (c *Command) download(ctx context.Context, manifestName string) error {
	man, err := manifest.UnmarshalFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := util.StemAndExt(man.Key)
	f, err := util.OpenExclFile(".", stem, ext, 0666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	name := f.Name()

	if err, _ = internal.Download(ctx, c.client, man.Bucket, man.Key, f), f.Close(); err != nil {
		if errors.Is(err, internal.ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			_ = os.Remove(name)
			return err
		}
	}

	if c.Extract {
		if _, err = internal.Decompress(ctx, name, "."); err == nil {
			_ = os.Remove(name)
		}
	}

	return err
}
