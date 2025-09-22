package download

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/extract"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

type Command struct {
	Extract             bool    `long:"extract" description:"if specified, the downloaded file will automatically be decompressed and extracted if it's an archive'"`
	ExpectedBucketOwner *string `long:"expected-bucket-owner" description:"optional ExpectedBucketOwner field to apply when the manifest does not have its own expectedBucketOwner"`
	MaxConcurrency      int     `short:"P" long:"max-concurrency" description:"use up to max-concurrency number of goroutines at a time for range downloads." default:"5"`
	Args                struct {
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

	if err, _ = Download(ctx, c.client, man.Bucket, man.Key, f), f.Close(); err != nil {
		if errors.Is(err, ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			_ = os.Remove(name)
			return err
		}
	}

	if c.Extract {
		if err = c.extract(ctx, name); err != nil {
			return fmt.Errorf("extract error: %w", err)
		}

		_ = os.Remove(name)
	}

	return nil
}

func (c *Command) extract(ctx context.Context, name string) error {
	f, err := os.Open(name)
	if err != nil {
		return fmt.Errorf(`open file "%s" error: %w`, name, err)
	}

	_, ext := util.StemAndExt(name)
	bar := internal.DefaultBytes(-1, fmt.Sprintf(`extracting "%s"`, filepath.Base(name)))
	if err, _, _ := extract.Extract(ctx, f, ext, ".", func(opts *extract.Options) {
		opts.ProgressBar = bar
	}), f.Close(), bar.Close(); err != nil {
		if errors.Is(err, extract.ErrUnknownArchiveExtension) {
			c.logger.Printf("file is not eligible for auto-extracting: %v", err)
			return nil
		}

		return err
	}

	return nil
}
