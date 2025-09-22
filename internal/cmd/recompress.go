package cmd

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
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

type Recompress struct {
	Args struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files each containing a single S3 URI" required:"yes"`
	} `positional-args:"yes"`

	client *s3.Client
	logger *log.Logger
}

func (c *Recompress) Execute(args []string) error {
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

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)

		if err = c.recompress(ctx, string(file)); err == nil {
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("recompress error: %v", err)
	}

	log.Printf("successfully recompressed %d/%d files", success, n)
	return nil
}

func (c *Recompress) recompress(ctx context.Context, manifestName string) error {
	man, err := manifest.UnmarshalFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// we'll create a temp directory to store all intermediate artifacts.
	// this temp directory is deleted only after complete success.
	stem, ext := util.StemAndExt(man.Key)
	dir, err := os.MkdirTemp(".", stem+"-*")
	if err != nil {
		return fmt.Errorf("create temp dir error: %w", err)
	}

	success := false
	defer func() {
		// TODO change !success to success
		if !success {
			if err = os.RemoveAll(dir); err != nil {
				c.logger.Printf(`clean up "%s" error: %v`, dir, err)
			}
		}
	}()

	// this is essentially download and extract mode.
	f, err := os.CreateTemp(dir, stem+"-*"+ext)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}

	if err, _ = internal.Download(ctx, c.client, man.Bucket, man.Key, f), f.Close(); err != nil {
		if errors.Is(err, internal.ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			return err
		}
	}

	uncompressedDir, err := internal.Decompress(ctx, f.Name(), dir)
	if err != nil {
		return err
	}

	// now compress the extracted contents.
	f, err = os.OpenFile(filepath.Join(dir, stem+internal.DefaultAlgorithm.Ext()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("create archive error: %w", err)
	}

	if err, _ = internal.CompressDir(ctx, uncompressedDir, f), f.Close(); err != nil {
		return fmt.Errorf(`compress "%s" error: %w`, filepath.Join(dir, "tmp"), err)
	}

	// TODO upload file.
	success = true
	return nil
}
