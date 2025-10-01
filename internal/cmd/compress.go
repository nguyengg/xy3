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

	"github.com/jessevdk/go-flags"
	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/codec"
	"github.com/nguyengg/xy3/internal"
)

type Compress struct {
	Algorithm      string `short:"a" long:"algorithm" choice:"zstd" choice:"zip" choice:"gzip" choice:"xz" default:"zstd"`
	Delete         bool   `long:"delete" description:"if specified, delete the original files or directories that were successfully compressed and uploaded."`
	MaxConcurrency int    `short:"P" long:"max-concurrency"`
	Args           struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the files/directories to be compressed" required:"yes"`
	} `positional-args:"yes"`

	logger *log.Logger
}

func (c *Compress) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start compressing")

		if err = c.compress(ctx, string(file)); err == nil {
			c.logger.Printf("done compressing")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf(`compress "%s" error: %v`, file, err)
	}

	log.Printf("successfully compressed %d/%d files", success, n)
	return nil
}

func (c *Compress) compress(ctx context.Context, name string) error {
	comp := xy3.NewCompressorFromName(c.Algorithm)
	ext := comp.ArchiveExt()
	success := false

	switch fi, err := os.Stat(name); {
	case err != nil:
		return fmt.Errorf(`stat file "%s" error: %w`, name, err)

	case fi.IsDir():
		dst, err := commons.OpenExclFile(".", filepath.Base(name), ext, 0666)
		if err != nil {
			return fmt.Errorf("create archive error: %w", err)
		}
		defer func() {
			_ = dst.Close()

			if success && c.Delete {
				if err = os.RemoveAll(name); err != nil {
					c.logger.Printf(`delete directory "%s" error: %v`, name, err)
				}
			}
		}()

		if err = xy3.CompressDir(ctx, name, dst, func(opts *xy3.CompressOptions) {
			opts.Algorithm = c.Algorithm
			if c.MaxConcurrency > 0 {
				opts.MaxConcurrency = c.MaxConcurrency
			}
		}); err != nil {
			_, _ = dst.Close(), os.Remove(dst.Name())
			return fmt.Errorf(`compress directory "%s" error: %w`, name, err)
		}

		if err = dst.Close(); err != nil {
			_ = os.Remove(dst.Name())
			return fmt.Errorf(`complete compressing directory "%s" error: %w`, name, err)
		}

	default:
		// if the compressor implements codec.Codec then use that extension since this is a single file.
		if cd, ok := comp.(codec.Codec); ok {
			ext = cd.Ext()
		}

		dst, err := commons.OpenExclFile(".", filepath.Base(name), ext, 0666)
		if err != nil {
			return fmt.Errorf("create output file error: %w", err)
		}
		defer func() {
			_ = dst.Close()

			if success && c.Delete {
				if err = os.Remove(name); err != nil {
					c.logger.Printf(`delete file "%s" error: %v`, name, err)
				}
			}
		}()

		src, err := os.Open(name)
		if err != nil {
			_, _ = dst.Close(), os.Remove(dst.Name())
			return fmt.Errorf(`open file "%s" error: %w`, name, err)
		}
		defer src.Close()

		fi, _ = src.Stat()

		if err = xy3.Compress(ctx, src, fi, dst, func(opts *xy3.CompressOptions) {
			opts.Algorithm = c.Algorithm
			if c.MaxConcurrency > 0 {
				opts.MaxConcurrency = c.MaxConcurrency
			}
		}); err != nil {
			_, _ = dst.Close(), os.Remove(dst.Name())
			return fmt.Errorf(`compress file "%s" error: %w`, name, err)
		}

		if err = dst.Close(); err != nil {
			_ = os.Remove(dst.Name())
			return fmt.Errorf(`complete compressing file "%s" error: %w`, name, err)
		}
	}

	success = true
	return nil
}
