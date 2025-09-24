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
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

type Compress struct {
	Algorithm      string `short:"a" long:"algorithm" choice:"zstd" choice:"zip" choice:"gzip" choice:"xz" default:"zstd"`
	MaxConcurrency int    `short:"P" long:"max-concurrency"`
	Args           struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the files/directories to be compressed" required:"yes"`
	} `positional-args:"yes"`

	logger *log.Logger
}

func (c *Compress) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	algorithm, err := internal.NewAlgorithmFromName(c.Algorithm)
	if err != nil {
		return fmt.Errorf("unknown algorithm: %v", c.Algorithm)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start compressing")

		// stat to determine if file or directory.
		path := string(file)
		fi, err := os.Stat(path)
		if err != nil {
			c.logger.Printf(`stat file "%s" error: %v`, path, err)
			continue
		}

		if fi.IsDir() {
			if err = c.compressDir(ctx, algorithm, path); err == nil {
				c.logger.Printf("done compressing")
				success++
				continue
			}

			if errors.Is(err, context.Canceled) {
				break
			}

			c.logger.Printf(`compress directory "%s" error: %v`, path, err)
			continue
		}

		if err = c.compressFile(ctx, algorithm, path); err == nil {
			c.logger.Printf("done compressing")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf(`compress file "%s" error: %v`, path, err)
	}

	log.Printf("successfully compressed %d/%d files", success, n)
	return nil
}

func (c *Compress) compressDir(ctx context.Context, algorithm internal.Algorithm, name string) error {
	ext := algorithm.Ext()
	if algorithm.ShouldTar() {
		ext = ".tar" + ext
	}

	dst, err := util.OpenExclFile(".", filepath.Base(name), ext, 0666)
	if err != nil {
		return fmt.Errorf("create archive error: %w", err)
	}

	if err, _ = internal.CompressDir(ctx, name, dst, func(opts *internal.CompressOptions) {
		opts.Algorithm = algorithm
		opts.MaxConcurrency = c.MaxConcurrency
	}), dst.Close(); err != nil {
		_ = os.Remove(dst.Name())
		return err
	}

	return nil
}

func (c *Compress) compressFile(ctx context.Context, algorithm internal.Algorithm, name string) error {
	src, err := os.Open(name)
	if err != nil {
		return fmt.Errorf(`open file "%s" error: %w`, name, err)
	}

	dst, err := util.OpenExclFile(".", filepath.Base(name), algorithm.Ext(), 0666)
	if err != nil {
		_ = src.Close()
		return fmt.Errorf("create archive error: %w", err)
	}

	if err, _, _ = internal.Compress(ctx, src, dst, func(opts *internal.CompressOptions) {
		opts.Algorithm = algorithm
		opts.MaxConcurrency = c.MaxConcurrency
	}), dst.Close(), src.Close(); err != nil {
		_ = os.Remove(dst.Name())
		return err
	}

	return nil
}
