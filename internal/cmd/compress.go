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
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/codec"
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
	fi, err := os.Stat(name)
	if err != nil {
		return fmt.Errorf(`stat "%s" error: %w`, name, err)
	}

	comp, _ := codec.NewCompressorFromAlgorithm(c.Algorithm)
	ext := comp.Ext(fi.IsDir())

	dst, err := util.OpenExclFile(".", filepath.Base(name), ext, 0666)
	if err != nil {
		return fmt.Errorf("create archive error: %w", err)
	}

	if err, _ = xy3.Compress(ctx, name, dst, func(opts *xy3.CompressOptions) {
		opts.Algorithm = c.Algorithm
		if c.MaxConcurrency > 0 {
			opts.MaxConcurrency = c.MaxConcurrency
		}
	}), dst.Close(); err != nil {
		_ = os.Remove(dst.Name())
		return err
	}

	return nil
}
