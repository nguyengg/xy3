package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jessevdk/go-flags"

	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
)

type Extract struct {
	DecompressOnly bool `long:"decompress-only" description:"if specified, the compressed archives will only be decompressed without extracting"`
	Args           struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files to be extracted" required:"yes"`
	} `positional-args:"yes"`
}

func (c *Extract) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	// SIGKILL cannot be caught by a Go handler; register SIGINT and SIGTERM instead.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	success := 0
	failures := make([]error, 0)
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		ctx := internal.WithPrefixLogger(ctx, internal.Prefix(i+1, n, file))
		logger := internal.MustLogger(ctx)
		logger.Printf("start decompressing")

		if _, err = xy3.Decompress(ctx, string(file), ".", func(opts *xy3.DecompressOptions) {
			opts.NoExtract = c.DecompressOnly
		}); err == nil {
			logger.Printf("done decompressing")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		logger.Printf("decompress error: %v", err)
		failures = append(failures, fmt.Errorf(`decompress "%s" error: %w`, file, err))
	}

	log.Printf("successfully decompressed %d/%d files", success, n)

	// surface interrupt so the process exits non-zero on Ctrl-C mid-batch.
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}

	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}
