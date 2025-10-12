package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
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
			logger.Printf("done decompresing")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		logger.Printf("decompress error: %v", err)
		failures = append(failures, fmt.Errorf(`decompress "%s" error: %v`, file, err))
	}

	log.Printf("successfully decompressed %d/%d files", success, n)
	if len(failures) != 0 {
		for _, err = range failures {
			log.Print(err)
		}
	}
	return nil
}
