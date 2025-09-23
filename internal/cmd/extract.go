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
	"github.com/nguyengg/xy3/internal"
)

type Extract struct {
	Args struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local files to be extracted" required:"yes"`
	} `positional-args:"yes"`

	logger *log.Logger
}

func (c *Extract) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)
		c.logger.Printf("start decompresing")

		if _, err = internal.Decompress(ctx, string(file), "."); err == nil {
			c.logger.Printf("done decompresing")
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("decompress error: %v", err)
	}

	log.Printf("successfully decompressed %d/%d files", success, n)
	return nil
}
