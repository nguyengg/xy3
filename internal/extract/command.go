package extract

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
)

type Command struct {
	Args struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local archives to be extracted" required:"yes"`
	} `positional-args:"yes"`
	Mode string `short:"m" long:"short" choice:"zstd" choice:"zip" choice:"gzip" default:"zstd"`

	logger *log.Logger
}

func (c *Command) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)

		f, err := os.Open(string(file))
		if err != nil {
			c.logger.Printf(`open file "%s" error: %v`, file, err)
			continue
		}

		err = Extract(ctx, f, filepath.Base(string(file)))
		_ = f.Close()
		if err == nil {
			success++
			continue
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("extract error: %v", err)
	}

	log.Printf("successfully extracted %d/%d files", success, n)
	return nil
}
