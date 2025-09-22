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
	"github.com/nguyengg/xy3/util"
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

		path := string(file)
		_, ext := util.StemAndExt(path)
		f, err := os.Open(path)
		if err != nil {
			c.logger.Printf(`open file "%s" error: %v`, file, err)
			continue
		}

		bar := internal.DefaultBytes(-1, filepath.Base(path))
		if err, _, _ = Extract(ctx, f, ext, ".", func(opts *Options) {
			opts.ProgressBar = bar
		}), f.Close(), bar.Close(); err == nil {
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
