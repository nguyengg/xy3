package compress

import (
	"context"
	"errors"
	"fmt"
	"io"
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
		Files []flags.Filename `positional-arg-name:"file" description:"the files/directories to be compressed" required:"yes"`
	} `positional-args:"yes"`
	Mode string `short:"m" long:"short" choice:"zstd" choice:"zip" choice:"gzip" choice:"xz" default:"zstd"`

	logger *log.Logger
}

func (c *Command) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	var mode Mode
	switch c.Mode {
	case "zstd":
		mode = ZSTD
	case "zip":
		mode = ZIP
	case "gzip":
		mode = GZIP
	case "xz":
		mode = XZ
	default:
		return fmt.Errorf("unknown mode: %v", mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)

		path := string(file)
		basename := filepath.Base(path)

		dst, err := util.OpenExclFile(".", basename, mode.Ext(), 066)
		if err != nil {
			return fmt.Errorf("create archive error: %w", err)
		}

		bar := internal.DefaultBytes(-1, basename)
		if err, _, _ = Compress(ctx, path, io.MultiWriter(dst, bar), func(opts *Options) {
			opts.Mode = mode
		}), dst.Close(), bar.Close(); err == nil {
			success++
			continue
		}

		if err := os.Remove(dst.Name()); err != nil {
			c.logger.Printf(`clean up "%s" error: %v`, dst.Name(), err)
		}

		if errors.Is(err, context.Canceled) {
			break
		}

		c.logger.Printf("compress error: %v", err)
	}

	log.Printf("successfully compressed %d/%d files", success, n)
	return nil
}
