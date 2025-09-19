package compress

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/util"
)

type Command struct {
	Args struct {
		File flags.Filename `positional-arg-name:"file" description:"the file or directory to be compressed" required:"yes"`
	} `positional-args:"yes"`
	Mode string `short:"m" long:"short" choice:"zstd" choice:"zip" choice:"gzip" default:"zstd"`
}

func (c *Command) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	var (
		dst  *os.File
		mode Mode
		path = string(c.Args.File)
	)
	switch c.Mode {
	case "zstd":
		if dst, err = util.OpenExclFile(".", filepath.Base(path), ".tar.zst", 0666); err != nil {
			return fmt.Errorf("create .tar.zst file error: %w", err)
		}

		mode = ZSTD
	case "zip":
		if dst, err = util.OpenExclFile(".", filepath.Base(path), ".zip", 0666); err != nil {
			return fmt.Errorf("create zip file error: %w", err)
		}

		mode = ZIP
	case "gzip":
		if dst, err = util.OpenExclFile(".", filepath.Base(path), ".tar.gz", 0666); err != nil {
			return fmt.Errorf("create .tar.gz file error: %w", err)
		}

		mode = GZIP
	default:
		return fmt.Errorf("unknown mode: %v", mode)
	}
	defer dst.Close()

	if err = CompressDir(ctx, path, dst, func(opts *Options) {
		opts.Mode = mode
	}); err != nil {
		return fmt.Errorf("compress error: %w", err)
	}

	return nil
}
