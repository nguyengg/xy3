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

func (c *Command) Execute(args []string) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	success := 0
	n := len(c.Args.Files)
	for i, file := range c.Args.Files {
		c.logger = internal.NewLogger(i, n, file)

		if err = c.extract(ctx, string(file)); err == nil {
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

func (c *Command) extract(ctx context.Context, name string) error {
	f, err := os.Open(name)
	if err != nil {
		return fmt.Errorf(`open file "%s" error: %w`, name, err)
	}

	defer f.Close()

	stem, ext := util.StemAndExt(name)
	ex := DetectExtractorFromExt(ext)
	if ex == nil {
		c.logger.Printf(`file "%s" is not a supported archive`, filepath.Base(name))
		return nil
	}

	dir, err := util.MkExclDir(".", stem, 0755)
	if err != nil {
		return fmt.Errorf("create directory error: %w", err)
	}

	bar := internal.DefaultBytes(-1, filepath.Base(name))
	if err, _ = ex.Extract(ctx, f, dir, func(opts *Options) {
		opts.ProgressBar = bar
	}), bar.Close(); err != nil {
		_ = os.RemoveAll(dir)
	}

	return err
}
