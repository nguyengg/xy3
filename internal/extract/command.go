package extract

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/mholt/archiver/v4"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
)

type Command struct {
	Args struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local archives to be extracted" required:"yes"`
	} `positional-args:"yes"`
}

type Success struct {
	File   string
	Output string
}

type Failure struct {
	File string
	Err  error
}

func (c *Command) Execute(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unknown positional arguments: %s", strings.Join(args, " "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	// save the results so that at the end, we can reprint them.
	n := len(c.Args.Files)
	successes := make([]Success, 0, n)
	failures := make([]Failure, 0)

	for i, file := range c.Args.Files {
		log.Printf("[%d/%d %s]: start extracting", i+1, n, file)

		output, err := c.extract(ctx, string(file))
		if err != nil {
			log.Printf("[%d/%d %s]: extract error: %v", i+1, n, file, err)
			failures = append(failures, Failure{
				File: string(file),
				Err:  err,
			})

			// TODO add a flag to leave existing artifacts intact.
			if output != "" {
				_ = os.RemoveAll(output)
			}

			if errors.Is(err, context.Canceled) {
				return err
			}
		} else {
			log.Printf(`[%d/%d %s]: successfully extracted to "%s"`, i+1, n, file, output)
			successes = append(successes, Success{
				File:   string(file),
				Output: output,
			})
		}
	}

	return nil
}

// extract extracts the content of the named archive and returns the newly created directory.
func (c *Command) extract(ctx context.Context, name string) (string, error) {
	if in, err := zip.OpenReader(name); err == nil {
		defer in.Close()
		return (&ZipExtractor{Name: name, In: in}).Extract(ctx)
	}

	in, err := archiver.FileSystem(ctx, name)
	if err != nil {
		return "", err
	}
	return (&FSExtractor{Name: name, In: in}).Extract(ctx)
}

// createOutputDir creates the output directory and the function to join the output path for each file in the archive.
func createOutputDir(topLevelDir, stem string) (output string, pathFn func(string) string, err error) {
	if output = topLevelDir; output != "" {
		pathFn = func(s string) string {
			return filepath.Join("", s)
		}
	} else {
		output = stem
		pathFn = func(s string) string {
			return filepath.Join(stem, s)
		}
	}

	for i := 0; ; {
		switch err = os.Mkdir(output, 0755); {
		case err == nil:
			return
		case errors.Is(err, os.ErrExist):
			i++
			output = stem + "-" + strconv.Itoa(i)
			pathFn = func(s string) string {
				return filepath.Join(output, s)
			}
		default:
			return "", nil, err
		}
	}
}

// createExclFile creates a new exclusive file for writing and ensures all parent directories to the file exist.
//
// Caller must close the writer.
func createExclFile(name string, perm fs.FileMode) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(name), perm); err != nil {
		return nil, err
	}

	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
}

// copyWithContext is a custom implementation of io.Copy that is cancellable.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (err error) {
	buf := make([]byte, 32*1024)

	var nr, nw int
	var read int64
	for {
		nr, err = src.Read(buf)

		if nr > 0 {
			switch nw, err = dst.Write(buf[0:nr]); {
			case err != nil:
				return err
			case nr < nw:
				return io.ErrShortWrite
			case nr != nw:
				return fmt.Errorf("invalid write: expected to write %d bytes, wrote %d bytes instead", nr, nw)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				read += int64(nr)
			}
		}

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
