package extract

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jessevdk/go-flags"
	"github.com/nguyengg/xy3/internal"
	"golang.org/x/time/rate"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Command struct {
	Args struct {
		Files []flags.Filename `positional-arg-name:"file" description:"the local .zip files to be extracted" required:"yes"`
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
		output, err := c.extract(ctx, string(file))
		if err != nil {
			log.Printf(`%d/%d: uncompress "%s" error: %v`, i+1, n, file, err)
			failures = append(failures, Failure{
				File: string(file),
				Err:  err,
			})
		} else {
			log.Printf(`%d/%d: successfully uncompressed "%s" to "%s"`, i+1, n, file, output)
			successes = append(successes, Success{
				File:   string(file),
				Output: output,
			})
		}
	}

	return nil
}

// extract extracts the content of the named ZIP file and returns the newly created directory.
func (c *Command) extract(ctx context.Context, name string) (output string, err error) {
	r, err := zip.OpenReader(name)
	if err != nil {
		return "", err
	}
	defer r.Close()

	// go through all the files first to see if they're all under the same root directory. if not, create a new one.
	// this is similar to what Mac does when uncompressing files.
	root := ""
findRootLoop:
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}

		switch paths := strings.SplitN(f.Name, "/", 2); {
		case len(paths) == 1:
			// no root dir so must create one.
			log.Printf("%s has no dir", f.Name)
			fallthrough
		case root != "" && root != paths[0]:
			// multiple roots, must create a common one.
			log.Printf("%s has different root from %s", f.Name, root)
			root = ""
			break findRootLoop
		default:
			root = paths[0]
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
	}

	// even if common root exists for files in the archive, a directory with the same name may exist on fs.
	// in that case, we'll use the archive's name to create a unique dir.
	stem, _ := internal.SplitStemAndExt(name)
	output = root
	if output == "" {
		output = stem
		root = stem
	} else {
		root = ""
	}
createOutputLoop:
	for i := 0; ; {
		switch err = os.Mkdir(output, 0755); {
		case err == nil:
			break createOutputLoop
		case errors.Is(err, os.ErrExist):
			i++
			output = stem + "-" + strconv.Itoa(i)
			root = output
		default:
			return "", fmt.Errorf("create directory error: %w", err)
		}
	}

	// now decompress with progress report.
	sometimes := rate.Sometimes{Interval: 5 * time.Second}
	n := len(r.File)
	for i, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}

		path := filepath.Join(root, f.Name)
		if err = os.MkdirAll(filepath.Dir(path), f.Mode()); err != nil {
			_ = os.Remove(output)
			return "", err
		}

		w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			_ = os.Remove(output)
			return "", err
		}

		if err = c.copy(ctx, i+1, n, w, f); err != nil {
			_ = os.Remove(output)
			return "", err
		}

		select {
		case <-ctx.Done():
			_ = os.Remove(output)
			return "", ctx.Err()
		default:
			sometimes.Do(func() {
				log.Printf(`[%d/%d] done uncompressing "%s"`, i+1, n, f.Name)
			})
		}
	}

	return output, nil
}

// copy is an implementation of io.Copy that is cancellable and also provides progress report.
func (c *Command) copy(ctx context.Context, i, n int, w io.Writer, f *zip.File) (err error) {
	sometimes := rate.Sometimes{Interval: 5 * time.Second}
	sometimes.Do(func() {})

	r, err := f.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	buf := make([]byte, 32*1024)

	var nr, nw int
	var written int64
	for {
		nr, err = r.Read(buf)

		if nr > 0 {
			switch nw, err = w.Write(buf[0:nr]); {
			case err != nil:
				return err
			case nr < nw:
				return io.ErrShortWrite
			case nr != nw:
				return fmt.Errorf("invalid write: expected to write %d bytes, wrote %d bytes instead", nr, nw)
			}

			written += int64(nw)

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				sometimes.Do(func() {
					log.Printf(`[%d/%d] uncompressed %.2f%% of "%s" (%s) so far`, i, n, float64(written)/float64(f.CompressedSize64)*100.0, f.Name, humanize.Bytes(f.CompressedSize64))
				})
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
