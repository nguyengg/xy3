package recompress

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/download"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) recompress(ctx context.Context, manifestName string) error {
	man, err := manifest.UnmarshalFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// we'll create a temp directory to store all intermediate artifacts.
	// this temp directory is deleted only after complete success.
	stem, ext := util.StemAndExt(man.Key)
	dir, err := os.MkdirTemp(".", stem+"-*")
	if err != nil {
		return fmt.Errorf("create temp dir error: %w", err)
	}

	success := false
	defer func() {
		// TODO change !success to success
		if !success {
			if err = os.RemoveAll(dir); err != nil {
				c.logger.Printf(`clean up "%s" error: %v`, dir, err)
			}
		}
	}()

	// this is essentially download and extract mode.
	f, err := os.CreateTemp(dir, stem+"-*"+ext)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}

	if err, _ = download.Download(ctx, c.client, man.Bucket, man.Key, f), f.Close(); err != nil {
		if errors.Is(err, download.ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			return err
		}
	}

	uncompressedDir, err := internal.Decompress(ctx, f.Name(), dir)
	if err != nil {
		return err
	}

	// now compress the extracted contents.
	f, err = os.OpenFile(filepath.Join(dir, stem+internal.DefaultAlgorithm.Ext()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("create archive error: %w", err)
	}

	if err, _ = internal.CompressDir(ctx, uncompressedDir, f), f.Close(); err != nil {
		return fmt.Errorf(`compress "%s" error: %w`, filepath.Join(dir, "tmp"), err)
	}

	// TODO upload file.
	success = true
	return nil
}
