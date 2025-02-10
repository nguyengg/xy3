package download

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-s3readseeker"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/zipper"
	"github.com/schollz/progressbar/v3"
)

func (c *Command) streamAndExtract(ctx context.Context, man manifest.Manifest) (bool, error) {
	// check first if we're eligible for stream and extract mode.
	cd, err := zipper.NewCDScannerFromS3(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	})
	if err != nil {
		if errors.Is(err, zipper.ErrNoEOCDFound) {
			log.Printf("did not find EOCD, will download file normally")
			return false, nil
		}

		return false, err
	}

	// while going through the cd to compute uncompressed size, we'll also calculate if there's a common root.
	// we'll use the headers slice later to do parallel download and extract so save them.
	var uncompressedSize uint64
	headers := make([]zipper.CDFileHeader, 0)
	trimRoot := func(path string) string {
		return path
	}

	if names := make([]string, 0, cd.RecordCount()); true {
		bar := parseHeadersProgressBar(cd.RecordCount())

		for fh := range cd.All() {
			_ = bar.Add(1)
			headers = append(headers, fh)
			names = append(names, fh.Name)
			uncompressedSize += fh.UncompressedSize64
		}

		if _, err = bar.Close(), cd.Err(); err != nil {
			return true, err
		}

		if root, err := zipper.FindRoot(ctx, names); err != nil {
			return true, err
		} else if root != "" {
			trimRoot = func(path string) string {
				return strings.TrimLeft(strings.TrimPrefix(path, root), `\/`)
			}
		}
	}

	// attempt to create the local directory that will store the extracted files.
	// if we fail to download the file complete, clean up by deleting the directory.
	stem, _ := xy3.StemAndExt(man.Key)
	dir, err := xy3.MkExclDir(".", stem)
	if err != nil {
		return true, fmt.Errorf("create output directory error: %w", err)
	}

	success := false
	defer func(name string) {
		if !success {
			c.logger.Printf(`deleting output directory "%s"`, name)
			if err = os.RemoveAll(name); err != nil {
				c.logger.Printf("delete output directory error: %v", err)
			}
		}
	}(dir)

	c.logger.Printf(`extracting from "s3://%s/%s" to "%s"`, man.Bucket, man.Key, dir)

	// for progress report, we'll use the uncompressed bytes.
	bar := internal.DefaultBytes(int64(uncompressedSize), fmt.Sprintf("extracting %d files", cd.RecordCount()))
	defer bar.Close()

	// use a goroutine pool sharing this ReaderAt.
	var r io.ReaderAt
	r, err = s3readseeker.New(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	})
	if err != nil {
		return true, err
	}

	var wg sync.WaitGroup
	wg.Add(c.MaxConcurrency)
	ch := make(chan zipper.CDFileHeader, c.MaxConcurrency)
	closeCh := sync.OnceFunc(func() {
		close(ch)
	})
	defer closeCh()

	ctx, cancel := context.WithCancelCause(ctx)
	for range c.MaxConcurrency {
		go func() {
			defer wg.Done()

			var (
				f *os.File
			)

			for fh := range ch {
				size := int64(fh.CompressedSize64)
				if size == 0 {
					continue
				}

				offset := int64(fh.Offset)
				var decompressor = io.NopCloser
				if fh.Method == zip.Deflate {
					decompressor = flate.NewReader
				}

				// https://en.wikipedia.org/wiki/ZIP_(file_format)#Local_file_header
				data := make([]byte, 30)
				if _, err = r.ReadAt(data, offset); err != nil {
					cancel(err)
					break
				}

				n := int(data[26]) | int(data[27])<<8
				m := int(data[28]) | int(data[29])<<8

				name := fh.Name
				path := filepath.Join(dir, trimRoot(name))

				if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
					f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
				}
				if err != nil {
					cancel(fmt.Errorf("create local file error: %w", err))
					break
				}

				_, err = xy3.CopyBufferWithContext(
					ctx,
					io.MultiWriter(f, bar),
					decompressor(io.NewSectionReader(r, offset+int64(30+n+m), size)),
					nil)
				_ = f.Close()
				if err != nil {
					cancel(fmt.Errorf("write to file error: %w", err))
					break
				}
			}
		}()
	}

	for _, fh := range headers {
		select {
		case ch <- fh:
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}

	closeCh()
	wg.Wait()
	success = true
	return true, ctx.Err()
}

func parseHeadersProgressBar(n int) *progressbar.ProgressBar {
	return progressbar.NewOptions(
		n,
		progressbar.OptionSetDescription(fmt.Sprintf("scanning %d headers", n)),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(1*time.Second),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			_, _ = fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true))
}
