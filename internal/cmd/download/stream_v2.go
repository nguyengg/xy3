package download

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/zipper"
)

func (c *Command) streamV2(ctx context.Context, man internal.Manifest) (bool, error) {
	logger := internal.MustLogger(ctx)

	cfg, client, err := c.createClient(ctx, man.Bucket)
	if err != nil {
		return false, err
	}
	expectedBucketOwner := man.ExpectedBucketOwner
	if expectedBucketOwner == nil {
		expectedBucketOwner = cfg.ExpectedBucketOwner
	}

	// check for streaming eligibility by finding the ZIP headers.
	headers, uncompressedSize, rootDir, err := c.canStream(ctx, man)
	if err != nil {
		if errors.Is(err, zipper.ErrNoEOCDFound) {
			return false, nil
		}

		return false, err
	}

	// attempt to create the local directory that will store the extracted files.
	// if we fail to download the file complete, clean up by deleting the directory.
	stem, _ := commons.StemExt(man.Key)
	dir, err := commons.MkExclDir(".", stem, 0755)
	if err != nil {
		return true, fmt.Errorf("create output directory error: %w", err)
	}

	logger.Printf(`extracting to "%s"`, dir)

	// TODO figure out how to compute checksum while downloading.
	// it might be impossible due to this algorithm not streaming full file.

	success := false
	defer func(name string) {
		if !success {
			logger.Printf(`deleting output directory "%s"`, name)
			if err = os.RemoveAll(name); err != nil {
				logger.Printf("delete output directory error: %v", err)
			}
		}
	}(dir)

	// the difference between this and V1 is that this (V2) may not end up downloading the entire file.
	// instead, we'll spin up a number of goroutines, each working on a file in the archive independently of each
	// other, using a shared cancellable-with-cause context so that if one goroutine fails, all others can fail
	// gracefully and early.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	r, err := s3reader.New(ctx, client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: expectedBucketOwner,
	})
	if err != nil {
		return true, err
	}
	defer r.Close()

	inputs := make(chan zipper.CDFileHeader, len(headers))

	bar := tspb.DefaultBytes(int64(uncompressedSize), fmt.Sprintf("extracting %d files", len(headers)))
	defer bar.Close()

	var wg sync.WaitGroup
	for range runtime.NumCPU() {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for fh := range inputs {
				name := fh.Name
				path := rootDir.Join(dir, name)

				fi := fh.FileInfo()
				if fi.IsDir() {
					if err = os.MkdirAll(path, fi.Mode()); err != nil {
						cancel(fmt.Errorf("create dir error: %w", err))
						return
					}

					continue
				}

				var decompressor = io.NopCloser
				if fh.Method == zip.Deflate {
					decompressor = flate.NewReader
				}

				// https://en.wikipedia.org/wiki/ZIP_(file_format)#Local_file_header
				data := make([]byte, 30)
				offset := int64(fh.Offset)
				if _, err = r.ReadAt(data, offset); err != nil {
					cancel(fmt.Errorf("read local file header error: %w", err))
					return
				}

				n := int(data[26]) | int(data[27])<<8
				m := int(data[28]) | int(data[29])<<8
				dst := decompressor(io.NewSectionReader(r, offset+int64(30+n+m), int64(fh.CompressedSize64)))

				if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					cancel(fmt.Errorf("create path to file error: %w", err))
					return
				}

				f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, fi.Mode())
				if err != nil {
					cancel(fmt.Errorf("create file error: %w", err))
					return
				}

				_, err = commons.CopyBufferWithContext(ctx, io.MultiWriter(f, bar), dst, nil)
				_ = f.Close()
				if err != nil {
					cancel(fmt.Errorf("write to file error: %w", err))
					return
				}
			}
		}()
	}

	for _, fh := range headers {
		inputs <- fh
	}
	close(inputs)

	// wait in a separate goroutine so that we can catch context cancel.
	done := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		_ = bar.Finish()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-done:
		if err = ctx.Err(); err == nil {
			success = true
		}

		return true, err
	}
}
