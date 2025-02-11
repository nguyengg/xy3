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
	"github.com/nguyengg/go-s3readseeker"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/executor"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/zipper"
)

func (c *Command) streamV2(ctx context.Context, man manifest.Manifest) (bool, error) {
	// check for streaming eligibility by finding the ZIP headers.
	headers, uncompressedSize, trimRoot, err := c.canStream(ctx, man)
	if err != nil {
		if errors.Is(err, zipper.ErrNoEOCDFound) {
			return false, nil
		}

		return false, err
	}

	// for progress report, we'll use the uncompressed bytes.
	bar := internal.DefaultBytes(int64(uncompressedSize), fmt.Sprintf("extracting %d files", len(headers)))
	defer bar.Close()

	// attempt to create the local directory that will store the extracted files.
	// if we fail to download the file complete, clean up by deleting the directory.
	stem, _ := xy3.StemAndExt(man.Key)
	dir, err := xy3.MkExclDir(".", stem)
	if err != nil {
		return true, fmt.Errorf("create output directory error: %w", err)
	}
	c.logger.Printf("extracting to %s", dir)

	// TODO figure out how to compute checksum while downloading.
	// it might be impossible due to this algorithm not streaming full file.

	success := false
	defer func(name string) {
		if !success {
			c.logger.Printf(`deleting output directory "%s"`, name)
			if err = os.RemoveAll(name); err != nil {
				c.logger.Printf("delete output directory error: %v", err)
			}
		}
	}(dir)

	// the difference between this and V1 is that this (V2) may not end up downloading the entire file.
	// instead, we'll spin up a number of goroutines matching the number of processors, each working on a file in
	// the archive independently of each other.
	// a shared cancellable-with-cause context is used so that if one goroutine fails, all others can fail
	// gracefully and early.
	ctx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	wg.Add(len(headers))

	s3reader, err := s3readseeker.New(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	})
	if err != nil {
		return true, err
	}
	defer s3reader.Close()

	ex := executor.NewCallerRunOnRejectExecutor(runtime.NumCPU())
	defer ex.Close()

	for _, fh := range headers {
		ex.Execute(func() {
			defer wg.Done()

			name := fh.Name
			path := filepath.Join(dir, trimRoot(name))

			fi := fh.FileInfo()
			if fi.IsDir() {
				err = os.MkdirAll(path, fi.Mode())
				return
			}

			var decompressor = io.NopCloser
			if fh.Method == zip.Deflate {
				decompressor = flate.NewReader
			}

			// https://en.wikipedia.org/wiki/ZIP_(file_format)#Local_file_header
			data := make([]byte, 30)
			offset := int64(fh.Offset)
			if _, err = s3reader.ReadAt(data, offset); err != nil {
				cancel(err)
				return
			}

			n := int(data[26]) | int(data[27])<<8
			m := int(data[28]) | int(data[29])<<8
			dst := decompressor(io.NewSectionReader(s3reader, offset+int64(30+n+m), int64(fh.CompressedSize64)))

			if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				cancel(fmt.Errorf("create path to file error: %w", err))
				return
			}

			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, fi.Mode())
			if err != nil {
				cancel(fmt.Errorf("create file error: %w", err))
				return
			}

			_, err = xy3.CopyBufferWithContext(ctx, io.MultiWriter(f, bar), dst, nil)
			_ = f.Close()
			if err != nil {
				cancel(fmt.Errorf("write to file error: %w", err))
				return
			}
		})
	}

	// wait in a separate goroutine so that we can catch context cancel.
	done := make(chan struct{}, 1)
	go func() {
		wg.Wait()
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
