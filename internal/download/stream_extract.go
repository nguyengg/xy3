package download

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/nguyengg/xy3/s3readseeker"
	"github.com/nguyengg/xy3/zipper"
)

func (c *Command) streamAndExtract(ctx context.Context, man manifest.Manifest) (bool, error) {
	// check first if we're eligible for stream and extract mode.
	headers, err := zipper.ExtractZipHeadersFromS3(ctx, c.client, man.Bucket, man.Key, func(options *s3readseeker.Options) {
		options.ModifyGetObjectInput = func(input *s3.GetObjectInput) *s3.GetObjectInput {
			input.ExpectedBucketOwner = man.ExpectedBucketOwner
			return input
		}
		options.ModifyHeadObjectInput = func(input *s3.HeadObjectInput) *s3.HeadObjectInput {
			input.ExpectedBucketOwner = man.ExpectedBucketOwner
			return input
		}
	})
	if err != nil {
		if errors.Is(err, zipper.ErrNoEOCDFound) {
			log.Printf("did not find EOCD, will download file normally")
			return false, nil
		}

		return false, err
	}

	var size int64
	trimRoot := func(path string) string {
		return path
	}
	{
		names := make([]string, len(headers))
		for i, fh := range headers {
			names[i] = fh.Name
			size += int64(fh.UncompressedSize64)
		}
		if root, err := zipper.FindRoot(ctx, names); err != nil {
			return true, err
		} else if root != "" {
			trimRoot = func(path string) string {
				return strings.TrimLeft(strings.TrimPrefix(path, root), `\/`)
			}
		}
	}

	// while downloading, also computes checksum to verify against the downloaded content.
	h, err := namedhash.NewFromChecksumString(man.Checksum)
	if err != nil {
		return true, err
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

	// we'll use a pipe here.
	// one goroutine will be responsible for downloading to PipeWriter.
	// the main goroutine then reads from PipeReader to extract files using krolaw/zipstream. if there is error with
	// reading/extracting the files, cancel the child context so that all goroutines can gracefully stop.
	//
	// why don't we use s3reader.ReadSeeker here? turns out zipstream.NewReader would only download the small
	// parts sequentially, while xy3.Download can download the parts in parallel.
	ctx, cancel := context.WithCancelCause(ctx)
	pr, pw := io.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer pw.Close()

		if err := xy3.Download(ctx, c.client, man.Bucket, man.Key, io.MultiWriter(pw, h), func(options *xy3.DownloadOptions) {
			options.Concurrency = c.MaxConcurrency
			options.ModifyHeadObjectInput = func(input *s3.HeadObjectInput) {
				v := man.ExpectedBucketOwner
				if v == nil {
					v = c.ExpectedBucketOwner
				}
				input.ExpectedBucketOwner = v
			}
			options.ModifyGetObjectInput = func(input *s3.GetObjectInput) {
				v := man.ExpectedBucketOwner
				if v == nil {
					v = c.ExpectedBucketOwner
				}
				input.ExpectedBucketOwner = v
			}

			options.PostGetPart = nil
		}); err != nil && !errors.Is(err, context.Canceled) {
			cancel(fmt.Errorf("download error: %w", err))
		}
	}()

	// for progress report, we'll use the uncompressed bytes.
	bar := internal.DefaultBytes(size, fmt.Sprintf("extracting %d files", len(headers)))
	defer bar.Close()

	var (
		buf = make([]byte, 32*1024)
		fh  *zip.FileHeader
		f   *os.File
	)
	for zr := zipstream.NewReader(pr); err == nil; {
		fh, err = zr.Next()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			err = fmt.Errorf("stream zip error: %w", err)
			break
		}

		name := fh.Name
		path := filepath.Join(dir, trimRoot(name))

		fi := fh.FileInfo()
		if fi.IsDir() {
			err = os.MkdirAll(path, fi.Mode())
			continue
		}

		if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
			f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, fi.Mode())
		}
		if err != nil {
			err = fmt.Errorf(`create file "%s" error: %w`, path, err)
			break
		}

		_, err = xy3.CopyBufferWithContext(ctx, io.MultiWriter(f, bar), zr, buf)
		_ = f.Close()
		if err != nil {
			err = fmt.Errorf(`write to file "%s" error: %w`, path, err)
			break
		}
	}

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			cancel(err)
		}

		return true, err
	}

	wg.Wait()
	success = true
	return true, nil
}
