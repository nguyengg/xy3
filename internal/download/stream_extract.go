package download

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/schollz/progressbar/v3"
)

func (c *Command) streamAndExtract(ctx context.Context, man manifest.Manifest) (bool, error) {
	// check first if we're eligible for stream and extract mode.
	if headObjectOutput, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:              &man.Bucket,
		Key:                 &man.Key,
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}); err != nil || "application/zip" != aws.ToString(headObjectOutput.ContentType) {
		return false, err
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
				input.ExpectedBucketOwner = man.ExpectedBucketOwner
			}
			options.ModifyGetObjectInput = func(input *s3.GetObjectInput) {
				input.ExpectedBucketOwner = man.ExpectedBucketOwner
			}

			var bar *progressbar.ProgressBar
			var completedPartCount int
			options.PostGetPart = func(data []byte, size int64, partNumber, partCount int) {
				if bar == nil {
					bar = internal.DefaultBytes(size, "stream and extract")
				}
				if completedPartCount++; completedPartCount == partCount {
					_ = bar.Close()
				} else {
					_ = bar.Add64(int64(len(data)))
				}
			}
		}); err != nil && !errors.Is(err, context.Canceled) {
			cancel(fmt.Errorf("download error: %w", err))
		}
	}()

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
		path := filepath.Join(dir, name)

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

		err = xy3.CopyBufferWithContext(ctx, f, zr, buf)
		_ = f.Close()
		if err != nil {
			err = fmt.Errorf(`write to file "%s" error: %w`, path, err)
			break
		}

		// TODO how to report file writing progress?
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
