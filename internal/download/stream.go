package download

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/namedhash"
	"github.com/nguyengg/xy3/s3reader"
	"github.com/nguyengg/xy3/zipper"
	"github.com/schollz/progressbar/v3"
)

func (c *Command) canStream(ctx context.Context, man manifest.Manifest) (headers []zipper.CDFileHeader, uncompressedSize uint64, rootDir internal.RootDir, err error) {
	r, err := s3reader.New(ctx, c.client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	})
	if err != nil {
		return nil, 0, "", err
	}
	defer r.Close()

	cd, err := zipper.NewCDScanner(r, r.Size())
	if err != nil {
		return nil, 0, "", err
	}

	// while going through the headers to compute uncompressed size, we'll also calculate if there's a common root.
	n := cd.RecordCount()
	bar := parseHeadersProgressBar(n)
	rootFinder := internal.NewZipRootDirFinder()
	headers = make([]zipper.CDFileHeader, 0, n)
	for fh := range cd.All() {
		_ = bar.Add(1)
		rootDir, _ = rootFinder(fh.Name)
		headers = append(headers, fh)
		uncompressedSize += fh.UncompressedSize64
	}

	if _, err = bar.Close(), cd.Err(); err != nil {
		return headers, uncompressedSize, rootDir, err
	}

	return
}

func (c *Command) stream(ctx context.Context, man manifest.Manifest) (bool, error) {
	// check for streaming eligibility by finding the ZIP headers.
	headers, uncompressedSize, rootDir, err := c.canStream(ctx, man)
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

	// while downloading, also computes checksum to verify against the downloaded content.
	h, err := namedhash.NewFromChecksumString(man.Checksum)
	if err != nil {
		return true, err
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

	// we'll use a pipe here.
	// one goroutine will be responsible for downloading to PipeWriter.
	// the main goroutine then reads from PipeReader to extract files using krolaw/zipstream. if there is error with
	// reading/extracting the files, cancel the child context so that all goroutines can gracefully stop.
	ctx, cancel := context.WithCancelCause(ctx)
	pr, pw := io.Pipe()

	go func() {
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

	var (
		buf = make([]byte, 32*1024)
		fh  *zip.FileHeader
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
		path := rootDir.Join(dir, name)

		fi := fh.FileInfo()
		if fi.IsDir() {
			if err = os.MkdirAll(path, fi.Mode()); err != nil {
				err = fmt.Errorf("create dir error: %w", err)
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			err = fmt.Errorf("create path to file error: %w", err)
			break
		}

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, fi.Mode())
		if err != nil {
			err = fmt.Errorf("create file error: %w", err)
			break
		}

		_, err = xy3.CopyBufferWithContext(ctx, io.MultiWriter(f, bar), zr, buf)
		_ = f.Close()
		if err != nil {
			err = fmt.Errorf("write to file error: %w", err)
			break
		}
	}

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			cancel(err)
		}

		return true, err
	}

	success = true
	return true, nil
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
