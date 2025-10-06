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
	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/zipper"
	"github.com/schollz/progressbar/v3"
)

func (c *Command) canStream(ctx context.Context, man internal.Manifest) (headers []zipper.CDFileHeader, uncompressedSize uint64, rootDir internal.RootDir, err error) {
	cfg, client, err := c.createClient(ctx, man.Bucket)
	if err != nil {
		return headers, uncompressedSize, rootDir, err
	}
	expectedBucketOwner := man.ExpectedBucketOwner
	if expectedBucketOwner == nil {
		expectedBucketOwner = cfg.ExpectedBucketOwner
	}

	r, err := s3reader.New(ctx, client, &s3.GetObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: expectedBucketOwner,
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

func (c *Command) stream(ctx context.Context, man internal.Manifest) (bool, error) {
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

	verifier, _ := sri.NewVerifier(man.Checksum)

	// attempt to create the local directory that will store the extracted files.
	// if we fail to download the file complete, clean up by deleting the directory.
	stem, _ := commons.StemExt(man.Key)
	dir, err := commons.MkExclDir(".", stem, 0755)
	if err != nil {
		return true, fmt.Errorf("create output directory error: %w", err)
	}

	c.logger.Printf(`extracting to "%s"`, dir)

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
	//
	// why can't zipstream.NewReader reads directly from s3reader? it can, but it is significantly slower for some
	// reasons. probably because zipstream.NewReader reads slowly, while using a pipe allows downloading to go as
	// fast as the pipe can support. the current implementation hits ~7-8MB/s, while streaming directly from
	// s3reader dips to ~3-4MB/s.
	ctx, cancel := context.WithCancelCause(ctx)
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		r, err := s3reader.New(ctx, client, &s3.GetObjectInput{
			Bucket:              aws.String(man.Bucket),
			Key:                 aws.String(man.Key),
			ExpectedBucketOwner: expectedBucketOwner,
		}, func(opts *s3reader.Options) {
			opts.MaxBytesInSecond = c.MaxBytesInSecond
		})
		if err != nil {
			cancel(fmt.Errorf("create s3 reader error: %w", err))
		}
		defer r.Close()

		if _, err = r.WriteTo(io.MultiWriter(pw, verifier)); err != nil {
			cancel(err)
		}
	}()

	bar := tspb.DefaultBytes(int64(uncompressedSize), fmt.Sprintf("extracting %d files", len(headers)))
	defer bar.Close()

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

		_, err = commons.CopyBufferWithContext(ctx, io.MultiWriter(f, bar), zr, buf)
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
	_ = bar.Finish()

	if verifier == nil {
		c.logger.Printf("done downloading; no checksum to verify")
		return true, nil
	}

	if verifier.SumAndVerify(nil) {
		c.logger.Printf("done downloading; checksum matches")
	} else {
		c.logger.Printf("done downloading; checksum does not match: expect %s, got %s", man.Checksum, verifier.SumToString(nil))
	}

	return true, nil
}

func parseHeadersProgressBar(n int) *progressbar.ProgressBar {
	return progressbar.NewOptions(
		n,
		progressbar.OptionSetDescription(fmt.Sprintf("scanning %d headers", n)),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(false),
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
