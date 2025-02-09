package download

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bodgit/sevenzip"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
)

type s3ReaderAt struct {
	ctx                 context.Context
	client              *s3.Client
	bucket, key         string
	expectedBucketOwner *string
}

func (s s3ReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	getObjectOutput, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket:              &s.bucket,
		Key:                 &s.key,
		ExpectedBucketOwner: s.expectedBucketOwner,
		Range:               aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p)-1))),
	})
	if err != nil {
		return 0, fmt.Errorf("s3ReaderAt: ranged get error: %w", err)
	}
	defer getObjectOutput.Body.Close()
	return io.ReadFull(getObjectOutput.Body, p)
}

func (c *Command) tryStream7z(ctx context.Context, man manifest.Manifest) (bool, error) {
	// HeadObject to give the complete length.
	var compressedSize int64
	if headObjectOutput, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:              &man.Bucket,
		Key:                 &man.Key,
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}); err != nil {
		return false, err
	} else if compressedSize = aws.ToInt64(headObjectOutput.ContentLength); compressedSize <= 10*1024*1024 {
		// if the file is less than 10 MB then don't bother with streaming.
		log.Printf("file is too small for streaming")
		return false, nil
	}

	r, err := sevenzip.NewReader(s3ReaderAt{
		ctx:                 ctx,
		client:              c.client,
		bucket:              man.Bucket,
		key:                 man.Key,
		expectedBucketOwner: man.ExpectedBucketOwner,
	}, compressedSize)
	if err != nil {
		log.Printf("will not treat file as 7z, read error: %v", err)
		return false, nil
	}

	// this is a variant of zipper.findRoot that doesn't stop even if noRoot is true.
	// we will go through all the headers to find the total uncompressed size.
	var uncompressedSize uint64
	noRoot, root := false, ""
	fileCount := 0
	for _, fh := range r.File {
		if fh.FileInfo().IsDir() {
			continue
		}

		fileCount++
		uncompressedSize += fh.UncompressedSize

		if !noRoot {
			paths := sep.Split(fh.Name, 2)
			if len(paths) == 1 {
				// this is a file at top level so there is no root for sure.
				noRoot, root = true, ""
			} else {
				switch root {
				case paths[0]:
				case "":
					root = paths[0]
				default:
					noRoot, root = true, ""
				}
			}
		}
	}
	trimRoot := func(path string) string {
		return path
	}
	if !noRoot && root != "" {
		trimRoot = func(path string) string {
			return strings.TrimLeft(strings.TrimPrefix(path, root), `\/`)
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
	bar := internal.DefaultBytes(int64(uncompressedSize), fmt.Sprintf("extracting %d files", fileCount))
	defer bar.Close()

	var (
		buf = make([]byte, 32*1024)
		dst *os.File
		src fs.File
	)
	for _, fh := range r.File {
		if fh.FileInfo().IsDir() {
			continue
		}

		name := fh.Name
		path := filepath.Join(dir, trimRoot(name))

		fi := fh.FileInfo()
		if fi.IsDir() {
			err = os.MkdirAll(path, fi.Mode())
			continue
		}

		src, err = r.Open(fh.Name)
		if err != nil {
			return true, fmt.Errorf("open file in archive error: %w", err)
		}

		if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
			dst, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, fi.Mode())
		}
		if err != nil {
			_ = src.Close()
			return true, fmt.Errorf("create local file error: %w", err)
		}

		_, err = xy3.CopyBufferWithContext(ctx, io.MultiWriter(dst, bar), src, buf)
		_, _ = src.Close(), dst.Close()
		if err != nil {
			return true, fmt.Errorf("write to local file error: %w", err)
		}
	}

	return true, nil
}
