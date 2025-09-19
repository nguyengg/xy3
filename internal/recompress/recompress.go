package recompress

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bodgit/sevenzip"
	"github.com/nguyengg/go-aws-commons/s3writer"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
	"github.com/schollz/progressbar/v3"
)

func (c *Command) recompressArchive(ctx context.Context, manifestName string) error {
	// at the moment, only support recompressing from 7z to zip which is my main use case.
	srcManifest, err := manifest.UnmarshalFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read mannifest error: %w", err)
	}
	stem, ext := util.StemAndExt(srcManifest.Key)
	if ext != ".7z" {
		return fmt.Errorf("unsupported extension; expected .7z, got %s", ext)
	}

	// download to temp file.
	srcName, err := c.download(ctx, srcManifest)
	if err != nil {
		return fmt.Errorf("download original archive error: %w", err)
	}

	// whilst decompressing from 7z, compress to zip and upload immediately, all in memory.
	key, err := c.findUnusedS3Key(ctx, srcManifest, stem)
	if err != nil {
		return err
	}

	c.logger.Printf(`uploading to "s3://%s/%s"`, srcManifest.Bucket, key)

	src, err := sevenzip.OpenReader(srcName)
	if err != nil {
		return fmt.Errorf("open original archive error: %w", err)
	}
	defer src.Close()

	bar := internal.DefaultBytes(
		int64(len(src.File)),
		"recompressing",
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetMaxDetailRow(1))
	defer bar.Close()

	w, err := s3writer.New(ctx, c.client, &s3.PutObjectInput{
		Bucket:              aws.String(srcManifest.Bucket),
		Key:                 aws.String(key),
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		ContentType:         aws.String("application/zip"),
		Metadata:            map[string]string{"name": stem},
		StorageClass:        types.StorageClassIntelligentTiering,
	}, func(options *s3writer.Options) {
		options.Concurrency = c.MaxConcurrency
	})
	if err != nil {
		return fmt.Errorf("create s3 writer error: %w", err)
	}

	hash := sri.NewSha256()
	sizer := &sizer{}
	zw := zip.NewWriter(io.MultiWriter(w, hash, sizer))
	zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.BestCompression)
	})

	buf := make([]byte, 32*1024)
	for _, f := range src.File {
		if f.FileInfo().IsDir() {
			continue
		}

		_ = bar.AddDetail(f.Name)

		if _, err := c.recompressFile(ctx, f, zw, buf); err != nil {
			return err
		}

		_ = bar.Add(1)
	}
	if err = zw.Close(); err != nil {
		return fmt.Errorf("write to zip archive error: %w", err)
	}
	if err = w.Close(); err != nil {
		return fmt.Errorf("upload to s3 error: %w", err)
	}

	c.logger.Printf("done uploading")

	// now generate the local .s3 file that contains the S3 URI. if writing to file fails, prints the JSON content
	// to standard output so that they can be saved manually later.
	m := manifest.Manifest{
		Bucket:              srcManifest.Bucket,
		Key:                 key,
		ExpectedBucketOwner: c.ExpectedBucketOwner,
		Size:                sizer.size,
		Checksum:            hash.SumToString(nil),
	}
	mf, err := util.OpenExclFile(".", stem, ".zip.s3", 0666)
	if err != nil {
		return err
	}
	if err, _ = m.MarshalTo(mf), mf.Close(); err != nil {
		return err
	}

	c.logger.Printf(`wrote to manifest "%s"`, mf.Name())

	// success so let's delete the original archive on local fs.
	if err := os.Remove(srcName); err != nil {
		c.logger.Printf(`delete original archive "%s" error: %v`, srcName, err)
	}

	// TODO delete remote file as well as local manifest of source.
	return nil
}

func (c *Command) recompressFile(ctx context.Context, src *sevenzip.File, zw *zip.Writer, buf []byte) (string, error) {
	r, err := src.Open()
	if err != nil {
		return "", fmt.Errorf(`open file "%s" in source archive error: %w`, src.Name, err)
	}
	defer r.Close()

	fh := &zip.FileHeader{
		Name:     strings.ReplaceAll(src.Name, "\\", "/"),
		Modified: src.Modified,
	}
	fh.SetMode(src.Mode())

	w, err := zw.CreateHeader(fh)
	if err != nil {
		return fh.Name, fmt.Errorf(`create file "%s" in target archive error: %w`, fh.Name, err)
	}

	if _, err = util.CopyBufferWithContext(ctx, w, r, buf); err != nil {
		return fh.Name, fmt.Errorf(`recompress file "%s" to "%s" error: %w`, src.Name, fh.Name, err)
	}

	return fh.Name, nil
}

// findUnusedS3Key returns an S3 key pointing to a non-existing S3 object that can be used to upload file.
func (c *Command) findUnusedS3Key(ctx context.Context, src manifest.Manifest, stem string) (string, error) {
	prefix := filepath.Dir(src.Key)
	if prefix == "." {
		prefix = ""
	} else {
		prefix = prefix + "/"
	}

	key := prefix + stem + ".zip"
	for i := 0; ; {
		if _, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:              aws.String(src.Bucket),
			Key:                 aws.String(key),
			ExpectedBucketOwner: c.ExpectedBucketOwner,
		}); err != nil {
			if errors.Is(err, context.Canceled) {
				return "", err
			}

			var re *awshttp.ResponseError
			if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
				break
			}

			return "", fmt.Errorf("find unused S3 key error: %w", err)
		}
		i++
		key = fmt.Sprintf("%s%s-%d.zip", prefix, stem, i)
	}

	return key, nil
}

type sizer struct {
	size int64
}

func (s *sizer) Write(p []byte) (n int, err error) {
	n = len(p)
	s.size += int64(n)
	return
}
