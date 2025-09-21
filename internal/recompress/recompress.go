package recompress

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
	"github.com/nguyengg/go-aws-commons/sri"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/extract"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) recompressArchive(ctx context.Context, manifestName string) error {
	srcManifest, err := manifest.UnmarshalFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// 7z archives require local files anyway so let's just download the file.
	src, err := c.download(ctx, srcManifest)
	if err != nil {
		return err
	}

	// use the extension of the file to determine which kind of compression algorithm to use.
	stem, ext := util.StemAndExt(srcManifest.Key)
	files, err := extract.EntriesFromExt(src, ext)
	if err != nil {
		return err
	}

	// compute checksum to include in manifest.
	lev := sri.NewSha256()

	// right now, we'll recompress to a file on local system using tar and zstd at best compression.
	// TODO re-upload the file right to S3 right away to keep the in-memory challenge.
	dst, err := util.OpenExclFile(".", stem, ".tar.zst", 0666)
	if err != nil {
		return fmt.Errorf("create local file error: %w", err)
	}
	defer dst.Close()

	// use an unknown length progress bar.
	bar := internal.DefaultBytes(-1, "recompressing")
	defer bar.Close()

	zw, err := zstd.NewWriter(
		io.MultiWriter(dst, lev, bar),
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(c.MaxConcurrency))
	if err != nil {
		return fmt.Errorf("create zstd writer error: %w", err)
	}

	buf := make([]byte, 32*1024)
	tw := tar.NewWriter(zw)
	for f, err := range files {
		if err != nil {
			_ = f.Close()
			return err
		}

		name, fi := f.Name(), f.FileInfo()

		hdr, err := tar.FileInfoHeader(fi, name)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf(`create tar header for "%s" error: %w`, name, err)
		}
		hdr.Name = name

		if err = tw.WriteHeader(hdr); err != nil {
			_ = f.Close()
			return fmt.Errorf(`write tar header for "%s" error: %w`, name, err)
		}

		if _, err = util.CopyBufferWithContext(ctx, tw, f, buf); err != nil {
			_ = f.Close()
			return fmt.Errorf(`recompression file "%s" error: %w`, name, err)
		}

		if err = f.Close(); err != nil {
			return fmt.Errorf(`close file "%s" error: %w`, name, err)
		}
	}

	if err = tw.Close(); err != nil {
		return fmt.Errorf("close tar writer error: %w", err)
	}

	if err = zw.Close(); err != nil {
		return fmt.Errorf("close zstd writer error: %w", err)
	}

	log.Printf("new checksum: %v", lev.SumToString(nil))
	return nil
}

// findUnusedS3Key returns an S3 key pointing to a non-existing S3 object that can be used to upload file.
func (c *Command) findUnusedS3Key(ctx context.Context, src manifest.Manifest, stem, ext string) (string, error) {
	prefix := filepath.Dir(src.Key)
	if prefix == "." {
		prefix = ""
	} else {
		prefix = prefix + "/"
	}

	key := prefix + stem + ext
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
		key = fmt.Sprintf("%s%s-%d%s", prefix, stem, i, ext)
	}

	return key, nil
}
