package download

import (
	"context"
	"fmt"
	"os"

	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) downloadFromManifest(ctx context.Context, manifestName string) error {
	man, err := internal.LoadManifestFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	cfg, client, err := c.createClient(ctx, man.Bucket)
	if err != nil {
		return err
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := util.StemAndExt(man.Key)
	f, err := util.OpenExclFile(".", stem, ext, 0666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	defer f.Close()

	name := f.Name()

	err = xy3.Download(
		ctx,
		client,
		man.Bucket,
		man.Key,
		f,
		xy3.WithExpectedBucketOwner(internal.FirstNonNil(man.ExpectedBucketOwner, cfg.ExpectedBucketOwner)),
		func(opts *xy3.DownloadOptions) {
			opts.S3ReaderOptions = func(opts *s3reader.Options) {
				opts.MaxBytesInSecond = c.MaxBytesInSecond
			}

			opts.ExpectedChecksum = man.Checksum
		})
	if err != nil {
		if _, ok := xy3.IsErrChecksumMismatch(err); !ok {
			_, _ = f.Close(), os.Remove(name)
			return err
		}

		c.logger.Print(err)
	}

	if !c.NoExtract {
		if err = c.extract(ctx, name); err == nil {
			_, _ = f.Close(), os.Remove(name)
		}
	}

	return err
}

func (c *Command) downloadFromS3(ctx context.Context, s3Uri string) error {
	bucket, key, err := internal.ParseS3URI(s3Uri)
	if err != nil {
		return fmt.Errorf(`invalid s3 URI "%s": %w`, s3Uri, err)
	}

	cfg, client, err := c.createClient(ctx, bucket)
	if err != nil {
		return err
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := util.StemAndExt(key)
	f, err := util.OpenExclFile(".", stem, ext, 0666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	defer f.Close()

	name := f.Name()

	err = xy3.Download(
		ctx,
		client,
		bucket,
		key,
		f,
		xy3.WithExpectedBucketOwner(cfg.ExpectedBucketOwner),
		func(opts *xy3.DownloadOptions) {
			opts.S3ReaderOptions = func(opts *s3reader.Options) {
				opts.MaxBytesInSecond = c.MaxBytesInSecond
			}
		})
	if err != nil {
		if _, ok := xy3.IsErrChecksumMismatch(err); !ok {
			_, _ = f.Close(), os.Remove(name)
			return err
		}

		c.logger.Print(err)
	}

	if !c.NoExtract {
		if err = c.extract(ctx, name); err == nil {
			_, _ = f.Close(), os.Remove(name)
		}
	}

	return err
}

func (c *Command) extract(ctx context.Context, name string) (err error) {
	// if file is eligible for auto-extract then proceed to do so.
	if cd := xy3.NewDecompressorFromName(name); cd != nil {
		if _, err = xy3.Decompress(ctx, name, "."); err == nil {
			c.logger.Printf(`deleting temporary archive "%s"`, name)
			_ = os.Remove(name)
		}
	}

	return err
}
