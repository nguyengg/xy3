package download

import (
	"context"
	"fmt"
	"os"

	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/go-aws-commons/s3reader"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
)

func (c *Command) downloadFromManifest(ctx context.Context, manifestName string) error {
	logger := internal.MustLogger(ctx)

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
	stem, ext := commons.StemExt(man.Key)
	f, err := commons.OpenExclFile(".", stem, ext, 0666)
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
		xy3.WithExpectedBucketOwner(internal.FirstNonNilPtr(man.ExpectedBucketOwner, cfg.ExpectedBucketOwner)),
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

		logger.Print(err)
	}

	if !c.NoExtract {
		if err = c.extract(ctx, name); err == nil {
			_, _ = f.Close(), os.Remove(name)
		}
	}

	return err
}

func (c *Command) downloadFromS3(ctx context.Context, s3Uri string) error {
	logger := internal.MustLogger(ctx)

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
	stem, ext := commons.StemExt(key)
	f, err := commons.OpenExclFile(".", stem, ext, 0666)
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

		logger.Print(err)
	}

	if !c.NoExtract {
		if err = c.extract(ctx, name); err == nil {
			_, _ = f.Close(), os.Remove(name)
		}
	}

	return err
}

func (c *Command) extract(ctx context.Context, name string) (err error) {
	logger := internal.MustLogger(ctx)

	// if file is eligible for auto-extract then proceed to do so.
	if cd := xy3.NewDecompressorFromName(name); cd != nil {
		if _, err = xy3.Decompress(ctx, name, "."); err == nil {
			logger.Printf(`deleting temporary archive "%s"`, name)
			_ = os.Remove(name)
		}
	}

	return err
}
