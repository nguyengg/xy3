package download

import (
	"context"
	"errors"
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

	// attempt to create the local file that will store the downloaded artefact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := commons.StemExt(man.Key)
	f, err := commons.OpenExclFile(".", stem, ext, 0o666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	defer f.Close()

	name := f.Name()

	downloadErr := xy3.Download(
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
	if downloadErr != nil {
		if _, ok := xy3.IsErrChecksumMismatch(downloadErr); !ok {
			_, _ = f.Close(), os.Remove(name)
			return downloadErr
		}

		// checksum mismatch: log it, keep the file (and any extract that follows), but preserve
		// downloadErr so the CLI counts this as a failure and exits non-zero.
		logger.Print(downloadErr)
	}

	if !c.NoExtract {
		if extractErr := c.extract(ctx, name); extractErr != nil {
			if downloadErr != nil {
				return errors.Join(downloadErr, extractErr)
			}
			return extractErr
		}
		// extract succeeded — the extracted directory stays on disk;
		// clean up only the intermediate archive file.
		_, _ = f.Close(), os.Remove(name)
	}

	return downloadErr
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

	// attempt to create the local file that will store the downloaded artefact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := commons.StemExt(key)
	f, err := commons.OpenExclFile(".", stem, ext, 0o666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	defer f.Close()

	name := f.Name()

	downloadErr := xy3.Download(
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
	if downloadErr != nil {
		if _, ok := xy3.IsErrChecksumMismatch(downloadErr); !ok {
			_, _ = f.Close(), os.Remove(name)
			return downloadErr
		}

		// see downloadFromManifest for policy: on mismatch, keep the file, still exit non-zero.
		logger.Print(downloadErr)
	}

	if !c.NoExtract {
		if extractErr := c.extract(ctx, name); extractErr != nil {
			if downloadErr != nil {
				return errors.Join(downloadErr, extractErr)
			}
			return extractErr
		}
		_, _ = f.Close(), os.Remove(name)
	}

	return downloadErr
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
