package download

import (
	"context"
	"fmt"
	"os"

	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) downloadFromManifest(ctx context.Context, manifestName string) error {
	man, err := internal.LoadManifestFromFile(manifestName)
	if err != nil {
		return fmt.Errorf("read manifest error: %w", err)
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := util.StemAndExt(man.Key)
	f, err := util.OpenExclFile(".", stem, ext, 0666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	name := f.Name()

	if err, _ = xy3.Download(ctx, c.client, man.Bucket, man.Key, f, func(opts *xy3.DownloadOptions) {
		opts.ExpectedChecksum = man.Checksum
	}), f.Close(); err != nil {
		if _, ok := xy3.IsErrChecksumMismatch(err); ok {
			c.logger.Print(err)
		} else {
			_ = os.Remove(name)
			return err
		}
	}

	if c.Extract {
		if _, err = xy3.Decompress(ctx, name, "."); err == nil {
			_ = os.Remove(name)
		}
	}

	return err
}

func (c *Command) downloadFromS3(ctx context.Context, s3Uri string) error {
	bucket, key, err := util.ParseS3URI(s3Uri)
	if err != nil {
		return fmt.Errorf(`invalid s3 URI "%s": %w`, s3Uri, err)
	}

	// attempt to create the local file that will store the downloaded artifact.
	// if we fail to download the file successfully, clean up by deleting the local file.
	stem, ext := util.StemAndExt(key)
	f, err := util.OpenExclFile(".", stem, ext, 0666)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	name := f.Name()

	if err, _ = xy3.Download(ctx, c.client, bucket, key, f), f.Close(); err != nil {
		if _, ok := xy3.IsErrChecksumMismatch(err); ok {
			c.logger.Print(err)
		} else {
			_ = os.Remove(name)
			return err
		}
	}

	if c.Extract {
		if _, err = xy3.Decompress(ctx, name, "."); err == nil {
			_ = os.Remove(name)
		}
	}

	return err
}
