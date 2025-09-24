package download

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/manifest"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) downloadFromManifest(ctx context.Context, manifestName string) error {
	man, err := manifest.UnmarshalFromFile(manifestName)
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

	if err, _ = internal.Download(ctx, c.client, man.Bucket, man.Key, f), f.Close(); err != nil {
		if errors.Is(err, internal.ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			_ = os.Remove(name)
			return err
		}
	}

	if c.Extract {
		if _, err = internal.Decompress(ctx, name, "."); err == nil {
			_ = os.Remove(name)
		}
	}

	return err
}

func (c *Command) downloadFromS3(ctx context.Context, s3Uri string) error {
	bucket, key, err := internal.ParseS3URI(s3Uri)
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

	if err, _ = internal.Download(ctx, c.client, bucket, key, f), f.Close(); err != nil {
		if errors.Is(err, internal.ErrChecksumMismatch{}) {
			c.logger.Print(err)
		} else {
			_ = os.Remove(name)
			return err
		}
	}

	if c.Extract {
		if _, err = internal.Decompress(ctx, name, "."); err == nil {
			_ = os.Remove(name)
		}
	}

	return err
}
