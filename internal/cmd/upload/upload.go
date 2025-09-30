package upload

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3writer"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) upload(ctx context.Context, name string) (err error) {
	// f will be either name opened as-is, or a new archive created from compressing directory with that name.
	// if the latter is the case, the file will be deleted upon return.
	var (
		f           *os.File
		fi          os.FileInfo
		contentType *string
		size        int64
		checksum    string
		success     bool
	)

	// name can either be a file or a directory, so use stat to determine what to do.
	// if it's a directory, compress it and the resulting archive will be deleted upon return.
	switch fi, err = os.Stat(name); {
	case err != nil:
		return fmt.Errorf(`stat file "%s" error: %w`, name, err)

	case fi.IsDir():
		var archiveName string
		archiveName, contentType, size, checksum, err = c.compressDir(ctx, name)
		if err != nil {
			return fmt.Errorf(`compress directory "%s" error: %w`, name, err)
		}

		if f, err = os.Open(archiveName); err != nil {
			_ = os.Remove(archiveName)
			return fmt.Errorf(`open archive "%s" error: %w`, archiveName, err)
		}

		defer func() {
			_, _ = f.Close(), os.Remove(archiveName)

			if success && c.Delete {
				if err = os.RemoveAll(name); err != nil {
					c.logger.Printf(`delete directory "%s" error: %v`, name, err)
				}
			}
		}()

	default:
		if f, err = os.Open(name); err != nil {
			return fmt.Errorf(`open file "%s" error: %w`, name, err)
		}
		defer func() {
			_ = f.Close()

			if success && c.Delete {
				if err = os.Remove(name); err != nil {
					c.logger.Printf(`delete file "%s" error: %v`, name, err)
				}
			}
		}()

		// read first 512 bytes to detect content type.
		// if this won't produce a usable content type then let S3 decides it (which is probably going to be "binary/octet-stream").
		data := make([]byte, 512)
		if _, err = f.Read(data); err != nil {
			return fmt.Errorf("read first 512 bytes error: %w", err)
		}

		if v := http.DetectContentType(data); v != "application/octet-stream" {
			contentType = &v
		}

		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf(`seek start of "%s" error: %w`, name, err)
		}
	}

	// use the name of the archive (in the case of directory) to have meaningful extensions.
	stem, ext := util.StemAndExt(f.Name())
	key := c.prefix + stem + ext

	c.logger.Printf(`uploading to "s3://%s/%s"`, c.bucket, key)

	man, err := xy3.Upload(
		ctx,
		c.client,
		f,
		c.bucket,
		key,
		func(uploadOpts *xy3.UploadOptions) {
			uploadOpts.S3WriterOptions = func(s3writerOpts *s3writer.Options) {
				s3writerOpts.MaxBytesInSecond = c.MaxBytesInSecond
			}

			uploadOpts.PutObjectInputOptions = func(input *s3.PutObjectInput) {
				input.ContentType = contentType
				input.ExpectedBucketOwner = c.cfg.ExpectedBucketOwner
				input.StorageClass = c.cfg.StorageClass
			}

			uploadOpts.ExpectedChecksum = checksum
			uploadOpts.ExpectedSize = size
		})
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}

	c.logger.Printf("done uploading")

	// now generate the local .s3 file that contains the S3 URI. if writing to file fails, prints the JSON content
	// to standard output so that they can be saved manually later.
	mf, err := util.OpenExclFile(".", stem, ext+".s3", 0666)
	if err != nil {
		_ = man.SaveTo(os.Stdout)
		return fmt.Errorf("create manifest file error: %w", err)
	}

	if err, _ = man.SaveTo(mf), mf.Close(); err != nil {
		_ = man.SaveTo(os.Stdout)
		return fmt.Errorf("write manifest error: %w", err)
	}

	c.logger.Printf(`wrote to manifest "%s"`, mf.Name())

	success = true
	return nil
}
