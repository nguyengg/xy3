package remove

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"os"
	"path/filepath"
	"xy3/internal/manifest"
)

func (c *Command) remove(ctx context.Context, name string) error {
	basename := filepath.Base(name)
	logger := log.New(os.Stderr, `"`+basename+`" `, log.LstdFlags)

	file, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("open file error: %w", err)
	}
	man, err := manifest.UnmarshalFrom(file)
	if _ = file.Close(); err != nil {
		return err
	}

	logger.Printf(`deleting "s3://%s/%s"`, man.Bucket, man.Key)
	if _, err = c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:              aws.String(man.Bucket),
		Key:                 aws.String(man.Key),
		ExpectedBucketOwner: man.ExpectedBucketOwner,
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		var re *http.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() != 404 {
			return fmt.Errorf("remove S3 object error: %w", err)
		}
	}

	if !c.KeepLocal {
		if err = os.Remove(name); err != nil {
			logger.Printf(`deleting file "%s"`, name)
			if err = os.Remove(name); err != nil {
				logger.Printf("remove file error: %v", err)
			}
		}
	}

	return nil
}
