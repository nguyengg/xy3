package download

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

func (c *Command) downloadManifests(ctx context.Context, s3Location string) (n int, err error) {
	bucket, key, err := util.ParseS3URI(s3Location)
	if err != nil {
		return 0, fmt.Errorf(`invalid s3 location "%s": %w`, s3Location, err)
	}
	var prefix *string
	if key != "" {
		prefix = &key
	}

	for paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: prefix,
	}); paginator.HasMorePages(); {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("list objects error: %w", err)
		}

		for _, obj := range page.Contents {
			headObjectResult, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return n, fmt.Errorf(`get metadata about "%s" error: %w`, aws.ToString(obj.Key), err)
			}

			m := internal.Manifest{
				Bucket:   bucket,
				Key:      aws.ToString(obj.Key),
				Size:     aws.ToInt64(obj.Size),
				Checksum: headObjectResult.Metadata["checksum"],
			}

			f, err := os.OpenFile(path.Base(m.Key)+".s3", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return n, fmt.Errorf("create manifest file error: %w", err)
			}
			name := f.Name()

			if err, _ = m.SaveTo(f), f.Close(); err != nil {
				return n, fmt.Errorf(`write manifest to "%s" error: %w`, name, err)
			}

			n++
			if err = os.Chtimes(f.Name(), time.Time{}, aws.ToTime(headObjectResult.LastModified)); err != nil {
				log.Printf(`wrote "%s"; change modify time error: %v`, name, err)
			} else {
				log.Printf(`wrote "%s"`, name)
			}
		}
	}

	return n, nil
}
