# xy3
[![Go Reference](https://pkg.go.dev/badge/github.com/nguyengg/xy3.svg)](https://pkg.go.dev/github.com/nguyengg/xy3)

`xy3` is born out of my need to create S3 backups while using [XYplorer](https://en.wikipedia.org/wiki/XYplorer). Here
are the XYplorer's file associations that I use:
```
|"Download from S3" s3>"xy3.exe" "download"
|"Delete from S3" s3>"xy3.exe" "remove"
|"Compress and upload to S3" \>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/"
|"Upload to S3" *>"xy3.exe" "upload" -b "bucket-name" -k "<curfolder>/"
```

## CLI

```shell
# Uploading a file will generate a local .s3 (JSON) file that stores metadata about how to retrieve the file.
# For example, this command will create doc.txt.s3 and log.zip.s3.
xy3 up -b "bucket-name" -k "key-prefix/" --expected-bucket-owner "1234" doc.txt log.zip

# Downloading from the JSON .s3 files will create unique names to prevent duplicates.
# For example, since doc.txt and log.zip still exist, this command will create doc-1.txt and log-1.zip.
xy3 down doc.txt.s3 log.zip.s3

# To remove both local and remote files, use this command.
xy3 remove doc.txt.s3 log.zip.s3
```

## Go Package

```go
package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Panicf("create SDK default config error: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	if _, err = xy3.Upload(ctx, client, "path/to/file.zip", &s3.CreateMultipartUploadInput{
		Bucket:              aws.String("my-bucket"),
		Key:                 aws.String("my-key"),
	}); err != nil {
		log.Panicf("upload error: %v", err)
	}
}
```

If you want to use [github.com/aws/aws-sdk-go-v2/feature/s3/manager](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager)
that comes with the SDK and adds logging:
```go
package main

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Panicf("create SDK default config error: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	// use a logger for all UploadPart.
	uploader := manager.NewUploader(client, xy3.LogSuccessfulUploadPart(log.Default()))

	// or specify them on a specific upload call.
	_, _ = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("my-key"),
		Body:   bytes.NewReader([]byte("hello, world!")),
	}, xy3.LogSuccessfulUploadPartWithExpectedPartCount(log.Default(), 100))
}
```
