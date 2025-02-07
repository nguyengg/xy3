# Add Progress Logging to feature/s3/manager

## Setup

```shell
go get github.com/nguyengg/xy3
```

## Code Example

```go
package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/xy3/managerlogging"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, _ := config.LoadDefaultConfig(ctx)
	client := s3.NewFromConfig(cfg)

	// use a logger for all UploadPart. the log messages will be in this format: `uploaded %d parts so far`.
	uploader := manager.NewUploader(client, managerlogging.LogSuccessfulUploadPart(log.Default()))

	// or specify them on a specific upload call. because the expected number of parts is known, the log message
	// will be in this format: `uploaded %d/%d parts so far`.
	//
	// note that is it preferable to use the logging wrappers on a per Upload/Download operation like this.
	// it's because each wrapper maintains a running tally that will be wrong if the same LoggingUploadAPIClient is
	// reused for subsequent Uploads/Downloads. I could cache the UploadId or the Bucket/Key but that is complexity
	// not worth adding from the current usage pattern.
	_, _ = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("my-key"),
		Body:   bytes.NewReader([]byte("hello, world!")),
	}, managerlogging.LogSuccessfulUploadPartWithExpectedPartCount(log.Default(), 100))

	// same for download, both types of logging wrappers exist.
	downloader := manager.NewDownloader(client, managerlogging.LogSuccessfulDownloadPart(log.Default()))
	_, _ = downloader.Download(ctx, nil, &s3.GetObjectInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("my-key"),
	}, managerlogging.LogSuccessfulDownloadPartWithExpectedPartCount(log.Default(), 100))
}

```
