package managerlogging

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"sync/atomic"
)

// LoggingDownloadAPIClient provides pre- and post- hooks on the methods that manager.Downloader may call.
//
// This package provides a few convenient methods to help log the progress of downloading large files.
//
// Unlike MultipartDownloader.PreDownloadPart, LoggingDownloadAPIClient.PreDownloadPart cannot be used to compute hash because
// it may be called from any of the goroutines that are responsible for downloading parts in parallel. MultipartDownloader
// provides a guarantee that MultipartDownloader.PreDownloadPart is only called from the main goroutine that calls Download,
// while there is currently no hook into manager.Downloader to do the same at the moment.
type LoggingDownloadAPIClient struct {
	manager.DownloadAPIClient
	PreGetObject  func(context.Context, *s3.GetObjectInput, ...func(*s3.Options))
	PostGetObject func(*s3.GetObjectOutput, error)
}

// WrapDownloadAPIClient wraps the specified manager.DownloadAPIClient as a LoggingDownloadAPIClient.
func WrapDownloadAPIClient(client manager.DownloadAPIClient, optFns ...func(*LoggingDownloadAPIClient)) *LoggingDownloadAPIClient {
	w := &LoggingDownloadAPIClient{DownloadAPIClient: client}
	for _, fn := range optFns {
		fn(w)
	}

	return w
}

// DownloadPartLogger configures the default logger for logging a successful download part.
type DownloadPartLogger struct {
	// Logger is the log.Logger instance to be used.
	Logger *log.Logger

	// PartCount is usually unknown (0 or negative values), but if provided, the logger will also print the expected
	// total number of parts.
	//
	// For example, if PartCount is 0, log messages will be in format `downloaded %d parts so far`.
	// If PartCount is positive, log messages will be in format `downloaded %d/%d parts so far`. It is possible for the
	// number of downloaded parts to exceed PartCount in this scenario.
	PartCount int
}

// LogSuccessfulDownloadPart creates a LoggingDownloadAPIClient.PostGetObject that logs only successfully downloaded
// parts.
//
// The logger keeps a running tally of the completed parts, and the log messages will be in this format:
// `downloaded %d parts so far`.
func LogSuccessfulDownloadPart(logger *log.Logger) func(*manager.Downloader) {
	return func(downloader *manager.Downloader) {
		var client *LoggingDownloadAPIClient
		switch v := downloader.S3.(type) {
		case *LoggingDownloadAPIClient:
			client = &LoggingDownloadAPIClient{
				DownloadAPIClient: v,
				PreGetObject:      v.PreGetObject,
				PostGetObject:     nil, // will be replaced
			}
		default:
			client = &LoggingDownloadAPIClient{DownloadAPIClient: v}
		}
		downloader.S3 = client

		var n atomic.Int32
		client.PostGetObject = func(output *s3.GetObjectOutput, err error) {
			if err == nil {
				logger.Printf("downloaded %d parts so far", n.Add(1))
			}
		}
	}
}

// LogSuccessfulDownloadPartWithExpectedPartCount creates a LoggingDownloadAPIClient.PostGetObject that logs only
// successfully downloaded parts against an expected total number of parts.
//
// The logger keeps a running tally of the completed parts, and the log messages will be in this format:
// `downloaded %d/%d parts so far` except for when the tally equals partCount, in which case the message becomes
// `downloaded %d/%d parts`. The tally is allowed to exceed the expected total number of parts without validation.
func LogSuccessfulDownloadPartWithExpectedPartCount(logger *log.Logger, partCount int32) func(*manager.Downloader) {
	return func(downloader *manager.Downloader) {
		var client *LoggingDownloadAPIClient

		switch v := downloader.S3.(type) {
		case *LoggingDownloadAPIClient:
			client = &LoggingDownloadAPIClient{
				DownloadAPIClient: v,
				PreGetObject:      v.PreGetObject,
				PostGetObject:     nil, // will be replaced
			}
		default:
			client = &LoggingDownloadAPIClient{DownloadAPIClient: v}
		}
		downloader.S3 = client

		var n atomic.Int32
		client.PostGetObject = func(output *s3.GetObjectOutput, err error) {
			if err == nil {
				v := n.Add(1)
				if v == partCount {
					logger.Printf("downloaded %d/%d parts", partCount, partCount)
				} else {
					logger.Printf("downloaded %d/%d parts so far", v, partCount)
				}
			}
		}
	}
}

func (l LoggingDownloadAPIClient) GetObject(ctx context.Context, input *s3.GetObjectInput, f ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if l.PreGetObject != nil {
		l.PreGetObject(ctx, input, f...)
	}
	o, err := l.DownloadAPIClient.GetObject(ctx, input, f...)
	if l.PostGetObject != nil {
		l.PostGetObject(o, err)
	}
	return o, err
}

var _ manager.DownloadAPIClient = LoggingDownloadAPIClient{}
var _ manager.DownloadAPIClient = &LoggingDownloadAPIClient{}
