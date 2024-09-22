package managerlogging

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"sync/atomic"
)

// LoggingUploadAPIClient provides pre- and post- hooks on the methods that manager.Uploader may call.
//
// This package provides a few convenient methods to help log the progress of uploading large files.
//
// Unlike MultipartUploader.PreUploadPart, LoggingUploadAPIClient.PreUploadPart cannot be used to compute hash because
// it may be called from any of the goroutines that are responsible for uploading parts in parallel. MultipartUploader
// provides a guarantee that MultipartUploader.PreUploadPart is only called from the main goroutine that calls Upload,
// while there is currently no hook into manager.Uploader to do the same at the moment.
type LoggingUploadAPIClient struct {
	manager.UploadAPIClient
	PrePutObject                func(context.Context, *s3.PutObjectInput, ...func(*s3.Options))
	PostPutObject               func(*s3.PutObjectOutput, error)
	PreUploadPart               func(context.Context, *s3.UploadPartInput, ...func(*s3.Options))
	PostUploadPart              func(*s3.UploadPartOutput, error)
	PreCreateMultipartUpload    func(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options))
	PostCreateMultipartUpload   func(*s3.CreateMultipartUploadOutput, error)
	PreCompleteMultipartUpload  func(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options))
	PostCompleteMultipartUpload func(*s3.CompleteMultipartUploadOutput, error)
	PreAbortMultipartUpload     func(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options))
	PostAbortMultipartUpload    func(*s3.AbortMultipartUploadOutput, error)
}

// WrapUploadAPIClient wraps the specified manager.UploadAPIClient as a LoggingUploadAPIClient.
func WrapUploadAPIClient(client manager.UploadAPIClient, optFns ...func(*LoggingUploadAPIClient)) *LoggingUploadAPIClient {
	w := &LoggingUploadAPIClient{UploadAPIClient: client}
	for _, fn := range optFns {
		fn(w)
	}

	return w
}

// UploadPartLogger configures the default logger for logging a successful upload part.
type UploadPartLogger struct {
	// Logger is the log.Logger instance to be used.
	Logger *log.Logger

	// PartCount is usually unknown (0 or negative values), but if provided, the logger will also print the expected
	// total number of parts.
	//
	// For example, if PartCount is 0, log messages will be in format `uploaded %d parts so far`.
	// If PartCount is positive, log messages will be in format `uploaded %d/%d parts so far`. It is possible for the
	// number of uploaded parts to exceed PartCount in this scenario.
	PartCount int
}

// LogSuccessfulUploadPart creates a LoggingUploadAPIClient.PostUploadPart that logs only successfully uploaded parts.
//
// The logger keeps a running tally of the completed parts, and the log messages will be in this format:
// `uploaded %d parts so far`.
func LogSuccessfulUploadPart(logger *log.Logger) func(*manager.Uploader) {
	return func(uploader *manager.Uploader) {
		var client *LoggingUploadAPIClient
		switch v := uploader.S3.(type) {
		case *LoggingUploadAPIClient:
			client = &LoggingUploadAPIClient{
				UploadAPIClient:             v,
				PrePutObject:                v.PrePutObject,
				PostPutObject:               v.PostPutObject,
				PreUploadPart:               v.PreUploadPart,
				PostUploadPart:              nil, // will be replaced
				PreCreateMultipartUpload:    v.PreCreateMultipartUpload,
				PostCreateMultipartUpload:   v.PostCreateMultipartUpload,
				PreCompleteMultipartUpload:  v.PreCompleteMultipartUpload,
				PostCompleteMultipartUpload: v.PostCompleteMultipartUpload,
				PreAbortMultipartUpload:     v.PreAbortMultipartUpload,
				PostAbortMultipartUpload:    v.PostAbortMultipartUpload,
			}
		default:
			client = &LoggingUploadAPIClient{UploadAPIClient: v}
		}
		uploader.S3 = client

		var n atomic.Int32
		client.PostUploadPart = func(output *s3.UploadPartOutput, err error) {
			if err == nil {
				logger.Printf("uploaded %d parts so far", n.Add(1))
			}
		}
	}
}

// LogSuccessfulUploadPartWithExpectedPartCount creates a LoggingUploadAPIClient.PostUploadPart that logs only
// successfully uploaded parts against an expected total number of parts.
//
// The logger keeps a running tally of the completed parts, and the log messages will be in this format:
// `uploaded %d/%d parts so far` except for when the tally equals partCount, in which case the message becomes
// `uploaded %d/%d parts`. The tally is allowed to exceed the expected total number of parts without validation.
func LogSuccessfulUploadPartWithExpectedPartCount(logger *log.Logger, partCount int32) func(*manager.Uploader) {
	return func(uploader *manager.Uploader) {
		var client *LoggingUploadAPIClient

		switch v := uploader.S3.(type) {
		case *LoggingUploadAPIClient:
			client = &LoggingUploadAPIClient{
				UploadAPIClient:             v,
				PrePutObject:                v.PrePutObject,
				PostPutObject:               v.PostPutObject,
				PreUploadPart:               v.PreUploadPart,
				PostUploadPart:              nil, // will be replaced
				PreCreateMultipartUpload:    v.PreCreateMultipartUpload,
				PostCreateMultipartUpload:   v.PostCreateMultipartUpload,
				PreCompleteMultipartUpload:  v.PreCompleteMultipartUpload,
				PostCompleteMultipartUpload: v.PostCompleteMultipartUpload,
				PreAbortMultipartUpload:     v.PreAbortMultipartUpload,
				PostAbortMultipartUpload:    v.PostAbortMultipartUpload,
			}
		default:
			client = &LoggingUploadAPIClient{UploadAPIClient: v}
		}
		uploader.S3 = client

		var n atomic.Int32
		client.PostUploadPart = func(output *s3.UploadPartOutput, err error) {
			if err == nil {
				v := n.Add(1)
				if v == partCount {
					logger.Printf("uploaded %d/%d parts", partCount, partCount)
				} else {
					logger.Printf("uploaded %d/%d parts so far", v, partCount)
				}
			}
		}
	}
}

func (l LoggingUploadAPIClient) PutObject(ctx context.Context, input *s3.PutObjectInput, f ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if l.PrePutObject != nil {
		l.PrePutObject(ctx, input, f...)
	}
	o, err := l.UploadAPIClient.PutObject(ctx, input, f...)
	if l.PostPutObject != nil {
		l.PostPutObject(o, err)
	}
	return o, err
}

func (l LoggingUploadAPIClient) UploadPart(ctx context.Context, input *s3.UploadPartInput, f ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if l.PreUploadPart != nil {
		l.PreUploadPart(ctx, input, f...)
	}
	o, err := l.UploadAPIClient.UploadPart(ctx, input, f...)
	if l.PostUploadPart != nil {
		l.PostUploadPart(o, err)
	}
	return o, err
}

func (l LoggingUploadAPIClient) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, f ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if l.PreCreateMultipartUpload != nil {
		l.PreCreateMultipartUpload(ctx, input, f...)
	}
	o, err := l.UploadAPIClient.CreateMultipartUpload(ctx, input, f...)
	if l.PostCreateMultipartUpload != nil {
		l.PostCreateMultipartUpload(o, err)
	}
	return o, err
}

func (l LoggingUploadAPIClient) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, f ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if l.PreCompleteMultipartUpload != nil {
		l.PreCompleteMultipartUpload(ctx, input, f...)
	}
	o, err := l.UploadAPIClient.CompleteMultipartUpload(ctx, input, f...)
	if l.PostCompleteMultipartUpload != nil {
		l.PostCompleteMultipartUpload(o, err)
	}
	return o, err
}

func (l LoggingUploadAPIClient) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, f ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if l.PreAbortMultipartUpload != nil {
		l.PreAbortMultipartUpload(ctx, input, f...)
	}
	o, err := l.UploadAPIClient.AbortMultipartUpload(ctx, input, f...)
	if l.PostAbortMultipartUpload != nil {
		l.PostAbortMultipartUpload(o, err)
	}
	return o, err
}

var _ manager.UploadAPIClient = LoggingUploadAPIClient{}
var _ manager.UploadAPIClient = &LoggingUploadAPIClient{}
