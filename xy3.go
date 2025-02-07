package xy3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Amazon S3 multipart upload limits
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const (
	MaxFileSize        = int64(5_497_558_138_880)
	MaxPartCount       = 10_000
	MinPartSize        = int64(5_242_880)
	MaxPartSize        = int64(5_368_709_120)
	DefaultConcurrency = 3
)

// Upload uploads the named file to S3 using multipart upload with progress report.
//
// Upload can only upload named files on the local filesystem with fixed size. If you need to upload in-memory content\
// or streaming content in general (i.e. io.Reader), use [s3manager Uploader] which is smart enough to use a single
// PutObject if the content is small enough. Upload always uses multipart upload.
//
// If you would like to use [s3manager Uploader] but want to add progress report, check out
// managerlogging.WrapUploadAPIClient which provides progress logging by decorating the manager.UploadAPIClient
// and manager.DownloadAPIClient instance.
//
// [s3manager Uploader]: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager
func Upload(ctx context.Context, client UploadAPIClient, name string, input *s3.CreateMultipartUploadInput, optFns ...func(*UploadOptions)) (*s3.CompleteMultipartUploadOutput, error) {
	u, err := newMultipartUploader(client, optFns...)
	if err != nil {
		return nil, err
	}

	return u.upload(ctx, name, input)
}

// UploadAPIClient abstracts a subset of S3 methods that is used by Upload.
type UploadAPIClient interface {
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// Download downloads the S3 object specified by the given bucket and key and writes to the given io.Writer.
func Download(ctx context.Context, client DownloadAPIClient, bucket, key string, w io.Writer, optFns ...func(*DownloadOptions)) error {
	d, err := newDownloader(client, optFns...)
	if err != nil {
		return err
	}

	return d.download(ctx, bucket, key, w)
}

// DownloadAPIClient abstracts a subset of S3 methods that is required by Download.
//
// HeadObject is used to determine the total length of the file so that the number of parts is known beforehand, which
// makes progress report a lot more accurate.
type DownloadAPIClient interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}
