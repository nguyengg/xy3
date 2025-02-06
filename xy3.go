package xy3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Upload uploads the named file to S3 using multipart upload with progress report.
//
// Unlike manager.Uploader which receives an io.Reader which in turn can upload objects of unknown size (good for
// streaming object on the fly), this method requires the object to be entirely contained in a file with known definite
// size. If you would like to use manager.Uploader, check out WrapUploadAPIClient which provides progress logging by
// decorating the manager.UploadAPIClient instance. manager.DownloadAPIClient
//
// Unlike manager.Uploader which knows to use a single S3 PutObject if the file is small enough, this method always uses
// S3 Multipart Upload.
func Upload(ctx context.Context, client UploadAPIClient, name string, input *s3.CreateMultipartUploadInput, optFns ...func(*MultipartUploader)) (*s3.CompleteMultipartUploadOutput, error) {
	u, err := newMultipartUploader(client, optFns...)
	if err != nil {
		return nil, err
	}

	return u.upload(ctx, name, input)
}

// UploadAPIClient declares a subset of S3 methods that is required by Upload.
type UploadAPIClient interface {
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// Download downloads the S3 object specified by bucket and key and writes to the given io.Writer.
func Download(ctx context.Context, client DownloadAPIClient, bucket, key string, w io.Writer, optFns ...func(*Downloader)) error {
	d, err := newDownloader(client, optFns...)
	if err != nil {
		return err
	}

	return d.download(ctx, bucket, key, w)
}

// DownloadAPIClient declares a subset of S3 methods that is required by Download.
type DownloadAPIClient interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}
