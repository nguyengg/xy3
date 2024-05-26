package xy3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Upload uploads the named file to S3 using multipart upload with progress report.
func Upload(ctx context.Context, client *s3.Client, name string, input *s3.CreateMultipartUploadInput, optFns ...func(*MultipartUploader)) (*s3.CompleteMultipartUploadOutput, error) {
	u, err := newMultipartUploader(client, optFns...)
	if err != nil {
		return nil, err
	}

	return u.upload(ctx, name, input)
}
