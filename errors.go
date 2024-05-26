package xy3

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// MultipartUploadError is returned only if an error occurs after [s3.Client.CreateMultipartUpload] has been called
// successfully to provide an upload Id as well as the result of the abort attempt.
type MultipartUploadError struct {
	Err      error
	UploadID *string
	Abort    AbortAttempt
}

func (e MultipartUploadError) Unwrap() error {
	return e.Err
}

func (e MultipartUploadError) Error() string {
	switch e.Abort {
	case AbortNotAttempted:
		return fmt.Sprintf("multipart upload error, upload Id: %s (abort not attempted), cause: %s", aws.ToString(e.UploadID), e.Err.Error())
	case AbortSuccess:
		return fmt.Sprintf("multipart upload error, upload Id: %s (abort succeeds), cause: %s", aws.ToString(e.UploadID), e.Err.Error())
	case AbortFailure:
		return fmt.Sprintf("multipart upload error, upload Id: %s (abort fails), cause: %s", aws.ToString(e.UploadID), e.Err.Error())
	default:
		return fmt.Sprintf("multipart upload error, upload Id: %s, cause: %s", aws.ToString(e.UploadID), e.Err.Error())
	}
}

type AbortAttempt int

const (
	AbortNotAttempted AbortAttempt = iota
	AbortSuccess
	AbortFailure
)
