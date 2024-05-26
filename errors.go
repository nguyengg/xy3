package xy3

import (
	"fmt"
)

// MultipartUploadError is returned only if an error occurs after [s3.Client.CreateMultipartUpload] has been called
// successfully to provide an upload Id as well as the result of the abort attempt.
type MultipartUploadError struct {
	Err      error
	UploadID string
	Abort    AbortAttempt
	AbortErr error
}

func (e MultipartUploadError) Unwrap() error {
	return e.Err
}

func (e MultipartUploadError) Error() string {
	switch e.Abort {
	case AbortNotAttempted:
		return fmt.Sprintf("multipart upload error, upload Id: %s (abort not attempted), cause: %v", e.UploadID, e.Err)
	case AbortSuccess:
		return fmt.Sprintf("multipart upload error, upload Id: %s (abort succeeds), cause: %v", e.UploadID, e.Err)
	case AbortFailure:
		return fmt.Sprintf("multipart upload error, upload Id: %s (abort fails, cause: %v), cause: %v", e.UploadID, e.AbortErr, e.Err)
	default:
		return fmt.Sprintf("multipart upload error, upload Id: %s, cause: %v", e.UploadID, e.Err)
	}
}

type AbortAttempt int

const (
	AbortNotAttempted AbortAttempt = iota
	AbortSuccess
	AbortFailure
)
