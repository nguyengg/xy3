package s3writer

import (
	"fmt"
)

// MultipartUploadError is the error returns from any writes including Writer.Close if there was an error while using
// multipart upload.
type MultipartUploadError struct {
	// Err is the wrapped error encountered during multipart upload.
	Err error
	// UploadID is the upload Id of the multipart upload.
	UploadID string
	// Abort indicates whether an abort attempt was made successfully.
	Abort AbortAttempt
	// AbortErr is the error encountered while attempting to abort the multipart upload.
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
