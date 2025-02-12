package hashs3

import (
	"hash"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// HashS3 extends hash.Hash to add checksums to S3 operations.
//
// For PutObject, HashS3 can be used as a normal hash.Hash. When all data have been hashed, call SumPutObject to modify
// the [s3.PutObjectInput] with the expected algorithm and checksum.
//
// For multipart upload, HashS3 must be used at two places. First, it must be used as a normal hash.Hash passing the
// entire file, using SumCompleteMultipartUpload at the end. Secondly, for every UploadPart, HashUploadPart must be
// called to compute the hash for the specific part.
type HashS3 interface {
	hash.Hash

	// SumPutObject modifies the given [s3.PutObjectInput] with the appropriate checksum algorithm and value.
	//
	// Returns the same [s3.PutObjectInput].
	SumPutObject(*s3.PutObjectInput) *s3.PutObjectInput

	// SumCompleteMultipartUpload modifies the given [s3.CompleteMultipartUploadInput] with the appropriate checksum
	// algorithm, type, and value.
	//
	// If CreateMultipartUpload uses a checksum algorithm that does not support FULL_OBJECT type (e.g. SHA-1,
	// SHA-256), this method will no-op.
	//
	// Returns the same [s3.CompleteMultipartUploadInput].
	SumCompleteMultipartUpload(*s3.CompleteMultipartUploadInput) *s3.CompleteMultipartUploadInput

	// HashUploadPart modifies the given [s3.HashUploadPart] with the appropriate checksum algorith, type, and
	// value.
	//
	// If CreateMultipartUpload uses a checksum algorithm that does not support COMPOSITE type (e.g. CRC-64/NVME),
	// this method will no-op.
	//
	// Returns the same [s3.UploadPartInput].
	HashUploadPart([]byte, *s3.UploadPartInput) *s3.UploadPartInput
}

// NewOrDefault returns a new HashS3 from the specified checksum algorithm and type.
//
// If there is no checksum algorithm (empty string), returns the recommended hash and checksum algorithm to be used.
// This is because according to https://github.com/nguyengg/xy3/issues/1, if there is no checksum algorithm specified
// to a CreateMultipartUpload request, the CompleteMultipartUpload will ultimately fail so New at the moment will
// recommend a default combination that work.
//
// Caller should set the returned checksum algorithm and type to the [s3.CreateMultipartUploadInput] or
// [s3.PutObjectInput] accordingly.
func NewOrDefault(checksumAlgorithm types.ChecksumAlgorithm, checksumType types.ChecksumType) (HashS3, types.ChecksumAlgorithm, types.ChecksumType) {
	var h HashS3
	switch checksumAlgorithm {
	case types.ChecksumAlgorithmCrc32:
		h = newCrc32(checksumType)
	case types.ChecksumAlgorithmCrc32c:
		h = newCrc32c(checksumType)
	case types.ChecksumAlgorithmCrc64nvme:
		h = newCrc64nvme()
	case types.ChecksumAlgorithmSha1:
		h = newSha1()
	case types.ChecksumAlgorithmSha256:
		h = newSha256()
	default:
		// right now if nothing is specified default to crc32 full-object.
		// otherwise, it will fail, see https://github.com/nguyengg/xy3/issues/1.
		checksumAlgorithm = types.ChecksumAlgorithmCrc32
		checksumType = types.ChecksumTypeFullObject
		h = newCrc32(checksumType)
	}

	return h, checksumAlgorithm, checksumType
}

// NewFromPutObject is a variant of NewOrDefault that will modify the input parameters with default if necessary.
func NewFromPutObject(input *s3.PutObjectInput) HashS3 {
	h, alg, _ := NewOrDefault(input.ChecksumAlgorithm, types.ChecksumTypeFullObject)
	input.ChecksumAlgorithm = alg
	return h
}

// NewFromCreateMultipartUpload is a variant of NewOrDefault that will modify the input parameters with default if necessary.
func NewFromCreateMultipartUpload(input *s3.CreateMultipartUploadInput) HashS3 {
	h, alg, t := NewOrDefault(input.ChecksumAlgorithm, input.ChecksumType)
	input.ChecksumAlgorithm = alg
	input.ChecksumType = t
	return h
}

type base struct {
	hash.Hash
	composite hash.Hash
}

func (b base) SumPutObject(input *s3.PutObjectInput) *s3.PutObjectInput {
	return input
}

func (b base) SumCompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) *s3.CompleteMultipartUploadInput {
	return input
}

func (b base) HashUploadPart(_ []byte, input *s3.UploadPartInput) *s3.UploadPartInput {
	return input
}
