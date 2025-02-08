package hashs3

import (
	"encoding/base64"
	"hash/crc32"
	"hash/crc64"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func newCrc32(checksumType types.ChecksumType) HashS3 {
	return algCrc32{
		base{
			Hash:      crc32.NewIEEE(),
			composite: crc32.NewIEEE(),
		},
		checksumType,
	}
}

type algCrc32 struct {
	base
	checksumType types.ChecksumType
}

func (a algCrc32) SumPutObject(input *s3.PutObjectInput) *s3.PutObjectInput {
	if a.checksumType == types.ChecksumTypeFullObject {
		input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(a.Sum(nil)))
	}
	return input
}

func (a algCrc32) SumCompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) *s3.CompleteMultipartUploadInput {
	if a.checksumType == types.ChecksumTypeFullObject {
		input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(a.Sum(nil)))
	}
	return input
}

func (a algCrc32) HashUploadPart(data []byte, input *s3.UploadPartInput) *s3.UploadPartInput {
	a.composite.Reset()
	a.composite.Write(data)
	input.ChecksumCRC32 = aws.String(base64.StdEncoding.EncodeToString(a.composite.Sum(nil)))
	return input
}

func newCrc32c(checksumType types.ChecksumType) HashS3 {
	return algCrc32c{
		base{
			Hash:      crc32.New(crc32.MakeTable(crc32.Castagnoli)),
			composite: crc32.New(crc32.MakeTable(crc32.Castagnoli)),
		},
		checksumType,
	}
}

type algCrc32c struct {
	base
	checksumType types.ChecksumType
}

func (a algCrc32c) SumPutObject(input *s3.PutObjectInput) *s3.PutObjectInput {
	if a.checksumType == types.ChecksumTypeFullObject {
		input.ChecksumCRC32C = aws.String(base64.StdEncoding.EncodeToString(a.Sum(nil)))
	}
	return input
}

func (a algCrc32c) SumCompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) *s3.CompleteMultipartUploadInput {
	if a.checksumType == types.ChecksumTypeFullObject {
		input.ChecksumCRC32C = aws.String(base64.StdEncoding.EncodeToString(a.Sum(nil)))
	}
	return input
}

func (a algCrc32c) HashUploadPart(data []byte, input *s3.UploadPartInput) *s3.UploadPartInput {
	a.composite.Reset()
	a.composite.Write(data)
	input.ChecksumCRC32C = aws.String(base64.StdEncoding.EncodeToString(a.composite.Sum(nil)))
	return input
}

func newCrc64nvme() HashS3 {
	return algCrc64nvme{
		base{
			Hash: crc64.New(crc64.MakeTable(0xAD93D23594C93659)),
		},
	}
}

type algCrc64nvme struct {
	base
}

func (a algCrc64nvme) SumPutObject(input *s3.PutObjectInput) *s3.PutObjectInput {
	input.ChecksumCRC64NVME = aws.String(base64.StdEncoding.EncodeToString(a.Sum(nil)))
	return input
}

func (a algCrc64nvme) SumCompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) *s3.CompleteMultipartUploadInput {
	input.ChecksumCRC64NVME = aws.String(base64.StdEncoding.EncodeToString(a.Sum(nil)))
	return input
}
