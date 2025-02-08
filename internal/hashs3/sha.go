package hashs3

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newSha1() HashS3 {
	return algSha1{base{Hash: sha1.New(), composite: sha1.New()}}
}

type algSha1 struct {
	base
}

func (a algSha1) HashUploadPart(data []byte, input *s3.UploadPartInput) *s3.UploadPartInput {
	a.composite.Reset()
	a.composite.Write(data)
	input.ChecksumSHA1 = aws.String(base64.StdEncoding.EncodeToString(a.composite.Sum(nil)))
	return input
}

func newSha256() HashS3 {
	return algSha256{base{Hash: sha256.New(), composite: sha256.New()}}
}

type algSha256 struct {
	base
}

func (a algSha256) HashUploadPart(data []byte, input *s3.UploadPartInput) *s3.UploadPartInput {
	a.composite.Reset()
	a.composite.Write(data)
	input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(a.composite.Sum(nil)))
	return input
}
