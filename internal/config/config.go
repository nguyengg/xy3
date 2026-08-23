package config

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// UploadConfig contains upload configurations.
type UploadConfig struct {
	Bucket string
	Prefix string
}

// ForUpload returns configuration for upload.
func (l *Loader) ForUpload() (c UploadConfig) {
	sec, err := l.cfg.GetSection("upload")
	if err != nil {
		return c
	}

	c.Bucket = sec.Key("bucket").Value()
	c.Prefix = sec.Key("prefix").Value()

	return
}

// ForUpload calls Loader.ForUpload on the DefaultLoader instance.
func ForUpload() (c UploadConfig) {
	return DefaultLoader.ForUpload()
}

// BucketConfig contains configuration settings for a specific bucket.
type BucketConfig struct {
	Bucket              string
	AWSProfile          string
	ExpectedBucketOwner *string
	StorageClass        types.StorageClass
}

// ForBucket returns configuration for a specific bucket.
func (l *Loader) ForBucket(bucket string) (c BucketConfig) {
	sec, err := l.cfg.GetSection("s3://" + bucket)
	if err != nil {
		return c
	}

	c.Bucket = bucket

	c.AWSProfile = sec.Key("aws-profile").Value()

	// go-ini's Section.Key auto-creates missing keys, so we cannot check for nil to detect absence.
	// gate on the value being non-empty instead, otherwise a spurious aws.String("") is sent to S3.
	if v := sec.Key("expected-bucket-owner").Value(); v != "" {
		c.ExpectedBucketOwner = aws.String(v)
	}
	if v := sec.Key("storage-class").Value(); v != "" {
		c.StorageClass = types.StorageClass(v)
	}

	return
}

// ForBucket calls Loader.ForBucket on the DefaultLoader instance.
func ForBucket(bucket string) (c BucketConfig) {
	return DefaultLoader.ForBucket(bucket)
}
