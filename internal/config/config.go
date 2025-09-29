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

	if k := sec.Key("expected-bucket-owner"); k != nil {
		c.ExpectedBucketOwner = aws.String(k.Value())
	}
	if k := sec.Key("storage-class"); k != nil {
		c.StorageClass = types.StorageClass(k.Value())
	}

	return
}

// ForBucket calls Loader.ForBucket on the DefaultLoader instance.
func ForBucket(bucket string) (c BucketConfig) {
	return DefaultLoader.ForBucket(bucket)
}
