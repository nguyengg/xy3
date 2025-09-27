package config

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-ini/ini"
)

var cfg = ini.Empty()

// Load will traverse the directory hierarchy upwards to find the first ".xy3" file available.
func Load(ctx context.Context) (string, error) {
	var (
		path        = filepath.Join(".", ".xy3")
		fi          os.FileInfo
		err         error
		cur, parent string
	)

	if cur, err = os.Getwd(); err != nil {
		return "", err
	}

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		if fi, err = os.Stat(path); err == nil {
			if !fi.IsDir() {
				break
			}

			continue
		}

		if os.IsNotExist(err) {
			parent = filepath.Dir(cur)

			if parent == cur || parent == "." || parent == "/" {
				return "", nil
			}

			path = filepath.Join(parent, ".xy3")
			cur = parent
			continue
		}

		return "", err
	}

	cfg, err = ini.Load(path)
	if err != nil {
		cfg = ini.Empty()
		return path, err
	}

	return path, nil
}

// UploadConfig contains upload configurations.
type UploadConfig struct {
	Bucket string
	Prefix string
}

// BucketConfig contains configuration settings for a specific bucket.
type BucketConfig struct {
	Bucket              string
	AWSProfile          string
	ExpectedBucketOwner *string
	StorageClass        types.StorageClass
}

var cfgCache sync.Map

// ForUpload returns configuration for upload.
func ForUpload() (c UploadConfig) {
	if cache, ok := cfgCache.Load("upload"); ok {
		return cache.(UploadConfig)
	}

	sec, err := cfg.GetSection("upload")
	if err != nil {
		return c
	}

	c.Bucket = sec.Key("bucket").Value()
	c.Prefix = sec.Key("prefix").Value()

	cfgCache.Store("upload", c)
	return
}

// ForBucket returns configuration for a specific bucket.
func ForBucket(bucket string) (c BucketConfig) {
	if cache, ok := cfgCache.Load(bucket); ok {
		return cache.(BucketConfig)
	}

	sec, err := cfg.GetSection("s3://" + bucket)
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

	cfgCache.Store(bucket, c)
	return
}
