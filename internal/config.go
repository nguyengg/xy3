package internal

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-ini/ini"
)

var cfg *ini.File

func init() {
	dir, err := os.UserHomeDir()
	if err != nil {
		log.Printf("get user home dir error: %v", err)
		cfg = ini.Empty()
		return
	}

	cfg, err = ini.Load(filepath.Join(dir, ".xy3", "config.ini"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("load config error: %v", err)
		}
		cfg = ini.Empty()
		return
	}
}

// BucketConfig contains configuration settings for a specific bucket.
type BucketConfig struct {
	Bucket              string
	AWSProfile          string
	ExpectedBucketOwner *string
	SecretId            *string
	StorageClass        types.StorageClass
}

var cfgCache sync.Map

// ConfigForBucket returns configuration for a specific bucket.
func ConfigForBucket(bucket string) (c BucketConfig) {
	if cache, ok := cfgCache.Load(bucket); ok {
		return cache.(BucketConfig)
	}

	sec, err := cfg.GetSection(bucket)
	if err != nil {
		return c
	}

	c.Bucket = bucket
	c.AWSProfile = sec.Key("aws-profile").Value()
	if k := sec.Key("expected-bucket-owner"); k != nil {
		c.ExpectedBucketOwner = aws.String(k.Value())
	}
	if k := sec.Key("secret-id"); k != nil {
		c.SecretId = aws.String(k.Value())
	}
	if k := sec.Key("storage-class"); k != nil {
		c.StorageClass = types.StorageClass(k.Value())
	}

	cfgCache.Store(bucket, c)
	return
}
