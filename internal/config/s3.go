package config

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (l *Loader) NewS3Client(ctx context.Context, optFns ...func(*s3.Options)) (*s3.Client, error) {
	if c, ok := l.s3clientCache.Load("s3"); ok {
		return c.(*s3.Client), nil
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(l.Profile))
	if err != nil {
		return nil, err
	}

	c := s3.NewFromConfig(cfg, optFns...)
	l.s3clientCache.Store("s3", c)
	return c, nil
}

func NewS3Client(ctx context.Context, optFns ...func(*s3.Options)) (*s3.Client, error) {
	return DefaultLoader.NewS3Client(ctx, optFns...)
}

func (l *Loader) NewS3ClientForBucket(ctx context.Context, bucket string, optFns ...func(*s3.Options)) (*s3.Client, error) {
	key := "s3://" + bucket
	if c, ok := l.s3clientCache.Load(key); ok {
		return c.(*s3.Client), nil
	}

	cfg, err := config.LoadDefaultConfig(ctx, func(opts *config.LoadOptions) error {
		if l.Profile != "" {
			opts.SharedConfigProfile = l.Profile
			return nil
		}

		opts.SharedConfigProfile = l.ForBucket(bucket).AWSProfile
		return nil
	})
	if err != nil {
		return nil, err
	}

	c := s3.NewFromConfig(cfg, optFns...)
	l.s3clientCache.Store(key, c)
	return c, nil

}

func NewS3ClientForBucket(ctx context.Context, bucket string, optFns ...func(*s3.Options)) (*s3.Client, error) {
	return DefaultLoader.NewS3ClientForBucket(ctx, bucket, optFns...)
}
