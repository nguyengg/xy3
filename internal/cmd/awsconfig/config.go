package awsconfig

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type ConfigLoader interface {
	AddOption(optFn func(*config.LoadOptions) error)
}

type ConfigLoaderMixin struct {
	optFns []func(*config.LoadOptions) error
}

func (c *ConfigLoaderMixin) AddOption(optFn func(*config.LoadOptions) error) {
	c.optFns = append(c.optFns, optFn)
}

func (c *ConfigLoaderMixin) LoadDefaultConfig(ctx context.Context, optFns ...func(*config.LoadOptions) error) (aws.Config, error) {
	return config.LoadDefaultConfig(ctx, append(c.optFns, optFns...)...)
}
