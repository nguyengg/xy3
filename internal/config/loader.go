package config

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-ini/ini"
)

// Loader can be used for loading .xy3 configuration as well as overridden with default settings.
type Loader struct {
	// Profile is the AWS profile to use, taking precedence over bucket-based AWS profile setting.
	Profile string

	cfg           *ini.File
	s3clientCache sync.Map
}

// Load will traverse the directory hierarchy upwards to find the first ".xy3" file available and load its contents
// into the Loader.
//
// The name of the .xy3 file is returned.
func (l *Loader) Load(ctx context.Context) (string, error) {
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

	l.cfg, err = ini.Load(path)
	if err != nil {
		l.cfg = ini.Empty()
		return path, err
	}

	return path, nil
}

// LoadProfile is a convenient method to set Loader.Profile then call Load.
func (l *Loader) LoadProfile(ctx context.Context, profile string) (string, error) {
	l.Profile = profile
	return l.Load(ctx)
}

// DefaultLoader is the default Loader instance for package-level methods.
var DefaultLoader = &Loader{cfg: ini.Empty()}

// Load calls Loader.Load on the DefaultLoader instance.
func Load(ctx context.Context) (string, error) {
	return DefaultLoader.Load(ctx)
}

// LoadProfile calls Loader.LoadProfile on the DefaultLoader instance.
func LoadProfile(ctx context.Context, profile string) (string, error) {
	return DefaultLoader.LoadProfile(ctx, profile)
}
