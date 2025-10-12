package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jessevdk/go-flags"
)

// Prefix creates a consistent prefix for all file-based commands to use.
//
// i and n are the zero-based ordinal and expected count.
func Prefix(i, n int, name flags.Filename) string {
	return fmt.Sprintf(`[%d/%d] "%s" - `, i, n, TruncateRightWithSuffix(filepath.Base(string(name)), 30, "..."))
}

type prefixKey struct{}
type loggerKey struct{}

// WithPrefixLogger creates a new logger using the given prefix, then attaches both the logger and prefix to context.
func WithPrefixLogger(ctx context.Context, prefix string) context.Context {
	logger := log.New(os.Stderr, prefix, 0)
	return context.WithValue(context.WithValue(ctx, prefixKey{}, prefix), loggerKey{}, logger)
}

// MustPrefix returns the prefix string attached to the given context.
func MustPrefix(ctx context.Context) string {
	return ctx.Value(prefixKey{}).(string)
}

// MustLogger returns the logger attached to the given context.
func MustLogger(ctx context.Context) *log.Logger {
	return ctx.Value(loggerKey{}).(*log.Logger)
}
