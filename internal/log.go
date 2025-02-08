package internal

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jessevdk/go-flags"
)

// NewLogger creates a new logger with a prefix set.
func NewLogger(i, n int, name flags.Filename) *log.Logger {
	return log.New(os.Stderr, fmt.Sprintf(`[%d/%d] "%s" - `, i+1, n, filepath.Base(string(name))), 0)
}
