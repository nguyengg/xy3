package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
)

// DefaultBytes is equivalent to progressbar.DefaultBytes but with higher progressbar.OptionThrottle.
func DefaultBytes(maxBytes int64, description string, options ...progressbar.Option) *progressbar.ProgressBar {
	return progressbar.NewOptions64(maxBytes,
		append([]progressbar.Option{
			progressbar.OptionSetDescription(description),
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowBytes(true),
			progressbar.OptionSetWidth(10),
			progressbar.OptionThrottle(1 * time.Second),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() {
				_, _ = fmt.Fprint(os.Stderr, "\n")
			}),
			progressbar.OptionSpinnerType(14),
			progressbar.OptionFullWidth(),
			progressbar.OptionSetRenderBlankState(true)},
			options...)...)
}
