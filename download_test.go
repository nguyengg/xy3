package xy3

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrChecksumMismatch_ErrorsAs(t *testing.T) {
	var target *ErrChecksumMismatch
	assert.True(t, errors.As(&ErrChecksumMismatch{
		Expected: "hello",
		Actual:   "world",
	}, &target))
}
