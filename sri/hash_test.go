package sri

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	data := []byte("hello, world!")

	tests := []struct {
		name     string
		fn       func() Hash
		expected string
	}{
		{
			name:     "sha1",
			fn:       NewSha1,
			expected: "sha1-HwnTDHB9U/PRbFMN1z1wps51lqk",
		},
		{
			name:     "sha224",
			fn:       NewSha224,
			expected: "sha224-VE4THAfnVPg51auGzUqG0CViujIkCu/DTJpDAg",
		},
		{
			name:     "sha256",
			fn:       NewSha256,
			expected: "sha256-aOZWslHmfoNYvvhIOrDVHGYZ8+ehqfDnWDjUH/No9yg",
		},
		{
			name:     "sha384",
			fn:       NewSha384,
			expected: "sha384-b58jhCXsokOe1Fgawf20X8djeef7qUvAp2JPo+erHsNwG0v83aN2ynVRkub0XypO",
		},
		{
			name:     "sha512",
			fn:       NewSha512,
			expected: "sha512-bCYYNY2gfIMLiMWvjDU1CA6OYDyIuJECiiWczbmsgC0PwBcMmdWK/88AeGzhiPxddT6MZiivIHHDJw1QRFxLHA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tt.fn()
			assert.Equal(t, tt.name, h.Name())

			_, _ = h.Write(data)
			assert.Equal(t, tt.expected, h.SumToString(nil))
		})
	}
}
