package sri

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVerify(t *testing.T) {
	data := []byte("hello, world!")

	// these random digests will be mixed in with the real digest.
	randomDigests := make([]string, 0)
	for _, fn := range []func() Hash{NewSha1, NewSha224, NewSha256, NewSha384, NewSha512} {
		h := fn()
		_, _ = h.Write([]byte("i'm a teapot"))
		randomDigests = append(randomDigests, h.SumToString(nil))
	}

	tests := []struct {
		name   string
		digest string
	}{
		{
			name:   "sha1",
			digest: "sha1-HwnTDHB9U/PRbFMN1z1wps51lqk",
		},
		{
			name:   "sha224",
			digest: "sha224-VE4THAfnVPg51auGzUqG0CViujIkCu/DTJpDAg",
		},
		{
			name:   "sha256",
			digest: "sha256-aOZWslHmfoNYvvhIOrDVHGYZ8+ehqfDnWDjUH/No9yg",
		},
		{
			name:   "sha384",
			digest: "sha384-b58jhCXsokOe1Fgawf20X8djeef7qUvAp2JPo+erHsNwG0v83aN2ynVRkub0XypO",
		},
		{
			name:   "sha512",
			digest: "sha512-bCYYNY2gfIMLiMWvjDU1CA6OYDyIuJECiiWczbmsgC0PwBcMmdWK/88AeGzhiPxddT6MZiivIHHDJw1QRFxLHA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			digests := append([]string{tt.digest}, randomDigests...)
			rand.Shuffle(len(digests), func(i, j int) {
				digests[i], digests[j] = digests[j], digests[i]
			})

			v, _ := NewVerifier(digests[0], digests[1:]...)
			_, _ = v.Write(data)
			assert.True(t, v.SumAndVerify(nil))
		})
	}
}
