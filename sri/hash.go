package sri

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash"
	"strings"
	"sync"
)

// Hash extends hash.Hash with SumToString to generate the base64-encoded cryptographic hash that can be used to verify
// [Subresource Integrity].
//
// [Subresource Integrity]: https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity
type Hash interface {
	hash.Hash

	// Name returns the name of the hash function.
	Name() string

	// SumToString calls [hash.Hash.Sum] passing b and encodes the returned slice as a string prefixed with the hash
	// name.
	//
	// See [Subresource Integrity] for example usages of such strings in <script> and <link> tags such as
	//  <script
	//   src="https://example.com/example-framework.js"
	//   integrity="sha384-oqVuAfXRKap7fdgcCY5uykM6+R9GqQ8K/uxy9rx7HNQlGYl1kPzQho1wx4JwY8wC"
	//   crossorigin="anonymous"></script>
	//
	// [Subresource Integrity]: https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity
	SumToString(b []byte) string
}

// NewSha1 returns a new Hash using sha1 as the hash function.
func NewSha1() Hash {
	return &hasher{Hash: sha1.New(), name: "sha1"}
}

// NewSha224 returns a new Hash using sha224 as the hash function.
func NewSha224() Hash {
	return &hasher{Hash: sha512.New512_224(), name: "sha224"}
}

// NewSha256 returns a new Hash using sha256 as the hash function.
func NewSha256() Hash {
	return &hasher{Hash: sha256.New(), name: "sha256"}
}

// NewSha384 returns a new Hash using sha384 as the hash function.
func NewSha384() Hash {
	return &hasher{Hash: sha512.New384(), name: "sha384"}
}

// NewSha512 returns a new Hash using sha512 as the hash function.
func NewSha512() Hash {
	return &hasher{Hash: sha512.New(), name: "sha512"}
}

func parse(digest string) (h Hash, name string) {
	values := strings.SplitN(digest, "-", 2)

	switch name = values[0]; name {
	case "sha1":
		h = NewSha1()
	case "sha224":
		h = NewSha224()
	case "sha256":
		h = NewSha256()
	case "sha384":
		h = NewSha384()
	case "sha512":
		h = NewSha512()
	default:
		if fn, ok := customHashers.Load(name); ok {
			h = &hasher{
				Hash: fn.(func() hash.Hash)(),
				name: name,
			}
		}
	}

	return
}

var customHashers sync.Map

// Register can be used to register additional hash functions not supported out of the box.
func Register(name string, hashNewFn func() hash.Hash) {
	customHashers.Store(name, hashNewFn())
}

// hasher implements Hash.
type hasher struct {
	hash.Hash
	name string
}

func (h *hasher) Name() string {
	return h.name
}

func (h *hasher) SumToString(b []byte) string {
	return h.name + "-" + base64.RawStdEncoding.EncodeToString(h.Sum(b))
}

var _ Hash = (*hasher)(nil)
