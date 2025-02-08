package namedhash

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"strings"
)

// NamedHash is a hash.Hash with a name that can be used to prefix the string [hash.Hash.Sum].
//
// For example, [sha256] hashes would show up as "sha256-abcd".
//
// The zero value is ready for use with [sha256] as the hash.Hash.
type NamedHash struct {
	hash.Hash

	// Name is used to prefix SumToString in format "name-encodedSum".
	//
	// If Name is nil, no prefix will be added and SumToString will only return the encoded sum. Do not include the
	// `-` in the Name; SumToString will automatically add this rune.
	Name string

	// EncodeToString controls how SumToString encodes [hash.Hash.Sum] data.
	//
	// By default, [base64.StdEncoding.EncodeToString] is used.
	EncodeToString func([]byte) string
}

// NewFromChecksumString detects the prefix of the checksum string
func NewFromChecksumString(v string) (*NamedHash, error) {
	switch {
	case strings.HasPrefix(v, "sha384-"):
		return &NamedHash{
			Hash: sha512.New384(),
			Name: "sha384",
		}, nil
	case strings.HasPrefix(v, "sha256-"):
		return &NamedHash{
			Hash: sha256.New(),
			Name: "sha256",
		}, nil
	case strings.HasPrefix(v, "sha1-"):
		return &NamedHash{
			Hash: sha1.New(),
			Name: "sha1",
		}, nil
	case v == "":
		return nil, fmt.Errorf("empty checksum string")
	default:
		return nil, fmt.Errorf("unknown checksum algorithm")
	}
}

// SumToString sums and encodes the checksum as a string that can be used as a subresource integrity.
//
// See https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity for format.
//
// If Name is not given a non-nil value, no prefix will be added.
func (h *NamedHash) SumToString(b []byte) string {
	f := h.EncodeToString
	if f == nil {
		f = base64.StdEncoding.EncodeToString
	}

	if h.Name == "" {
		return f(h.Sum(b))
	}

	return h.Name + "-" + f(h.Sum(b))
}
