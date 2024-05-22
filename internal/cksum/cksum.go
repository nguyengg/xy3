package cksum

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"strings"
)

type Hasher struct {
	w      hash.Hash
	prefix string
}

func NewHasher() *Hasher {
	return &Hasher{
		w:      sha256.New(),
		prefix: "sha256-",
	}
}

func NewFromChecksumString(v string) (*Hasher, error) {
	switch {
	case strings.HasPrefix(v, "sha384-"):
		return &Hasher{
			w:      sha512.New384(),
			prefix: "sha384-",
		}, nil
	case strings.HasPrefix(v, "sha256-"):
		return &Hasher{
			w:      sha256.New(),
			prefix: "sha256-",
		}, nil
	case v == "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown checksum algorithm")
	}
}

func (h Hasher) Write(p []byte) (n int, err error) {
	return h.w.Write(p)
}

// SumToChecksumString sums and encodes the checksum as a string that can be used as a subresource integrity.
//
// See https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity for format.
func (h Hasher) SumToChecksumString(b []byte) string {
	return h.prefix + base64.StdEncoding.EncodeToString(h.w.Sum(b))
}
