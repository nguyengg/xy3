package sri

import (
	"crypto/subtle"
	"io"
)

// DigestVerifier extends Hash with SumAndVerify to verify the hash against a set of precomputed digests.
//
// [Using Subresource Integrity] allow for multiple digests to be given as match candidates. As a result, NewVerifier
// supports being given several precomputed digests to match against. If the precomputed digests use different hash
// functions, the function of the first digest will be the one that is used as the primary Hash function.
//
// [Using Subresource Integrity]: https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity#using_subresource_integrity
type DigestVerifier interface {
	Hash

	// SumAndVerify calls [hash.Hash.Sum] passing b and matches the resulting slice against the original set of
	// candidates.
	//
	// SumAndVerify returns true if and only if the hash matches at least one match candidate.
	SumAndVerify(b []byte) bool
}

// NewVerifier returns a new DigestVerifier that will match against the given set of digest candidates.
//
// The hash function of the first (primary) digest will be used as the primary hash function. Digests with unknown hash
// function are returned as the second value. If all digests are unrecognised, a nil DigestVerifier is returned. Call
// Register if you are expecting custom hash functions.
func NewVerifier(primary string, additional ...string) (DigestVerifier, []string) {
	writers := make([]io.Writer, 0)
	hashes := make([]Hash, 0)
	digests := make(map[string][]string)
	unknown := make([]string, 0)

	if h, name := parse(primary); h != nil {
		writers = append(writers, h)
		hashes = append(hashes, h)
		digests[name] = append(digests[name], primary)
	} else {
		unknown = append(unknown, primary)
	}

	for _, d := range additional {
		if h, name := parse(d); h != nil {
			writers = append(writers, h)
			hashes = append(hashes, h)
			digests[name] = append(digests[name], d)
		} else {
			unknown = append(unknown, d)
		}
	}

	if len(hashes) == 0 {
		return nil, unknown
	}

	return &verifier{
		Writer:  io.MultiWriter(writers...),
		hashes:  hashes,
		digests: digests,
	}, unknown
}

type verifier struct {
	io.Writer
	hashes  []Hash
	digests map[string][]string
}

func (v *verifier) Sum(b []byte) []byte {
	return v.hashes[0].Sum(b)
}

func (v *verifier) Reset() {
	for _, h := range v.hashes {
		h.Reset()
	}
}

func (v *verifier) Size() int {
	return v.hashes[0].Size()
}

func (v *verifier) BlockSize() int {
	return v.hashes[0].BlockSize()
}

func (v *verifier) Name() string {
	return v.hashes[0].Name()
}

func (v *verifier) SumToString(b []byte) string {
	return v.hashes[0].SumToString(b)
}

func (v *verifier) SumAndVerify(b []byte) bool {
	h := v.hashes[0]
	d := h.SumToString(b)
	for _, c := range v.digests[h.Name()] {
		if subtle.ConstantTimeCompare([]byte(d), []byte(c)) == 1 {
			return true
		}
	}

	for _, h = range v.hashes[1:] {
		d = h.SumToString(nil)
		for _, c := range v.digests[h.Name()] {
			if subtle.ConstantTimeCompare([]byte(d), []byte(c)) == 1 {
				return true
			}
		}
	}

	return false
}
