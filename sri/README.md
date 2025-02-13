# Subresource Integrity computation and verification

[![Go Reference](https://pkg.go.dev/badge/github.com/nguyengg/xy3.svg)](https://pkg.go.dev/github.com/nguyengg/xy3/sri)

This module provides `hash.Hash` implementation that can be used to compute and verify subresource integrity digests.
See https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity for an application of this.

## Generating SRI hashes

```go
package main

import (
	"os"

	"github.com/nguyengg/xy3/sri"
)

func main() {
	// create a new SHA-256 hash function.
	h := sri.NewSha256()

	// h implements hash.Hash which implements io.Writer so just pipes an entire file to it.
	f, _ := os.Open("path/to/file")
	_, _ = f.WriteTo(h)
	_ = f.Close()

	// SumToString will produce a digest in format sha256-aOZWslHmfoNYvvhIOrDVHGYZ8+ehqfDnWDjUH/No9yg for example.
	h.SumToString(nil)
}

```

## Verifying SRI hashes

```go
package main

import (
	"log"
	"os"
	"strings"

	"github.com/nguyengg/xy3/sri"
)

func main() {
	// per https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity#using_subresource_integrity,
	// an integrity value may contain multiple hashes separated by whitespace.
	//
	// NewVerifier accepts a number of hashes that the returned hash.Hash can verify against.
	//
	// unknown is a string slice containing hashes with unknown hash function. This module only supports SHA hash
	// functions out of the boxes; use Register to register more.
	h, unknown := sri.NewVerifier(
		"sha256-aOZWslHmfoNYvvhIOrDVHGYZ8+ehqfDnWDjUH/No9yg",
		"sha384-b58jhCXsokOe1Fgawf20X8djeef7qUvAp2JPo+erHsNwG0v83aN2ynVRkub0XypO",
		"sha512-bCYYNY2gfIMLiMWvjDU1CA6OYDyIuJECiiWczbmsgC0PwBcMmdWK/88AeGzhiPxddT6MZiivIHHDJw1QRFxLHA")
	if len(unknown) > 0 {
		log.Printf("unknown hash functions: %s", strings.Join(unknown, " "))
	}

	// h, once again, implements hash.Hash which implements io.Writer so just pipes an entire file to it.
	f, _ := os.Open("path/to/file")
	_, _ = f.WriteTo(h)
	_ = f.Close()

	// SumAndVerify will return true if and only if the resulting hash matches against one of the hashes given by
	// NewVerifier.
	if matches := h.SumAndVerify(nil); matches {
		// integrity matches! the file should be accepted.
	}
}

```
