package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Manifest contains the bucket, key, and additional metadata about the file that has been uploaded to S3.
type Manifest struct {
	Bucket              string  `json:"bucket"`
	Key                 string  `json:"key"`
	ExpectedBucketOwner *string `json:"expectedBucketOwner,omitempty"`
	Size                int64   `json:"size,omitempty"`
	Checksum            string  `json:"checksum,omitempty"`
}

// LoadManifestFromFile reads and returns a manifest from a file with the specified name.
func LoadManifestFromFile(name string) (m Manifest, err error) {
	var f *os.File
	if f, err = os.Open(name); err != nil {
		return m, fmt.Errorf(`open file "%s" error: %w`, name, err)
	}

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err, _ = dec.Decode(&m), f.Close(); err != nil {
		return m, fmt.Errorf("unmarshal manifest error: %w", err)
	}

	return m, nil
}

func (m *Manifest) SaveTo(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("save manifest error: %w", err)
	}

	return nil
}
