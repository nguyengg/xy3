package manifest

import (
	"encoding/json"
	"fmt"
	"io"
)

// Manifest contains the bucket, key, and expectedBucketOwner field.
type Manifest struct {
	Bucket              string  `json:"bucket"`
	Key                 string  `json:"key"`
	ExpectedBucketOwner *string `json:"expectedBucketOwner,omitempty"`
	Size                int64   `json:"size,omitempty"`
	Checksum            string  `json:"checksum,omitempty"`
}

func UnmarshalFrom(r io.Reader) (m Manifest, err error) {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		err = fmt.Errorf("unmarshal manifest error: %w", err)
	}

	return
}

func (m Manifest) MarshalTo(w io.Writer) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal manifest error: %w", err)
	}

	if _, err = w.Write(data); err != nil {
		return fmt.Errorf("write manifest error: %w", err)
	}

	return nil
}
