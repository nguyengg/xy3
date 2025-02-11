package s3reader

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	randv2 "math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

// testReaderClient implements GetAndHeadObjectClient by slicing into its in-memory data.
//
// calls keeps track of GetObject input parameters for asserting.
type testReaderClient struct {
	data []byte

	// mu guards write access to calls.
	mu    sync.Mutex
	calls []s3.GetObjectInput
}

func randomTestReaderClient(n int) *testReaderClient {
	data := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		panic(err)
	}

	return &testReaderClient{
		data:  data,
		calls: make([]s3.GetObjectInput, 0),
	}
}

func (c *testReaderClient) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = make([]s3.GetObjectInput, 0)
}

func (c *testReaderClient) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.mu.Lock()
	c.calls = append(c.calls, *input)
	c.mu.Unlock()

	rangeBytes := aws.ToString(input.Range)
	if rangeBytes == "" {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader(c.data)),
		}, nil
	}

	switch values := strings.SplitN(strings.TrimPrefix(aws.ToString(input.Range), "bytes="), "-", 2); len(values) {
	case 2:
		i, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid start byte in range `%s`: %w", rangeBytes, err)
		}

		if values[1] == "" {
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(c.data[i:])),
			}, nil
		}

		j, err := strconv.ParseInt(values[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid end byte in range `%s`: %w", rangeBytes, err)
		}

		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader(c.data[i : j+1])),
		}, nil
	default:
		return nil, fmt.Errorf("invalid range: %s", rangeBytes)
	}
}

func (c *testReaderClient) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(c.data))),
	}, nil
}

func TestReader_Read(t *testing.T) {
	ctx := context.Background()
	tc := randomTestReaderClient(1024)
	r, err := New(ctx, tc, &s3.GetObjectInput{})
	assert.NoErrorf(t, err, "New(...) error = %v", err)

	// a single read to get all data.
	buf := make([]byte, len(tc.data))
	assertReadEqual(t, r, buf, tc.data)
	assert.Equalf(t, 1, len(tc.calls), "Read(buf) should have made only 1 GetObject call; got %d", len(tc.calls))

	// attempting to read past EOF is safe due to clamping.
	tc.clear()
	n, err := r.Read(buf)
	assert.Equalf(t, err, io.EOF, "Read(buf) error should be io.EOF; got %v", err)
	assert.Equalf(t, 0, n, "Read(buf) should have returned 0 bytes; got %d", n)
	assert.Equalf(t, 0, len(tc.calls), "Read(buf) should not have made any GetObject calls; got %d", len(tc.calls))

	// read only the first 100 bytes, then next 100 bytes.
	// because bufferSize is larger than 200 bytes, there ends up being only 1 GetObject call.
	r = r.Reopen()
	r.(*reader).bufferSize = 200
	buf = make([]byte, 100)
	assertReadEqual(t, r, buf, tc.data[:100])
	assertReadEqual(t, r, buf, tc.data[100:200])
	assert.Equalf(t, 1, len(tc.calls), "Read(buf) should have made only 1 GetObject call; got %d", len(tc.calls))

	// now because bufferSize is small, the same test above will produce 2 GetObject calls.
	tc.clear()
	r.(*reader).bufferSize = 10
	assertReadEqual(t, r, buf, tc.data[200:300])
	assertReadEqual(t, r, buf, tc.data[300:400])
	assert.Equalf(t, 2, len(tc.calls), "Read(buf) should have made 2 GetObject calsl; got %d", len(tc.calls))
}

func TestReader_ReadAt(t *testing.T) {
	ctx := context.Background()
	tc := randomTestReaderClient(1024)
	r, err := NewReaderWithSize(ctx, tc, &s3.GetObjectInput{}, int64(len(tc.data)))
	assert.NoErrorf(t, err, "NewReaderWithSize(...) error = %v", err)

	// a simple offset read.
	buf := make([]byte, 100)
	n, err := r.ReadAt(buf, 42)
	assert.NoErrorf(t, err, "ReadAt(buf, 42) error = %v", err)
	assert.Equalf(t, len(buf), n, "Read(buf, 42) returns only %d bytes; expected %d", n, len(tc.data))
	assert.Equal(t, tc.data[42:42+100], buf)
	assert.Equalf(t, 1, len(tc.calls), "Read(buf) should have made only 1 GetObject call; got %d", len(tc.calls))
	assert.Equalf(t, r.(*reader).off, int64(0), "offset should not have advanced; got %d", r.(*reader).off)

	// attempting to read past EOF is safe due to clamping.
	tc.clear()
	n, err = r.ReadAt(buf, 1020)
	assert.NoErrorf(t, err, "ReadAt(buf, 1020) error = %v", err)
	assert.Equalf(t, 4, n, "Read(buf, 42) returns %d bytes; expected 4", n)
	assert.Equal(t, tc.data[1020:], buf[:4])
	assert.Equalf(t, 1, len(tc.calls), "Read(buf) should have made only 1 GetObject call; got %d", len(tc.calls))
}

func TestReader_WriteTo(t *testing.T) {
	ctx := context.Background()
	tc := randomTestReaderClient(1024)
	o, err := New(ctx, tc, &s3.GetObjectInput{})
	assert.NoErrorf(t, err, "New(...) error = %v", err)

	var buf bytes.Buffer

	// read all 1204 bytes.
	n, err := o.WriteTo(&buf)
	assert.NoErrorf(t, err, "Write(&buf) error = %v", err)
	assert.Equalf(t, int64(len(tc.data)), n, "Write(&buf) should have written %d bytes; got %d", len(tc.data), n)

	// let's do an offset write to read only the last 500 bytes.
	buf.Reset()
	n, err = o.Seek(-500, io.SeekEnd)
	assert.NoErrorf(t, err, "Seek(-500, io.SeekEnd) error = %v", err)
	assert.Equalf(t, int64(524), n, "Seek(-500, io.SeekEnd) should have set offset to %d; got %d", 524, n)
	n, err = o.WriteTo(&buf)
	assert.NoErrorf(t, err, "Write(&buf) error = %v", err)
	assert.Equalf(t, int64(500), n, "Write(&buf) should have written 500 bytes; got %d", n)
}

func TestReader_ReadLessThanThreshold(t *testing.T) {
	ctx := context.Background()
	tc := randomTestReaderClient(64)
	o, err := New(ctx, tc, &s3.GetObjectInput{})
	assert.NoErrorf(t, err, "New(...) error = %v", err)

	// threshold is larger than the amount of data being requested so only one call.
	buf := make([]byte, len(tc.data))
	o.(*reader).threshold = int64(len(tc.data)) + 1
	assertReadEqual(t, o, buf, tc.data)
	assert.Equalf(t, 1, len(tc.calls), "Read(buf) should have made only 1 GetObject call; got %d", len(tc.calls))
}

func TestReader_ReadParallel(t *testing.T) {
	for i := range 10 {
		t.Run(fmt.Sprintf("TestRead_Parallel_%d", i), func(t *testing.T) {
			ctx := context.Background()
			tc := randomTestReaderClient(1024)
			o, err := New(ctx, tc, &s3.GetObjectInput{})
			assert.NoErrorf(t, err, "New(...) error = %v", err)

			var (
				// threshold is small enough to kick in parallel get.
				threshold int64 = 10
				// partSize to control the expected number of GETs.
				partSize = 50 + randv2.Int64N(150)
				// there should be math.Ceil(1024/partSize) GetObject calls.
				expectedCallCount = int(math.Ceil(float64(len(tc.data)) / float64(partSize)))
			)
			if opts, ok := o.(*reader); ok {
				opts.threshold = threshold
				opts.partSize = partSize
			}

			buf := make([]byte, len(tc.data))
			assertReadEqual(t, o, buf, tc.data)
			assert.Equalf(t, expectedCallCount, len(tc.calls), "Read(buf) should have made %d GetObject call; got %d", expectedCallCount, len(tc.calls))
		})
	}
}

func assertReadEqual(t *testing.T, src io.Reader, dst []byte, expected []byte) {
	n, err := src.Read(dst)
	assert.NoErrorf(t, err, "Read error = %v", err)
	assert.Equalf(t, len(dst), n, "Read returns only %d bytes; expected %d", n, len(dst))
	assert.Equal(t, expected, dst, "Read returns data not equal expected")
}
