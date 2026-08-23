package xy3

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
)

// mockS3 is a hermetic *s3.Client fake built via aws-sdk-go-v2 middleware injection.
//
// Instead of hitting the network, it registers an initialise-phase middleware that short-circuits
// the SDK stack: it matches the in-flight request by middleware.GetOperationName and returns a
// caller-supplied canned output (or error) for that operation. Running at the initialise phase means
// the handler receives the original TYPED operation input (in.Parameters), and terminating there
// means no serialisation and no HTTP transport ever run, so the whole suite stays offline.
type mockS3 struct {
	mu sync.Mutex

	// handlers maps an S3 operation name (e.g. "PutObject") to a function producing its output.
	// The handler is passed the typed operation input (e.g. *s3.HeadObjectInput).
	handlers map[string]func(input any) (any, error)

	// calls records the operation name of every request the SDK issued, in order, so tests can
	// assert that (for example) no PutObject happened when validation should have failed first.
	calls []string
}

// newMockS3 returns an empty mock. Register operations with on before building the client.
func newMockS3() *mockS3 {
	return &mockS3{handlers: make(map[string]func(input any) (any, error))}
}

// on registers a handler for the named S3 operation. The handler receives the typed operation input
// and returns the typed output value plus an error.
func (m *mockS3) on(operation string, fn func(input any) (any, error)) *mockS3 {
	m.handlers[operation] = fn
	return m
}

// recorded returns the ordered list of operation names the SDK dispatched through the mock.
func (m *mockS3) recorded() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.calls))
	copy(out, m.calls)
	return out
}

// count returns how many times the named operation was dispatched.
func (m *mockS3) count(operation string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c == operation {
			n++
		}
	}
	return n
}

// client builds an *s3.Client wired to this mock. Credentials are static dummies and the region is
// fixed; nothing leaves the process because the injected middleware terminates the stack at the
// initialise phase, before the HTTP client is ever invoked.
func (m *mockS3) client(t *testing.T) *s3.Client {
	t.Helper()

	return s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  aws.AnonymousCredentials{},
		UsePathStyle: true,
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				return stack.Initialize.Add( //nolint:misspell // SDK phase name, not prose
					m.middleware(),
					middleware.Before,
				)
			},
		},
	})
}

func (m *mockS3) middleware() middleware.InitializeMiddleware {
	return middleware.InitializeMiddlewareFunc(
		"mockS3",
		func(ctx context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (
			out middleware.InitializeOutput, md middleware.Metadata, err error,
		) {
			op := middleware.GetOperationName(ctx)

			m.mu.Lock()
			m.calls = append(m.calls, op)
			handler, ok := m.handlers[op]
			m.mu.Unlock()

			if !ok {
				return out, md, fmt.Errorf("mockS3: no handler registered for operation %q", op)
			}

			result, herr := handler(in.Parameters)
			if herr != nil {
				return out, md, herr
			}

			out.Result = result
			return out, md, nil
		},
	)
}

// apiStatusError is a minimal error standing in for an S3 API failure with a given HTTP status.
// The Download code path only needs the error to propagate and be wrapped with "head object error",
// so a plain typed error suffices; we do not need the SDK's smithy response-error machinery here.
type apiStatusError struct {
	status int
	op     string
}

func (e *apiStatusError) Error() string {
	return fmt.Sprintf("api error %d on %s", e.status, e.op)
}

// httpStatusError builds an error representing an S3 operation failing with the given HTTP status.
func httpStatusError(op string, status int) error {
	return &apiStatusError{status: status, op: op}
}

// readCloser wraps a string body as an io.ReadCloser for GetObject outputs.
func readCloser(body string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(body))
}
