package executor

import (
	"io"
	"sync"
)

// Executor is inspired by Java Executor that abstracts submitting a task and executing it.
type Executor interface {
	// Execute executes the given command.
	Execute(func())
}

// ExecuteCloser adds io.Closer to Executor
type ExecuteCloser interface {
	Executor
	io.Closer
}

// NewCallerRunOnRejectExecutor returns a new Executor that will execute the command on same goroutine as caller if the
// pool is full.
func NewCallerRunOnRejectExecutor(n int) ExecuteCloser {
	if n == 0 {
		return &callerRunExecutor{}
	}

	inputs := make(chan func(), n)

	for range n {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					// if the channel is closed then exit here.
					if _, ok := <-inputs; !ok {
						return
					}
				}
			}()

			for f := range inputs {
				f()
			}
		}()
	}

	return &callerRunOnRejectExecutor{inputs: inputs}
}

type callerRunOnRejectExecutor struct {
	inputs chan func()

	// mu guards closed.
	mu     sync.Mutex
	closed bool
}

func (ex *callerRunOnRejectExecutor) Execute(f func()) {
	select {
	case ex.inputs <- f:
	default:
		f()
	}
}

func (ex *callerRunOnRejectExecutor) Close() error {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	close(ex.inputs)

	return nil
}

type callerRunExecutor struct {
}

func (ex callerRunExecutor) Execute(f func()) {
	f()
}

func (ex callerRunExecutor) Close() error {
	return nil
}
