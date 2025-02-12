package executor

import (
	"errors"
)

// Executor is inspired by Java Executor that abstracts submitting a task and executing it.
type Executor interface {
	// Execute executes the given command.
	//
	// The return error comes from the executor's attempt to run the function, not from the function itself.
	Execute(func()) error
}

// ExecuteCloser adds io.Closer to Executor.
type ExecuteCloser interface {
	Executor

	// Close signals the goroutines in this executor to stop.
	Close() error
}

// ErrFullExecutor is returned by the executor created by NewAbortOnFullExecutor if there are no goroutines available
// to run a task.
var ErrFullExecutor = errors.New("pool has no idle goroutine")

// NewAbortOnFullExecutor returns a new Executor that will return NewAbortOnFullExecutor if a new pool of n goroutines
// has no idle worker to pick up a task.
//
// Passing n == 0 effectively an executor that always returns NewAbortOnFullExecutor. Panics if n < 0.
func NewAbortOnFullExecutor(n int) ExecuteCloser {
	if n == 0 {
		return &abortExecutor{}
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

	return &abortOnFullExecutor{inputs: inputs}
}

type abortOnFullExecutor struct {
	inputs chan func()
}

func (ex *abortOnFullExecutor) Execute(f func()) error {
	// TODO if there are concurrent calls to Close and Execute, sending to inputs may panic.
	// see if this can be fixed.
	select {
	case ex.inputs <- f:
	default:
		return ErrFullExecutor
	}

	return nil
}

func (ex *abortOnFullExecutor) Close() error {
	close(ex.inputs)
	return nil
}

type abortExecutor struct {
}

func (ex abortExecutor) Execute(_ func()) error {
	return ErrFullExecutor
}

func (ex abortExecutor) Close() error {
	return nil
}

// NewCallerRunsOnFullExecutor returns a new Executor that will execute the command on same goroutine as caller if a
// new pool of n goroutines is full.
//
// Passing n == 0 effectively returns an always-caller-run executor. Panics if n < 0.
func NewCallerRunsOnFullExecutor(n int) ExecuteCloser {
	if n == 0 {
		return &callerRunsExecutor{}
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

	return &callerRunsOnFullExecutor{inputs: inputs}
}

type callerRunsOnFullExecutor struct {
	inputs chan func()
}

func (ex *callerRunsOnFullExecutor) Execute(f func()) error {
	// TODO if there are concurrent calls to Close and Execute, sending to inputs may panic.
	// see if this can be fixed.
	select {
	case ex.inputs <- f:
	default:
		f()
	}

	return nil
}

func (ex *callerRunsOnFullExecutor) Close() error {
	close(ex.inputs)
	return nil
}

type callerRunsExecutor struct {
}

func (ex callerRunsExecutor) Execute(f func()) error {
	f()
	return nil
}

func (ex callerRunsExecutor) Close() error {
	return nil
}
