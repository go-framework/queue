package queue

import (
	"context"
)

// Queue interface.
type Queuer interface {
	// Init with context.
	Init(ctx context.Context) error
	// Push data into queue.
	Push(data interface{}) error
	// Pop data from queue with context.
	Pop(ctx context.Context) (data interface{}, err error)
	// Remove data from queue.
	Remove(data interface{}) error
	// Size of queuer.
	Size() int64
}

// Assistant interface.
type Assistanter interface {
	// New data object.
	NewData() Dataer
	// Assistant done with context and dataer.
	Done(context.Context, Dataer) error
}

// Assistant data interface.
type Dataer interface {
	// Data name.
	Name() string
	// Data marshal.
	Marshal() ([]byte, error)
	// Unmarshal with bytes.
	Unmarshal([]byte) error
}
