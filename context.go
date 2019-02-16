package queue

import (
	"context"
	"time"
)

// duration context key.
type durationContextKey struct{}

// New duration context.
func NewDurationContext(ctx context.Context, duration time.Duration) context.Context {
	return context.WithValue(ctx, durationContextKey{}, duration)
}

// Get duration from context.
func DurationFromContext(ctx context.Context) (time.Duration, bool) {
	duration, ok := ctx.Value(durationContextKey{}).(time.Duration)
	return duration, ok
}

// do function context key.
type doFunctionKey struct{}

// Do function handler.
type DoFuncHandler func()

// New function context.
func NewDoFunctionContext(ctx context.Context, f DoFuncHandler) context.Context {
	return context.WithValue(ctx, doFunctionKey{}, f)
}

// Get function from context.
func DoFunctionFromContext(ctx context.Context) (DoFuncHandler, bool) {
	f, ok := ctx.Value(doFunctionKey{}).(DoFuncHandler)
	return f, ok
}
