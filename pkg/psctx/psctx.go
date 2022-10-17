package psctx

import (
	"context"
	"fmt"
	"time"
)

type StartKey struct{}

var ErrStartTimeNotSet = fmt.Errorf("start time not set")

func WithStartTime(ctx context.Context, start time.Time) context.Context {
	return context.WithValue(ctx, StartKey{}, start)
}

func StartTime(ctx context.Context) (time.Time, error) {
	val := ctx.Value(StartKey{})
	if val == nil {
		return time.Time{}, ErrStartTimeNotSet
	}
	t, ok := val.(time.Time)
	if !ok {
		return t, fmt.Errorf("start time not time.Time, is: %T", val)
	}
	return t, nil
}
