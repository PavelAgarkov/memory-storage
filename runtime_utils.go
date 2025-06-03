package memory_storage

import (
	"context"
)

func GoRecover(ctx context.Context, fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {

			}
		}()
		select {
		case <-ctx.Done():
			return
		default:
		}
		fn(ctx)
	}()
}
