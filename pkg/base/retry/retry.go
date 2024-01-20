package retry

import (
	"context"
	"sync"
	"time"
)

type retryEnt struct {
	ent    interface{}
	retry  chan interface{}
	execAt time.Time
}

var entMap sync.Map

func handler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			entMap.Range(func(k, v interface{}) bool {
				if time.Now().Before(v.(*retryEnt).execAt) {
					v.(*retryEnt).retry <- v.(*retryEnt).ent
					entMap.Delete(k)
				}
				return true
			})
		}
	}
}

func Initialize(ctx context.Context) {
	go handler(ctx)
}

func Retry(entID string, ent interface{}, retry chan interface{}) {
	entMap.Store(entID, &retryEnt{
		ent:    ent,
		retry:  retry,
		execAt: time.Now().Add(time.Minute),
	})
}
