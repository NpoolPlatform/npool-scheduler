package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	"github.com/go-redis/redis/v8"
)

const (
	redisTimeout = 5 * time.Second
	cacheTimeout = 4 * time.Hour
)

type UnmarshalFunc func(val string) (interface{}, error)

var (
	unmarshalFuncs = map[string]UnmarshalFunc{}
	_mutex         sync.Mutex
)

func CreateCache(ctx context.Context, key string, val interface{}, f UnmarshalFunc) error {
	cli, err := redis2.GetClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()

	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	if err := cli.Set(ctx, key, body, cacheTimeout).Err(); err != nil {
		return err
	}

	_mutex.Lock()
	unmarshalFuncs[key] = f
	_mutex.Unlock()

	return nil
}

func QueryCache(ctx context.Context, key string) (interface{}, error) {
	cli, err := redis2.GetClient()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()

	val, err := cli.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	f, ok := unmarshalFuncs[key]
	if !ok {
		return nil, fmt.Errorf("invalid unmarshaler")
	}

	return f(val)
}
