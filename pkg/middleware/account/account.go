package account

import (
	"fmt"

	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
)

func key(id string) string {
	return fmt.Sprintf("account-lock:%v", id)
}

func Lock(id string) error {
	return redis2.TryLock(key(id), 0)
}

func Unlock(id string) error {
	return redis2.Unlock(key(id))
}
