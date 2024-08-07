package persistent

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, msg interface{}, notif, done chan interface{}) error {
	_msg := msg.(*string)

	defer asyncfeed.AsyncFeed(ctx, _msg, done)
	fmt.Println("start persistent:", *_msg)
	return nil
}
