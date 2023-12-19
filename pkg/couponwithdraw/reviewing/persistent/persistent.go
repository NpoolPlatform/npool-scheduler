package persistent

import (
	"context"

	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, couponwithdraw interface{}, notif, done chan interface{}) error {
	return nil
}
