package persistent

import (
	"context"
	"fmt"

	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/notif/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, notif interface{}, retry, notif1 chan interface{}) error {
	_, ok := notif.(*types.PersistentNotif)
	if !ok {
		return fmt.Errorf("invalid notif")
	}

	return nil
}
