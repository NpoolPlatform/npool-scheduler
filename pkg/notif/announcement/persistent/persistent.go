package persistent

import (
	"context"
	"fmt"

	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, announcement interface{}, notif, done chan interface{}) error {
	_, ok := announcement.(*types.PersistentAnnouncement)
	if !ok {
		return fmt.Errorf("invalid announcement")
	}
	return nil
}
