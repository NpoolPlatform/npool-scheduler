package persistent

import (
	"context"
	"fmt"

	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/withdraw/review/notify/types"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, notify interface{}, reward, notif, done chan interface{}) error {
	_notify, ok := notify.(*types.PersistentWithdrawReviewNotify)
	if !ok {
		return fmt.Errorf("invalid notify")
	}

	asyncfeed.AsyncFeed(ctx, _notify, done)

	return nil
}
