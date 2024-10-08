package persistent

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	notifmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif"
	notifmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/notification/types"
	sendmwcli "github.com/NpoolPlatform/third-middleware/pkg/client/send"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, notif interface{}, reward, notif1, done chan interface{}) error {
	_notif, ok := notif.(*types.PersistentNotif)
	if !ok {
		return fmt.Errorf("invalid notif")
	}

	defer asyncfeed.AsyncFeed(ctx, _notif, done)

	if err := func() error {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start).Milliseconds()
			if elapsed > 1000 { //nolint
				logger.Sugar().Warnw(
					"Update",
					"ElapsedMS", elapsed,
					"NotifID", _notif.ID,
				)
			}
		}()
		return sendmwcli.SendMessage(ctx, _notif.MessageRequest)
	}(); err != nil {
		return err
	}
	if len(_notif.EventNotifs) == 0 {
		return nil
	}
	reqs := []*notifmwpb.NotifReq{}
	notified := true
	for _, notif := range _notif.EventNotifs {
		reqs = append(reqs, &notifmwpb.NotifReq{
			ID:       &notif.ID,
			Notified: &notified,
		})
	}
	if _, err := notifmwcli.UpdateNotifs(ctx, reqs); err != nil {
		return err
	}

	return nil
}
