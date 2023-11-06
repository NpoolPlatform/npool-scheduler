package persistent

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	ancsendmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement/sendstate"
	ancsendmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/announcement/sendstate"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	basepersistent "github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/announcement/types"
	sendmwcli "github.com/NpoolPlatform/third-middleware/pkg/client/send"
)

type handler struct{}

func NewPersistent() basepersistent.Persistenter {
	return &handler{}
}

func (p *handler) Update(ctx context.Context, announcement interface{}, notif, done chan interface{}) error {
	_announcement, ok := announcement.(*types.PersistentAnnouncement)
	if !ok {
		return fmt.Errorf("invalid announcement")
	}

	defer asyncfeed.AsyncFeed(ctx, _announcement, done)

	if err := func() error {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start).Milliseconds()
			if elapsed > 1000 { //nolint
				logger.Sugar().Warnw(
					"Update",
					"ElapsedMS", elapsed,
					"AnnouncementID", _announcement.ID,
				)
			}
		}()
		return sendmwcli.SendMessage(ctx, _announcement.MessageRequest)
	}(); err != nil {
		return err
	}

	if _, err := ancsendmwcli.CreateSendState(ctx, &ancsendmwpb.SendStateReq{
		AppID:          &_announcement.SendAppID,
		UserID:         &_announcement.SendUserID,
		AnnouncementID: &_announcement.EntID,
	}); err != nil {
		return err
	}

	return nil
}
