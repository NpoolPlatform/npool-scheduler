package executor

import (
	"context"
	"fmt"

	ancmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/announcement"
	baseexecutor "github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
)

type handler struct{}

func NewExecutor() baseexecutor.Exec {
	return &handler{}
}

func (e *handler) Exec(ctx context.Context, announcement interface{}, retry, persistent, notif chan interface{}) error {
	_announcement, ok := announcement.(*ancmwpb.Announcement)
	if !ok {
		return fmt.Errorf("invalid announcement")
	}

	h := &announcementHandler{
		Announcement: _announcement,
		persistent:   persistent,
	}
	return h.exec(ctx)
}
