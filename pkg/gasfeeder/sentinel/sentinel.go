package sentinel

import (
	"context"
	"time"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"

	"github.com/google/uuid"
)

type handler struct {
	w    *watcher.Watcher
	exec chan *coinmwpb.Coin
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc, exec chan *coinmwpb.Coin) {
	go action.Watch(ctx, cancel, func(_ctx context.Context) {
		h = &handler{
			w:    watcher.NewWatcher(),
			exec: exec,
		}
		h.run(_ctx)
	})
}

func (h *handler) scanCoins(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{}, offset, limit)
		if err != nil {
			return err
		}
		if len(coins) == 0 {
			return nil
		}

		for _, coin := range coins {
			if _, err := uuid.Parse(coin.FeeCoinTypeID); err != nil {
				continue
			}
			if coin.FeeCoinTypeID == uuid.Nil.String() {
				continue
			}
			if coin.FeeCoinTypeID == coin.ID {
				continue
			}
			h.exec <- coin
		}

		offset += limit
	}
}

func (h *handler) handler(ctx context.Context) bool {
	const scanInterval = time.Minute
	ticker := time.NewTicker(scanInterval)

	select {
	case <-ticker.C:
		if err := h.scanCoins(ctx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "scanWaitPayment",
				"Error", err,
			)
		}
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Error", ctx.Err(),
		)
		close(h.w.ClosedChan())
		return true
	case <-h.w.CloseChan():
		close(h.w.ClosedChan())
		return true
	}
}

func (h *handler) run(ctx context.Context) {
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func Finalize() {
	if h != nil && h.w != nil {
		h.w.Shutdown()
	}
}
