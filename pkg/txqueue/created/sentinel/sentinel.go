package sentinel

import (
	"context"
	"time"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
)

type handler struct {
	w    *watcher.Watcher
	exec chan *txmwpb.Tx
}

var h *handler

func Initialize(ctx context.Context, cancel context.CancelFunc, exec chan *txmwpb.Tx) {
	go action.Watch(ctx, cancel, func(_ctx context.Context) {
		h = &handler{
			w:    watcher.NewWatcher(),
			exec: exec,
		}
		h.run(_ctx)
	})
}

func (h *handler) scanTxs(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{}, offset, limit)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			return nil
		}

		ignores := map[string]struct{}{}
		for _, tx := range txs {
			if _, ok := ignores[tx.FromAccountID]; ok {
				continue
			}
			h.exec <- tx
			ignores[tx.FromAccountID] = struct{}{}
		}

		offset += limit
	}
}

func (h *handler) handler(ctx context.Context) bool {
	const scanInterval = time.Minute
	ticker := time.NewTicker(scanInterval)

	select {
	case <-ticker.C:
		if err := h.scanTxs(ctx); err != nil {
			logger.Sugar().Infow(
				"handler",
				"State", "scanTxs",
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
