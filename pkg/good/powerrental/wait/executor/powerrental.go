package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/good/v1"
	miningstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/stock"
	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	"github.com/NpoolPlatform/npool-scheduler/pkg/good/powerrental/wait/types"
)

var (
	currentMiningGoodStockState = v1.MiningGoodStockState_MiningGoodStockStateWait
	nextMiningGoodStockState    = v1.MiningGoodStockState_MiningGoodStockStateCreateGoodUser
)

type powerRentalHandler struct {
	*goodpowerrentalmwpb.PowerRental
	miningGoodStockReqs []*miningstockmwpb.MiningGoodStockReq
	persistent          chan interface{}
	notif               chan interface{}
	done                chan interface{}
}

func (h *powerRentalHandler) checkMiningGoodStockState() error {
	for _, miningGoodStock := range h.PowerRental.MiningGoodStocks {
		if miningGoodStock.State != currentMiningGoodStockState {
			return wlog.Errorf("invalid mininggoodstockstate: %v, mininggoodstock: %v", miningGoodStock.State, miningGoodStock.EntID)
		}
	}
	return nil
}

func (h *powerRentalHandler) constructMiningGoodStockReqs() {
	_miningGoodStockReqs := []*miningstockmwpb.MiningGoodStockReq{}
	for _, req := range h.PowerRental.MiningGoodStocks {
		_miningGoodStockReqs = append(_miningGoodStockReqs,
			&miningstockmwpb.MiningGoodStockReq{
				EntID: &req.EntID,
				State: &nextMiningGoodStockState,
			},
		)
	}
	h.miningGoodStockReqs = _miningGoodStockReqs
}

//nolint:gocritic
func (h *powerRentalHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Powerrental Good", h.PowerRental,
			"PaymentTransferCoins", h.MiningGoodStocks,
			"Error", *err,
		)
	}

	persistentPowerRental := &types.PersistentGoodPowerRental{
		PowerRental:         h.PowerRental,
		MiningGoodStockReqs: h.miningGoodStockReqs,
	}

	if err == nil {
		asyncfeed.AsyncFeed(ctx, persistentPowerRental, h.persistent)
	} else {
		asyncfeed.AsyncFeed(ctx, persistentPowerRental, h.done)
	}
}

func (h *powerRentalHandler) exec(ctx context.Context) error {
	var err error

	defer h.final(ctx, &err)

	if err = h.checkMiningGoodStockState(); err != nil {
		return wlog.WrapError(err)
	}
	h.constructMiningGoodStockReqs()
	return nil
}
