package executor

import (
	"context"
	"fmt"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
	ordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/order"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/done/types"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	ordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/order"
	"github.com/shopspring/decimal"
)

type goodHandler struct {
	*goodmwpb.Good
	persistent            chan interface{}
	notif                 chan interface{}
	done                  chan interface{}
	nextStartRewardAmount decimal.Decimal
	coin                  *coinmwpb.Coin
	benefitOrderIDs       []string
	rewardTx              *txmwpb.Tx
}

func (h *goodHandler) checkLeastTransferAmount() error {
	least, err := decimal.NewFromString(h.coin.LeastTransferAmount)
	if err != nil {
		return err
	}
	if least.Cmp(decimal.NewFromInt(0)) <= 0 {
		return fmt.Errorf("invalid leasttransferamount")
	}
	lastRewardAmount, err := decimal.NewFromString(h.LastRewardAmount)
	if err != nil {
		return err
	}
	if lastRewardAmount.Cmp(least) <= 0 {
		return nil
	}
	h.nextStartRewardAmount = h.nextStartRewardAmount.Sub(lastRewardAmount)
	return nil
}

func (h *goodHandler) getCoin(ctx context.Context) error {
	coin, err := coinmwcli.GetCoin(ctx, h.CoinTypeID)
	if err != nil {
		return err
	}
	if coin == nil {
		return fmt.Errorf("invalid coin")
	}
	h.coin = coin
	return nil
}

func (h *goodHandler) getTransfer(ctx context.Context) error {
	tx, err := txmwcli.GetTx(ctx, h.RewardTID)
	if err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("invalid tx")
	}
	h.rewardTx = tx
	return nil
}

func (h *goodHandler) getBenefitOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := ordermwcli.GetOrders(ctx, &ordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.ID},
			LastBenefitAt: &basetypes.Uint32Val{Op: cruder.EQ, Value: h.LastRewardAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			break
		}
		for _, order := range orders {
			h.benefitOrderIDs = append(h.benefitOrderIDs, order.ID)
		}
		offset += limit
	}
	return nil
}

//nolint:gocritic
func (h *goodHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"Good", h.Good,
			"NextStartRewardAmount", h.nextStartRewardAmount,
			"RewardTx", h.rewardTx,
			"Error", *err,
		)
	}

	persistentGood := &types.PersistentGood{
		Good:                  h.Good,
		NextStartRewardAmount: h.nextStartRewardAmount.String(),
		BenefitOrderIDs:       h.benefitOrderIDs,
	}
	if h.rewardTx != nil {
		persistentGood.BenefitMessage = fmt.Sprintf(
			"%v@%v(%v)",
			h.rewardTx.ChainTxID,
			h.LastRewardAt,
			h.RewardTID,
		)
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGood, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGood, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGood, h.done)
}

//nolint
func (h *goodHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, &err)

	if err = h.getTransfer(ctx); err != nil {
		return err
	}
	if err = h.getCoin(ctx); err != nil {
		return err
	}
	h.nextStartRewardAmount, err = decimal.NewFromString(h.NextRewardStartAmount)
	if err != nil {
		return err
	}
	if err = h.checkLeastTransferAmount(); err != nil {
		return err
	}
	if err = h.getBenefitOrders(ctx); err != nil {
		return err
	}

	return nil
}
