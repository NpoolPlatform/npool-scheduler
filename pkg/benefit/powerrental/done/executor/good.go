package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/powerrental/done/types"
	schedcommon "github.com/NpoolPlatform/npool-scheduler/pkg/common"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"

	"github.com/shopspring/decimal"
)

type coinNextReward struct {
	types.CoinNextReward
	lastRewardAmount decimal.Decimal
}

type goodHandler struct {
	*powerrentalmwpb.PowerRental
	persistent      chan interface{}
	notif           chan interface{}
	done            chan interface{}
	goodCoins       map[string]*coinmwpb.Coin
	rewardTxs       map[string]*txmwpb.Tx
	benefitOrderIDs []uint32
	coinNextRewards []*coinNextReward
}

func (h *goodHandler) getGoodCoins(ctx context.Context) (err error) {
	h.goodCoins, err = schedcommon.GetCoins(ctx, func() (coinTypeIDs []string) {
		for _, goodCoin := range h.GoodCoins {
			coinTypeIDs = append(coinTypeIDs, goodCoin.CoinTypeID)
		}
		return
	}())
	if err != nil {
		return wlog.WrapError(err)
	}
	for _, goodCoin := range h.GoodCoins {
		if _, ok := h.goodCoins[goodCoin.CoinTypeID]; !ok {
			return wlog.Errorf("invalid goodcoin")
		}
	}
	return nil
}

func (h *goodHandler) getRewardTxs(ctx context.Context) (err error) {
	h.rewardTxs, err = schedcommon.GetTxs(ctx, func() (txIDs []string) {
		for _, reward := range h.Rewards {
			txIDs = append(txIDs, reward.RewardTID)
		}
		return
	}())
	return wlog.WrapError(err)
}

func (h *goodHandler) calculateCoinNextRewardStartAmounts() error {
	for _, reward := range h.Rewards {
		lastRewardAmount, err := decimal.NewFromString(reward.LastRewardAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		coinNextReward := &coinNextReward{
			CoinNextReward: types.CoinNextReward{
				CoinTypeID: reward.CoinTypeID,
			},
			lastRewardAmount: lastRewardAmount,
		}
		transferred, err := h.checkLeastTransferAmount(coinNextReward)
		if err != nil {
			return wlog.WrapError(err)
		}
		if !transferred {
			continue
		}
		tx, ok := h.rewardTxs[reward.RewardTID]
		if !ok {
			continue
		}
		if tx.State != basetypes.TxState_TxStateSuccessful {
			continue
		}
		nextRewardStartAmount, err := decimal.NewFromString(reward.NextRewardStartAmount)
		if err != nil {
			return wlog.WrapError(err)
		}
		coinNextReward.NextRewardStartAmount = nextRewardStartAmount.Sub(lastRewardAmount).String()
		h.coinNextRewards = append(h.coinNextRewards, coinNextReward)
	}
	return nil
}

func (h *goodHandler) checkLeastTransferAmount(reward *coinNextReward) (bool, error) {
	coin, ok := h.goodCoins[reward.CoinTypeID]
	if !ok {
		return false, wlog.Errorf("invalid coin")
	}
	least, err := decimal.NewFromString(coin.LeastTransferAmount)
	if err != nil {
		return false, err
	}
	if least.Cmp(decimal.NewFromInt(0)) <= 0 {
		return false, wlog.Errorf("invalid leasttransferamount")
	}
	if reward.lastRewardAmount.Cmp(least) <= 0 {
		return false, nil
	}
	return true, nil
}

func (h *goodHandler) getBenefitOrders(ctx context.Context) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		orders, _, err := powerrentalordermwcli.GetPowerRentalOrders(ctx, &powerrentalordermwpb.Conds{
			GoodID:        &basetypes.StringVal{Op: cruder.EQ, Value: h.GoodID},
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
			"PowerRental", h.PowerRental,
			"RewardTxs", h.rewardTxs,
			"Error", *err,
		)
	}

	persistentGood := &types.PersistentGood{
		PowerRental: h.PowerRental,
		CoinNextRewards: func() (rewards []*types.CoinNextReward) {
			for _, reward := range h.coinNextRewards {
				rewards = append(rewards, &reward.CoinNextReward)
			}
			return
		}(),
		BenefitOrderIDs: h.benefitOrderIDs,
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

	if err = h.getGoodCoins(ctx); err != nil {
		return err
	}
	if err = h.getRewardTxs(ctx); err != nil {
		return err
	}
	if err = h.getBenefitOrders(ctx); err != nil {
		return err
	}
	if err = h.calculateCoinNextRewardStartAmounts(); err != nil {
		return err
	}

	return nil
}
