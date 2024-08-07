package executor

import (
	"context"

	"github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	v1 "github.com/NpoolPlatform/message/npool/basetypes/v1"
	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	"github.com/NpoolPlatform/message/npool/miningpool/mw/v1/rootuser"
	poolrootusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/rootuser"
	poolrootusermwcli "github.com/NpoolPlatform/miningpool-middleware/pkg/client/rootuser"
)

type powerRentalHandler struct {
	*goodpowerrentalmwpb.PowerRental
	poolRootUserMap map[string]*poolrootusermwpb.RootUser
	coinTypeIDs     []string
	persistent      chan interface{}
	notif           chan interface{}
	done            chan interface{}
}

func (h *powerRentalHandler) getPoolRootUsers(ctx context.Context) error {
	offset := 0
	limit := len(h.PowerRental.MiningGoodStocks)
	h.poolRootUserMap = make(map[string]*poolrootusermwpb.RootUser)

	poolRootUsers := []string{}
	for _, miningGoodStock := range h.PowerRental.MiningGoodStocks {
		poolRootUsers = append(poolRootUsers, miningGoodStock.PoolRootUserID)
	}

	infos, _, err := poolrootusermwcli.GetRootUsers(ctx, &rootuser.Conds{
		EntIDs: &v1.StringSliceVal{
			Op:    cruder.IN,
			Value: poolRootUsers,
		},
	}, int32(offset), int32(limit))
	if err != nil {
		return wlog.WrapError(err)
	}

	for _, info := range infos {
		h.poolRootUserMap[info.EntID] = info
	}
	return nil
}

func (h *powerRentalHandler) getCoinTypeIDs() {
	coinTypeIDs := []string{}
	for _, coinInfo := range h.PowerRental.GoodCoins {
		coinTypeIDs = append(coinTypeIDs, coinInfo.GetCoinTypeID())
	}
	h.coinTypeIDs = coinTypeIDs
}

func (h *powerRentalHandler) exec(ctx context.Context) error {

	return nil
}
