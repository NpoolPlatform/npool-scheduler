//nolint:dupl
package common

import (
	"context"

	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"

	"github.com/google/uuid"
)

func GetCoins(ctx context.Context, coinTypeIDs []string) (map[string]*coinmwpb.Coin, error) {
	for _, coinTypeID := range coinTypeIDs {
		if _, err := uuid.Parse(coinTypeID); err != nil {
			return nil, wlog.WrapError(err)
		}
	}

	coins, _, err := coinmwcli.GetCoins(ctx, &coinmwpb.Conds{
		EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: coinTypeIDs},
	}, int32(0), int32(len(coinTypeIDs)))
	if err != nil {
		return nil, wlog.WrapError(err)
	}
	coinMap := map[string]*coinmwpb.Coin{}
	for _, coin := range coins {
		coinMap[coin.EntID] = coin
	}
	return coinMap, nil
}
