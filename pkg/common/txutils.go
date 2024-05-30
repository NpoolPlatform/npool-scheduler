//nolint:dupl
package common

import (
	"context"

	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	wlog "github.com/NpoolPlatform/go-service-framework/pkg/wlog"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"

	"github.com/google/uuid"
)

func GetTxs(ctx context.Context, txIDs []string) (map[string]*txmwpb.Tx, error) {
	for _, txID := range txIDs {
		if _, err := uuid.Parse(txID); err != nil {
			return nil, wlog.WrapError(err)
		}
	}

	txs, _, err := txmwcli.GetTxs(ctx, &txmwpb.Conds{
		EntIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: txIDs},
	}, int32(0), int32(len(txIDs)))
	if err != nil {
		return nil, wlog.WrapError(err)
	}
	txMap := map[string]*txmwpb.Tx{}
	for _, tx := range txs {
		txMap[tx.EntID] = tx
	}
	return txMap, nil
}
