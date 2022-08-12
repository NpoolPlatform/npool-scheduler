package order

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	stockcli "github.com/NpoolPlatform/stock-manager/pkg/client"
	stockconst "github.com/NpoolPlatform/stock-manager/pkg/const"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	"google.golang.org/protobuf/types/known/structpb"
)

func updateStock(ctx context.Context, goodID string, unlocked, inservice int32) error {
	stock, err := stockcli.GetStockOnly(ctx, cruder.NewFilterConds().
		WithCond(stockconst.StockFieldGoodID, cruder.EQ, structpb.NewStringValue(goodID)))
	if err != nil {
		return err
	}
	if stock == nil {
		return fmt.Errorf("invalid stock")
	}

	fields := cruder.NewFilterFields()
	if inservice > 0 {
		fields = fields.WithField(stockconst.StockFieldInService, structpb.NewNumberValue(float64(inservice)))
	}
	if unlocked > 0 {
		fields = fields.WithField(stockconst.StockFieldLocked, structpb.NewNumberValue(float64(unlocked*-1)))
	}

	if len(fields) == 0 {
		return nil
	}

	logger.Sugar().Infow("updateStock", "good", goodID, "inservice", inservice, "unlocked", unlocked)

	_, err = stockcli.AddStockFields(ctx, stock.ID, fields)
	return err
}
