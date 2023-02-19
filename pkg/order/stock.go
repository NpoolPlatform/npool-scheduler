package order

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

func updateStock(ctx context.Context, goodID string, unlocked, inservice, waitstart decimal.Decimal) error {
	goodInfo, err := goodscli.GetGood(ctx, goodID)
	if err != nil {
		return err
	}

	if goodInfo == nil {
		return fmt.Errorf("invalid good")
	}

	unlockedStr := unlocked.Neg().String()
	inserviceStr := inservice.String()
	waitstartStr := waitstart.String()

	_, err = goodscli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:        &goodID,
		Locked:    &unlockedStr,
		InService: &inserviceStr,
		WaitStart: &waitstartStr,
	})
	if err != nil {
		return err
	}
	return err
}
