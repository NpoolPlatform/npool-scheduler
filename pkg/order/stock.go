package order

import (
	"context"
	"fmt"

	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

func updateStock(ctx context.Context, goodID string, unlocked, inservice, waitstart int32) error {
	goodInfo, err := goodscli.GetGood(ctx, goodID)
	if err != nil {
		return err
	}

	if goodInfo == nil {
		return fmt.Errorf("invalid good")
	}

	unlocked *= -1
	_, err = goodscli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:        &goodID,
		Locked:    &unlocked,
		InService: &inservice,
		WaitStart: &waitstart,
	})
	if err != nil {
		return err
	}
	return err
}
