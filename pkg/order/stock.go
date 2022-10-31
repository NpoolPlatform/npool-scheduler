package order

import (
	"context"
	"fmt"
	goodscli "github.com/NpoolPlatform/good-middleware/pkg/client/good"
	//
	//"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	//
	//stockcli "github.com/NpoolPlatform/stock-manager/pkg/client"
	//stockconst "github.com/NpoolPlatform/stock-manager/pkg/const"
	//
	//cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	//"google.golang.org/protobuf/types/known/structpb"

	goodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good"
)

func updateStock(ctx context.Context, goodID string, unlocked, inservice int32) error {
	goodInfo, err := goodscli.GetGood(ctx, goodID)
	if err != nil {
		return err
	}

	if goodInfo == nil {
		return fmt.Errorf("invalid good")
	}

	unlocked = unlocked * -1
	_, err = goodscli.UpdateGood(ctx, &goodmwpb.GoodReq{
		ID:        &goodID,
		Locked:    &inservice,
		InService: &unlocked,
	})
	if err != nil {
		return err
	}
	return err
}
