package notif

import (
	"context"
	"encoding/json"
	"fmt"

	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	notifbenefitmwcli "github.com/NpoolPlatform/notif-middleware/pkg/client/notif/goodbenefit"
)

func Prepare(body string) (interface{}, error) {
	req := notifbenefitmwpb.GoodBenefitReq{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func Apply(ctx context.Context, req interface{}) error {
	in, ok := req.(*notifbenefitmwpb.GoodBenefitReq)
	if !ok {
		return fmt.Errorf("invalid request in apply")
	}

	exist, err := notifbenefitmwcli.ExistGoodBenefitConds(ctx, &notifbenefitmwpb.Conds{
		GoodID:     &basetypes.StringVal{Op: cruder.EQ, Value: *in.GoodID},
		CoinTypeID: &basetypes.StringVal{Op: cruder.EQ, Value: *in.CoinTypeID},
		Generated:  &basetypes.BoolVal{Op: cruder.EQ, Value: false},
	})
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	if _, err := notifbenefitmwcli.CreateGoodBenefit(ctx, in); err != nil {
		return err
	}

	return nil
}
