package executor

import (
	"context"
	"fmt"
	"time"

	appgoodmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/good"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	appgoodmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/good"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/benefit/types"

	"github.com/shopspring/decimal"
)

type benefitHandler struct {
	benefits      []*notifbenefitmwpb.GoodBenefit
	persistent    chan interface{}
	notif         chan interface{}
	notifContents []*types.NotifContent
	content       string
	appGoods      map[string]*appgoodmwpb.Good
}

func (h *benefitHandler) getAppGoods(ctx context.Context) error {
	goodIDs := []string{}
	for _, benefit := range h.benefits {
		goodIDs = append(goodIDs, benefit.GoodID)
	}
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := appgoodmwcli.GetGoods(ctx, &appgoodmwpb.Conds{
			GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: goodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			return nil
		}
		for _, good := range goods {
			h.appGoods[good.GoodID] = good
		}
		offset += limit
	}
}

func (h *benefitHandler) generateNotifContent(ctx context.Context) error {
	h.content = "<html><head><style>table.notif-benefit {border-collapse: collapse;width: 100%;}#notif-good-benefit td,#notif-good-benefit th {border: 1px solid #dddddd;text-align: left;padding: 8px;}</style></head><table id='notif-good-benefit' class='notif-benefit'><tr><th>GoodID</th><th>GoodName</th><th>Amount</th><th>AmountPerUnit</th><th>State</th><th>Message</th><th>BenefitDate</th></tr>"
	for _, benefit := range h.benefits {
		good, ok := h.appGoods[benefit.GoodID]
		if !ok {
			return fmt.Errorf("invalid good")
		}
		total, err := decimal.NewFromString(good.GoodTotal)
		if err != nil {
			return err
		}
		amount, err := decimal.NewFromString(benefit.Amount)
		if err != nil {
			return err
		}

		tm := time.Unix(int64(benefit.BenefitDate), 0)
		h.content += fmt.Sprintf(
			`<tr><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td></tr>`,
			benefit.GoodID,
			benefit.GoodName,
			benefit.Amount,
			amount.Div(total),
			benefit.State,
			benefit.Message,
			tm,
		)
	}
	return nil
}

func (h *benefitHandler) generateNotifContents() {
	appIDs := map[string]struct{}{}
	for _, appGood := range h.appGoods {
		appIDs[appGood.AppID] = struct{}{}
	}
	for appID, _ := range appIDs {
		h.notifContents = append(h.notifContents, &types.NotifContent{
			AppID:   appID,
			Content: h.content,
		})
	}
}

func (h *benefitHandler) final(ctx context.Context, err *error) {
	persistentGoodBenefit := &types.PersistentGoodBenefit{
		Benefits:      h.benefits,
		NotifContents: h.notifContents,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(persistentGoodBenefit, h.persistent)
	} else {
		asyncfeed.AsyncFeed(persistentGoodBenefit, h.notif)

	}
}

func (h *benefitHandler) exec(ctx context.Context) error {
	h.appGoods = map[string]*appgoodmwpb.Good{}

	var err error
	defer h.final(ctx, &err)

	if err = h.getAppGoods(ctx); err != nil {
		return err
	}
	if err = h.generateNotifContent(ctx); err != nil {
		return err
	}
	h.generateNotifContents()

	return nil
}
