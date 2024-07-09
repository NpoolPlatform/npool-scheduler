package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	apppowerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/app/powerrental"
	powerrentalmwcli "github.com/NpoolPlatform/good-middleware/pkg/client/powerrental"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	apppowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/app/powerrental"
	powerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	notifbenefitmwpb "github.com/NpoolPlatform/message/npool/notif/mw/v1/notif/goodbenefit"
	asyncfeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/asyncfeed"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/notif/benefit/powerrental/types"

	"github.com/shopspring/decimal"
)

type benefitHandler struct {
	benefits        []*notifbenefitmwpb.GoodBenefit
	persistent      chan interface{}
	notif           chan interface{}
	done            chan interface{}
	notifContents   []*types.NotifContent
	content         string
	appPowerRentals map[string]map[string]*apppowerrentalmwpb.PowerRental
	powerRentals    map[string]*powerrentalmwpb.PowerRental
}

func (h *benefitHandler) getPowerRentals(ctx context.Context) error {
	goodIDs := []string{}
	for _, benefit := range h.benefits {
		goodIDs = append(goodIDs, benefit.GoodID)
	}
	goods, _, err := powerrentalmwcli.GetPowerRentals(ctx, &powerrentalmwpb.Conds{
		GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: goodIDs},
	}, int32(0), int32(len(goodIDs)))
	if err != nil {
		return err
	}
	for _, good := range goods {
		h.powerRentals[good.EntID] = good
	}
	return nil
}

func (h *benefitHandler) getAppPowerRentals(ctx context.Context) error {
	goodIDs := []string{}
	for _, benefit := range h.benefits {
		goodIDs = append(goodIDs, benefit.GoodID)
	}
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		goods, _, err := apppowerrentalmwcli.GetPowerRentals(ctx, &apppowerrentalmwpb.Conds{
			GoodIDs: &basetypes.StringSliceVal{Op: cruder.IN, Value: goodIDs},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(goods) == 0 {
			return nil
		}
		for _, good := range goods {
			appPowerRentals, ok := h.appPowerRentals[good.GoodID]
			if !ok {
				appPowerRentals = map[string]*apppowerrentalmwpb.PowerRental{}
			}
			appPowerRentals[good.EntID] = good
			h.appPowerRentals[good.GoodID] = appPowerRentals
		}
		offset += limit
	}
}

func (h *benefitHandler) generateHTMLHeader() {
	h.content += "<html>"
	h.content += "<head>"
	h.content += "<style>"
	h.content += "table.notif-benefit {border-collapse: collapse;width: 100%;}"
	h.content += "#notif-good-benefit td,#notif-good-benefit th {border: 1px solid #dddddd;text-align: left;padding: 8px;}"
	h.content += "</style>"
	h.content += "</head>"
	h.content += "<table id='notif-good-benefit' class='notif-benefit'>"
}

//nolint
func (h *benefitHandler) generateTableHeader(goodTypeName string, appPowerRental bool) {
	h.content += "<tr>"
	if appPowerRental {
		h.content += fmt.Sprintf(`<th colspan="8">%v</th>`, goodTypeName)
	} else {
		h.content += fmt.Sprintf(`<th colspan="7">%v</th>`, goodTypeName)
	}
	h.content += "</tr>"
	h.content += "<tr>"
	if appPowerRental {
		h.content += "<th>AppGoodID</th>"
	}
	h.content += "<th>GoodID</th>"
	h.content += "<th>GoodType</th>"
	h.content += "<th>GoodName</th>"
	h.content += "<th>CoinTypeID</th>"
	h.content += "<th>Amount</th>"
	h.content += "<th>AmountPerUnit</th>"
	h.content += "<th>State</th>"
	h.content += "<th>Message</th>"
	h.content += "<th>BenefitDate</th>"
	h.content += "</tr>"
}

func (h *benefitHandler) generateGoodNotifContent() error {
	h.generateTableHeader("Platform Products", false)
	for _, benefit := range h.benefits {
		tm := time.Unix(int64(benefit.BenefitDate), 0)
		good, ok := h.powerRentals[benefit.GoodID]
		if !ok {
			return fmt.Errorf("invalid good")
		}

		total, err := decimal.NewFromString(good.GoodTotal)
		if err != nil {
			return err
		}
		if total.Cmp(decimal.NewFromInt(0)) <= 0 {
			continue
		}
		amount, err := decimal.NewFromString(benefit.Amount)
		if err != nil {
			return err
		}
		h.content += fmt.Sprintf(
			`<tr><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td></tr>`,
			benefit.GoodID,
			benefit.GoodType,
			benefit.GoodName,
			benefit.CoinTypeID,
			benefit.Amount,
			amount.Div(total),
			benefit.State,
			benefit.Message,
			tm,
		)
	}
	return nil
}

//nolint:gocognit
func (h *benefitHandler) generateAppGoodNotifContent() error {
	h.generateTableHeader("Application Products", true)
	for _, benefit := range h.benefits {
		tm := time.Unix(int64(benefit.BenefitDate), 0)
		appPowerRentals, ok := h.appPowerRentals[benefit.GoodID]
		if !ok {
			continue
		}
		powerRental, ok := h.powerRentals[benefit.GoodID]
		if !ok {
			continue
		}
		for appGoodID, appPowerRental := range appPowerRentals {
			appGoodInService, err := decimal.NewFromString(appPowerRental.AppGoodInService)
			if err != nil {
				return err
			}
			if appGoodInService.Cmp(decimal.NewFromInt(0)) <= 0 {
				continue
			}

			total, err := decimal.NewFromString(appPowerRental.GoodTotal)
			if err != nil {
				return err
			}
			amount, err := decimal.NewFromString(benefit.Amount)
			if err != nil {
				return err
			}

			goodInService, err := decimal.NewFromString(powerRental.GoodInService)
			if err != nil {
				return err
			}
			if goodInService.Cmp(decimal.NewFromInt(0)) <= 0 {
				continue
			}

			h.content += fmt.Sprintf(
				`<tr><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td></tr>`,
				appGoodID,
				appPowerRental.GoodID,
				appPowerRental.GoodName,
				amount.Mul(appGoodInService).Div(goodInService),
				amount.Div(total),
				benefit.State,
				benefit.Message,
				tm,
			)
		}
	}
	return nil
}

func (h *benefitHandler) generateNotifContents() {
	appIDs := map[string]struct{}{}
	for _, appPowerRentals := range h.appPowerRentals {
		for _, appPowerRental := range appPowerRentals {
			appIDs[appPowerRental.AppID] = struct{}{}
		}
	}
	for appID := range appIDs {
		h.notifContents = append(h.notifContents, &types.NotifContent{
			AppID:   appID,
			Content: h.content,
		})
	}
}

//nolint:gocritic
func (h *benefitHandler) final(ctx context.Context, err *error) {
	if *err != nil {
		logger.Sugar().Errorw(
			"final",
			"GoodBenefits", h.benefits,
			"Error", *err,
		)
	}
	persistentGoodBenefit := &types.PersistentGoodBenefit{
		Benefits:      h.benefits,
		NotifContents: h.notifContents,
	}
	if *err == nil {
		asyncfeed.AsyncFeed(ctx, persistentGoodBenefit, h.persistent)
		return
	}
	asyncfeed.AsyncFeed(ctx, persistentGoodBenefit, h.notif)
	asyncfeed.AsyncFeed(ctx, persistentGoodBenefit, h.done)
}

//nolint:gocritic
func (h *benefitHandler) exec(ctx context.Context) error {
	h.appPowerRentals = map[string]map[string]*apppowerrentalmwpb.PowerRental{}
	h.powerRentals = map[string]*powerrentalmwpb.PowerRental{}

	var err error
	defer h.final(ctx, &err)

	if err = h.getPowerRentals(ctx); err != nil {
		return err
	}
	if err = h.getAppPowerRentals(ctx); err != nil {
		return err
	}
	h.generateHTMLHeader()
	if err = h.generateGoodNotifContent(); err != nil {
		return err
	}
	if err = h.generateAppGoodNotifContent(); err != nil {
		return err
	}
	h.generateNotifContents()

	return nil
}
