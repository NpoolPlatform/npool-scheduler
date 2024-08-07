package sentinel

import (
	"context"
	"time"

	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	"github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	ordertypes "github.com/NpoolPlatform/message/npool/basetypes/order/v1"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	powerrentalordermwpb "github.com/NpoolPlatform/message/npool/order/mw/v1/powerrental"
	cancelablefeed "github.com/NpoolPlatform/npool-scheduler/pkg/base/cancelablefeed"
	basesentinel "github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/order/powerrental/simulate/created/types"
	powerrentalordermwcli "github.com/NpoolPlatform/order-middleware/pkg/client/powerrental"
)

type handler struct{}

func NewSentinel() basesentinel.Scanner {
	return &handler{}
}

func (h *handler) scanPowerRentalOrders(ctx context.Context, state ordertypes.OrderState, exec chan interface{}) error {
	offset := int32(0)
	limit := constant.DefaultRowLimit

	for {
		createdAt := uint32(time.Now().Unix()) - timedef.SecondsPerMinute
		orders, _, err := powerrentalordermwcli.GetPowerRentalOrders(ctx, &powerrentalordermwpb.Conds{
			OrderState: &basetypes.Uint32Val{Op: cruder.EQ, Value: uint32(state)},
			Simulate:   &basetypes.BoolVal{Op: cruder.EQ, Value: true},
			CreatedAt:  &basetypes.Uint32Val{Op: cruder.LT, Value: createdAt},
		}, offset, limit)
		if err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		for _, order := range orders {
			cancelablefeed.CancelableFeed(ctx, order, exec)
		}

		offset += limit
	}
}

func (h *handler) Scan(ctx context.Context, exec chan interface{}) error {
	return h.scanPowerRentalOrders(ctx, ordertypes.OrderState_OrderStateCreated, exec)
}

func (h *handler) InitScan(ctx context.Context, exec chan interface{}) error {
	return nil
}

func (h *handler) TriggerScan(ctx context.Context, cond interface{}, exec chan interface{}) error {
	return nil
}

func (h *handler) ObjectID(ent interface{}) string {
	// For each user, we process only one order per time
	if order, ok := ent.(*types.PersistentPowerRentalOrder); ok {
		return order.OrderID
	}
	return ent.(*powerrentalordermwpb.PowerRentalOrder).OrderID
}
