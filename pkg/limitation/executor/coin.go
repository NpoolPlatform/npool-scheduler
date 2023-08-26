package executor

import (
	"context"
	"fmt"
	"time"

	accountmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/account"
	depositaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/deposit"
	payaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/payment"
	pltfaccmwcli "github.com/NpoolPlatform/account-middleware/pkg/client/platform"
	coinmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/coin"
	txmwcli "github.com/NpoolPlatform/chain-middleware/pkg/client/tx"
	timedef "github.com/NpoolPlatform/go-service-framework/pkg/const/time"
	cruder "github.com/NpoolPlatform/libent-cruder/pkg/cruder"
	accountmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/account"
	depositaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/deposit"
	payaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/payment"
	pltfaccmwpb "github.com/NpoolPlatform/message/npool/account/mw/v1/platform"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	coinmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
	sphinxproxypb "github.com/NpoolPlatform/message/npool/sphinxproxy"
	constant "github.com/NpoolPlatform/npool-scheduler/pkg/const"
	types "github.com/NpoolPlatform/npool-scheduler/pkg/gasfeeder/types"
	sphinxproxycli "github.com/NpoolPlatform/sphinx-proxy/pkg/client"

	"github.com/shopspring/decimal"
)

type coinHandler struct {
	*coinmwpb.Coin
	persistent chan interface{}
	notif      chan interface{}
}

func (h *coinHandler) final(ctx context.Context, err error) {
	persistentCoin := &types.PersistentCoin{
		Coin: h.Coin,
	}

	h.notif <- persistentCoin
	if err == nil {
		h.persistent <- persistentCoin
	}
}

func (h *coinHandler) exec(ctx context.Context) error {
	var err error
	defer h.final(ctx, err)
	return nil
}
