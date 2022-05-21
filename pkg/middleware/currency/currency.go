package currency

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/staker-manager/pkg/middleware/currency/coinbase"
	"github.com/NpoolPlatform/staker-manager/pkg/middleware/currency/coingecko"
)

func USDPrice(ctx context.Context, coinName string) (float64, error) {
	myPrice := 0.0
	var err error

	if myPrice, err = coingecko.USDPrice(ctx, coinName); err == nil {
		return myPrice, nil
	}
	if myPrice, err = coinbase.USDPrice(ctx, coinName); err == nil {
		return myPrice, nil
	}
	return 0, fmt.Errorf("fail get %v currency: %v", coinName, err)
}
