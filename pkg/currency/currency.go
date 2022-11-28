package currency

import (
	"context"
	"fmt"
	coinfeed "github.com/NpoolPlatform/chain-middleware/pkg/client/coin/currency/feed"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	feedpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/coin/currency/feed"
)

func saveCurrency(ctx context.Context) {

}

func getCoinFeed(ctx context.Context) {
	offset := int32(0)
	limit := int32(1000)

	for {
		currencyFeeds, _, err := coinfeed.GetCurrencyFeeds(ctx, nil, offset, limit)
		if err != nil {
			logger.Sugar().Errorw("getCoinFeed", "offset", offset, "limit", limit, "error", err)
			return
		}
		if len(currencyFeeds) == 0 {
			return
		}

		err = processCurrencyFeeds(ctx, currencyFeeds)
		if err != nil {
			logger.Sugar().Errorw("processCurrencyFeeds", "offset", offset, "limit", limit, "error", err)
			return
		}
		offset += limit
	}
}

func processCurrencyFeeds(ctx context.Context, feed []*feedpb.CurrencyFeed) error {
	coinNames := []string{}
	for _, val := range feed {
		coinNames = append(coinNames, val.CoinName)
	}
	coins, err := CoinGeckoUSDPrices(coinNames)
	if err != nil {
		return err
	}
	fmt.Println(coins)
	return err
}
