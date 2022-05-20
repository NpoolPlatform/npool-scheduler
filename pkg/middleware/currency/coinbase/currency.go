package coinbase

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"

	"golang.org/x/xerrors"
)

const (
	coinbaseAPI = "https://api.coinbase.com/v2/prices/COIN-USD/sell"
)

func mapCoin(coinName string) string {
	coinMap := map[string]string{
		"fil":        "FIL",
		"filecoin":   "FIL",
		"tfilecoin":  "FIL",
		"btc":        "BTC",
		"bitcoin":    "BTC",
		"tbitcoin":   "BTC",
		"tethereum":  "ETH",
		"eth":        "ETH",
		"ethereum":   "ETH",
		"tusdt":      "USDT",
		"usdt":       "USDT",
		"tusdterc20": "USDT",
		"usdterc20":  "USDT",
		"sol":        "SOL",
		"solana":     "SOL",
		"tsolana":    "SOL",
	}
	if coin, ok := coinMap[coinName]; ok {
		return coin
	}
	return coinName
}

type apiResp struct {
	Base     string `json:"base"`
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
}

func USDPrice(ctx context.Context, coinName string) (float64, error) {
	coin := mapCoin(strings.ToLower(coinName))

	socksProxy := os.Getenv("ENV_CURRENCY_REQUEST_PROXY")

	url := strings.ReplaceAll(coinbaseAPI, "COIN", coin)

	cli := resty.New()
	cli = cli.SetTimeout(5 * time.Second)
	if socksProxy != "" {
		cli = cli.SetProxy(socksProxy)
	}

	resp, err := cli.R().Get(url)
	if err != nil {
		return 0, xerrors.Errorf("fail get currency %v: %v", coin, err)
	}
	r := apiResp{}
	err = json.Unmarshal(resp.Body(), &r)
	if err != nil {
		return 0, xerrors.Errorf("fail parse currency %v: %v", coin, err)
	}

	if coin != r.Base {
		return 0, xerrors.Errorf("invalid get coin currently: %v", resp.Body())
	}

	amount, err := strconv.ParseFloat(r.Amount, 64)
	if err != nil {
		return 0, xerrors.Errorf("invalid coin currency amount: %v", err)
	}

	return amount, nil
}
