package coinbase

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NpoolPlatform/staker-manager/pkg/middleware/currency/common"
	"github.com/go-resty/resty/v2"

	"golang.org/x/xerrors"
)

const (
	coinbaseAPI = "https://api.coinbase.com/v2/prices/COIN-USD/sell"
)

type apiData struct {
	Base     string `json:"base"`
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
}

type apiResp struct {
	Data apiData `json:"data"`
}

func USDPrice(ctx context.Context, coinName string) (float64, error) {
	coin := common.MapCoin(strings.ToLower(coinName))

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

	if coin != r.Data.Base {
		return 0, xerrors.Errorf("invalid get coin currency from %v: %v", url, string(resp.Body()))
	}

	amount, err := strconv.ParseFloat(r.Data.Amount, 64)
	if err != nil {
		return 0, xerrors.Errorf("invalid coin currency amount: %v", err)
	}

	return amount, nil
}
