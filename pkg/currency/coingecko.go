package currency

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NpoolPlatform/oracle-manager/pkg/middleware/currency/common"
	"github.com/go-resty/resty/v2"
)

const (
	coinGeckoAPI = "https://api.coingecko.com/api/v3"
)

func CoinGeckoUSDPrices(coinNames []string) (map[string]map[string]float64, error) {
	coins := ""
	for _, val := range coinNames {
		coins += fmt.Sprintf("%v,", common.MapCoin(strings.ToLower(val)))
	}
	coins = coins[:len(coins)-1]

	socksProxy := os.Getenv("ENV_CURRENCY_REQUEST_PROXY")
	url := fmt.Sprintf("%v%v?ids=%v&vs_currencies=usd", coinGeckoAPI, "/simple/price", coins)

	cli := resty.New()
	cli = cli.SetTimeout(5 * time.Second)
	if socksProxy != "" {
		cli = cli.SetProxy(socksProxy)
	}

	resp, err := cli.R().Get(url)
	if err != nil {
		return nil, fmt.Errorf("fail get currency %v: %v", coins, err)
	}
	respMap := map[string]map[string]float64{}
	err = json.Unmarshal(resp.Body(), &respMap)
	if err != nil {
		return nil, fmt.Errorf("fail parse currency %v: %v", coins, err)
	}

	return respMap, nil
}
