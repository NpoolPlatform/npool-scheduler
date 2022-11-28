package currency

func MapCoin(coinName string) string {
	coinMap := map[string]string{
		"fil":          "filecoin",
		"filecoin":     "filecoin",
		"tfilecoin":    "filecoin",
		"btc":          "bitcoin",
		"bitcoin":      "bitcoin",
		"tbitcoin":     "bitcoin",
		"tethereum":    "ethereum",
		"eth":          "ethereum",
		"ethereum":     "ethereum",
		"tusdt":        "tether",
		"usdt":         "tether",
		"tusdterc20":   "tether",
		"usdterc20":    "tether",
		"tusdttrc20":   "tether",
		"usdttrc20":    "tether",
		"sol":          "solana",
		"solana":       "solana",
		"tsolana":      "solana",
		"tbinancecoin": "binancecoin",
		"binancecoin":  "binancecoin",
		"tbinanceusd":  "binance-usd",
		"binanceusd":   "binance-usd",
		"ttron":        "tron",
		"tron":         "tron",
		"tusdcerc20":   "tusdcerc20",
		"usdcerc20":    "usdcerc20",
	}
	if coin, ok := coinMap[coinName]; ok {
		return coin
	}
	return coinName
}
