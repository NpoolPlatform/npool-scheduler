package common

func MapCoin(coinName string) string {
	coinMap := map[string]string{
		"fil":        "filecoin",
		"filecoin":   "filecoin",
		"tfilecoin":  "filecoin",
		"btc":        "bitcoin",
		"bitcoin":    "bitcoin",
		"tbitcoin":   "bitcoin",
		"tethereum":  "ethereum",
		"eth":        "ethereum",
		"ethereum":   "ethereum",
		"tusdt":      "tether",
		"usdt":       "tether",
		"tusdterc20": "tether",
		"usdterc20":  "tether",
		"tusdttrc20": "tether",
		"usdttrc20":  "tether",
		"sol":        "solana",
		"solana":     "solana",
		"tsolana":    "solana",
	}
	if coin, ok := coinMap[coinName]; ok {
		return coin
	}
	return coinName
}

func PriceCoin(coinName string) bool {
	priceMap := map[string]string{
		"tusdt":      "tether",
		"usdt":       "tether",
		"tusdterc20": "tether",
		"usdterc20":  "tether",
		"tusdttrc20": "tether",
		"usdttrc20":  "tether",
	}
	_, ok := priceMap[coinName]
	return ok
}
