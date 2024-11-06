package types

import (
	miningstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/stock"
	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
)

type PersistentGoodPowerRental struct {
	*goodpowerrentalmwpb.PowerRental
	MiningGoodStockReqs []*miningstockmwpb.MiningGoodStockReq
}
