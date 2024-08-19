package types

import (
	miningstockmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/good/stock"
	goodpowerrentalmwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/powerrental"
	goodusermwpb "github.com/NpoolPlatform/message/npool/miningpool/mw/v1/gooduser"
)

type PersistentGoodPowerRental struct {
	*goodpowerrentalmwpb.PowerRental
	MiningGoodStockReqs []*miningstockmwpb.MiningGoodStockReq
	GoodUserReqs        []*goodusermwpb.GoodUserReq
}
