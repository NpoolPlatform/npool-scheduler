package types

import (
	goodpledgemwpb "github.com/NpoolPlatform/message/npool/good/mw/v1/pledge"
)

type PersistentGoodPledge struct {
	*goodpledgemwpb.Pledge
}
