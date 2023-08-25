package types

import (
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
)

type PersistentTx struct {
	*txmwpb.Tx
}
