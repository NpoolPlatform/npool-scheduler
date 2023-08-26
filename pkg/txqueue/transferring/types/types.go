package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
)

type PersistentTx struct {
	*txmwpb.Tx
	NewTxState basetypes.TxState
	TxExtra    string
	TxCID      string
}
