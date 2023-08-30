package types

import (
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	txmwpb "github.com/NpoolPlatform/message/npool/chain/mw/v1/tx"
)

type PersistentTx struct {
	*txmwpb.Tx
	TransactionExist bool
	CoinName         string
	Amount           string
	FloatAmount      float64
	FromAddress      string
	ToAddress        string
	AccountMemo      *string
	NewTxState       basetypes.TxState
	Error            error
}
