package types

import (
	withdrawreviewnotifypb "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/withdraw/review/notify"
)

type PersistentWithdrawReviewNotify struct {
	AppWithdraws []*withdrawreviewnotifypb.AppWithdrawInfos
}
