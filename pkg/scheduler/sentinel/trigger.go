package sentinel

import (
	"fmt"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/sentinel"
	benefitwait "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait"
	benefitwaittypes "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/types"
)

func triggerBenefitWait(req *npool.TriggerRequest) {
	cond := &benefitwaittypes.TriggerCond{}
	if goodID := req.GetGoodID(); goodID != "" {
		cond.GoodID = &goodID
	}
	if len(req.GetGoodIDs().GoodIDs) > 0 {
		cond.GoodIDs = &(req.GetGoodIDs().GoodIDs)
	}
	benefitwait.Trigger(cond)
}

func Trigger(req *npool.TriggerRequest) error {
	switch req.Subsystem {
	case "benefitwait":
		triggerBenefitWait(req)
	default:
		return fmt.Errorf("invalid subsystem")
	}
	return nil
}
