package sentinel

import (
	"fmt"

	npool "github.com/NpoolPlatform/message/npool/scheduler/mw/v1/sentinel"
	// benefitwait "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait"
	// benefitwaittypes "github.com/NpoolPlatform/npool-scheduler/pkg/benefit/wait/types"
)

func triggerBenefitWait(req *npool.BenefitWait) {
	benefitwait.Trigger(&benefitwaittypes.TriggerCond{
		GoodIDs:  req.GetGoodIDs(),
		RewardAt: req.GetRewardAt(),
	})
}

func Trigger(req *npool.TriggerRequest) error {
	switch req.Subsystem {
	case "benefitwait":
		triggerBenefitWait(req.GetBenefitWait())
	default:
		return fmt.Errorf("invalid subsystem")
	}
	return nil
}
