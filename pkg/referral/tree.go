package referral

import (
	"context"

	inspirecli "github.com/NpoolPlatform/cloud-hashing-inspire/pkg/client"
	inspirepb "github.com/NpoolPlatform/message/npool/cloud-hashing-inspire"
)

func GetReferrals(
	ctx context.Context, appID, userID string,
) (
	inviters []string, settings map[string][]*inspirepb.AppPurchaseAmountSetting, err error,
) {
	curUser := userID

	settings = map[string][]*inspirepb.AppPurchaseAmountSetting{}

	for {
		sets, err := inspirecli.GetAmountSettings(ctx, appID, curUser)
		if err != nil {
			return nil, nil, err
		}

		settings[curUser] = sets

		invitation, err := inspirecli.GetInvitation(ctx, appID, curUser)
		if err != nil {
			return nil, nil, err
		}
		if invitation == nil {
			break
		}

		inviters = append(inviters, invitation.InviterID)
		curUser = invitation.InviterID
	}

	return inviters, settings, nil
}
