package pubsub

import (
	"context"
	"fmt"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/go-service-framework/pkg/pubsub"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	"github.com/NpoolPlatform/npool-scheduler/pkg/db"
	"github.com/NpoolPlatform/npool-scheduler/pkg/db/ent"
	entpubsubmsg "github.com/NpoolPlatform/npool-scheduler/pkg/db/ent/pubsubmessage"
	benefitnotif "github.com/NpoolPlatform/npool-scheduler/pkg/pubsub/benefit/notif"
	depositnotif "github.com/NpoolPlatform/npool-scheduler/pkg/pubsub/deposit/notif"
	orderpaidnotif "github.com/NpoolPlatform/npool-scheduler/pkg/pubsub/order/paid/notif"
	withdrawnotif "github.com/NpoolPlatform/npool-scheduler/pkg/pubsub/withdraw/notif"

	"github.com/google/uuid"
)

var (
	subscriber *pubsub.Subscriber
	publisher  *pubsub.Publisher
)

// TODO: here we should call from DB transaction context
func finish(ctx context.Context, msg *pubsub.Msg, err error) error {
	state := basetypes.MsgState_StateSuccess
	if err != nil {
		state = basetypes.MsgState_StateFail
	}

	return db.WithClient(ctx, func(_ctx context.Context, cli *ent.Client) error {
		c := cli.
			PubsubMessage.
			Create().
			SetID(msg.UID).
			SetMessageID(msg.MID).
			SetArguments(msg.Body).
			SetState(state.String())
		if msg.RID != nil {
			c.SetRespToID(*msg.RID)
		}
		if msg.UnID != nil {
			c.SetUndoID(*msg.UnID)
		}
		_, err = c.Save(ctx)
		return err
	})
}

//nolint
func prepare(mid, body string) (req interface{}, err error) {
	switch mid {
	case basetypes.MsgID_DepositReceivedReq.String():
		req, err = depositnotif.Prepare(body)
	case basetypes.MsgID_WithdrawRequestReq.String():
		fallthrough //nolint
	case basetypes.MsgID_WithdrawSuccessReq.String():
		req, err = withdrawnotif.Prepare(body)
	case basetypes.MsgID_CreateGoodBenefitReq.String():
		req, err = benefitnotif.Prepare(body)
	case basetypes.MsgID_OrderPaidReq.String():
		req, err = orderpaidnotif.Prepare(body)
	default:
		return nil, nil
	}

	if err != nil {
		logger.Sugar().Errorw(
			"handler",
			"MID", mid,
			"Body", body,
		)
		return nil, err
	}

	return req, nil
}

// Query a request message
//  Return
//   bool   appliable == true, caller should go ahead to apply this message
//   error  error message
func statReq(ctx context.Context, mid string, uid uuid.UUID) (bool, error) { //nolint
	var err error

	err = db.WithClient(ctx, func(_ctx context.Context, cli *ent.Client) error {
		_, err = cli.
			PubsubMessage.
			Query().
			Where(
				entpubsubmsg.ID(uid),
			).
			Only(_ctx)
		return err
	})

	switch err {
	case nil:
	default:
		if ent.IsNotFound(err) {
			return true, nil
		}
		logger.Sugar().Warnw(
			"stat",
			"MID", mid,
			"UID", uid,
			"Error", err,
		)
		return false, err
	}

	return false, nil
}

// Query a message state in database
//  Return
//   bool    appliable == true, caller should go ahead to apply this message
//   error   error message
func statMsg(ctx context.Context, mid string, uid uuid.UUID, rid *uuid.UUID) (bool, error) { //nolint
	switch mid { //nolint
	case basetypes.MsgID_DepositReceivedReq.String():
		fallthrough //nolint
	case basetypes.MsgID_WithdrawRequestReq.String():
		fallthrough //nolint
	case basetypes.MsgID_CreateGoodBenefitReq.String():
		fallthrough //nolint
	case basetypes.MsgID_WithdrawSuccessReq.String():
		fallthrough //nolint
	case basetypes.MsgID_OrderPaidReq.String():
		return statReq(ctx, mid, uid)
	default:
		return false, fmt.Errorf("invalid message")
	}
}

// Stat if message in right status, and is appliable
//  Return
//   bool    appliable == true, the message needs to be applied
//   error   error happens
func stat(ctx context.Context, mid string, uid uuid.UUID, rid *uuid.UUID) (bool, error) {
	return statMsg(ctx, mid, uid, rid)
}

// Process will consume the message and return consuming state
//  Return
//   error   reason of error, if nil, means the message should be acked
//nolint
func process(ctx context.Context, mid string, uid uuid.UUID, req interface{}) (err error) {
	switch mid {
	case basetypes.MsgID_DepositReceivedReq.String():
		err = depositnotif.Apply(ctx, req)
	case basetypes.MsgID_WithdrawRequestReq.String():
		fallthrough //nolint
	case basetypes.MsgID_WithdrawSuccessReq.String():
		err = withdrawnotif.Apply(ctx, mid, req)
	case basetypes.MsgID_CreateGoodBenefitReq.String():
		err = benefitnotif.Apply(ctx, req)
	case basetypes.MsgID_OrderPaidReq.String():
		err = orderpaidnotif.Apply(ctx, req)
	default:
		return nil
	}
	return err
}

// No matter what handler return, the message will be acked, unless handler halt
// If handler halt, the service will be restart, all message will be requeue
func handler(ctx context.Context, msg *pubsub.Msg) (err error) {
	var req interface{}
	var appliable bool

	defer func(req *interface{}, appliable *bool) {
		msg.Ack()
		if *req != nil && *appliable {
			_ = finish(ctx, msg, err) //nolint
		}
	}(&req, &appliable)

	req, err = prepare(msg.MID, msg.Body)
	if err != nil {
		return err
	}
	if req == nil {
		return nil
	}

	appliable, err = stat(ctx, msg.MID, msg.UID, msg.RID)
	if err != nil {
		return err
	}
	if !appliable {
		return nil
	}

	err = process(ctx, msg.MID, msg.UID, req)
	return err
}

func Subscribe(ctx context.Context) (err error) {
	subscriber, err = pubsub.NewSubscriber()
	if err != nil {
		return err
	}

	publisher, err = pubsub.NewPublisher()
	if err != nil {
		return err
	}

	return subscriber.Subscribe(ctx, handler)
}

func Shutdown(ctx context.Context) error {
	if subscriber != nil {
		subscriber.Close()
	}
	if publisher != nil {
		publisher.Close()
	}

	return nil
}
