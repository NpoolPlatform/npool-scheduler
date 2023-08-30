package created

import (
	"context"
	"sync"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/txqueue/created/sentinel"
)

const subsystem = "txqueuecreated"

var (
	h     *base.Handler
	mutex sync.Mutex
)

func Initialize(ctx context.Context, cancel context.CancelFunc) {
	_h, err := base.NewHandler(
		ctx,
		cancel,
		base.WithSubsystem(subsystem),
		base.WithScanInterval(time.Minute),
		base.WithScanner(sentinel.NewSentinel(&mutex)),
		base.WithExec(executor.NewExecutor(&mutex)),
		base.WithPersistenter(persistent.NewPersistent(&mutex)),
	)
	if err != nil || _h == nil {
		logger.Sugar().Errorw(
			"Initialize",
			"Subsystem", subsystem,
			"Error", err,
		)
		return
	}

	h = _h
	go h.Run(ctx, cancel)
}

func Finalize() {
	if h != nil {
		h.Finalize()
	}
}
