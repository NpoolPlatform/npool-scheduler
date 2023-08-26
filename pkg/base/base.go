package base

import (
	"context"
	"fmt"
	"time"

	"github.com/NpoolPlatform/go-service-framework/pkg/action"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
	redis2 "github.com/NpoolPlatform/go-service-framework/pkg/redis"
	"github.com/NpoolPlatform/go-service-framework/pkg/watcher"
	basetypes "github.com/NpoolPlatform/message/npool/basetypes/v1"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/executor"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/notif"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/persistent"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

type Handler struct {
	persistent   chan interface{}
	notif        chan interface{}
	w            *watcher.Watcher
	sentinel     sentinel.Sentinel
	scanner      sentinel.Scanner
	executor     executor.Executor
	execer       executor.Exec
	persistenter persistent.Persistent
	persistentor persistent.Persistenter
	notifier     notif.Notif
	notify       notif.Notify
	subsystem    string
	scanInterval time.Duration
}

func (h *Handler) lockKey() string {
	return fmt.Sprintf("%v:%v", basetypes.Prefix_PrefixScheduler, h.subsystem)
}

func NewHandler(ctx context.Context, cancel context.CancelFunc, options ...func(*Handler)) (*Handler, error) {
	h := &Handler{}
	for _, opt := range options {
		opt(h)
	}
	if b := config.SupportSubsystem(h.subsystem); !b {
		return nil, nil
	}

	h.persistent = make(chan interface{})
	h.notif = make(chan interface{})

	h.sentinel = sentinel.NewSentinel(ctx, cancel, h.scanner, h.scanInterval)
	h.executor = executor.NewExecutor(ctx, cancel, h.persistent, h.notif, h.execer)
	h.persistenter = persistent.NewPersistent(ctx, cancel, h.persistentor)
	h.notifier = notif.NewNotif(ctx, cancel, h.notify)

	h.w = watcher.NewWatcher()

	if err := redis2.TryLock(h.lockKey(), 0); err != nil {
		logger.Sugar().Infow(
			"Initialize",
			"Subsystem", h.subsystem,
			"Error", err,
		)
		return nil, err
	}

	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", h.subsystem,
	)

	return h, nil
}

func WithSubsystem(subsystem string) func(*Handler) {
	return func(h *Handler) {
		h.subsystem = subsystem
	}
}

func WithScanner(scanner sentinel.Scanner) func(*Handler) {
	return func(h *Handler) {
		h.scanner = scanner
	}
}

func WithScanInterval(scanInterval time.Duration) func(*Handler) {
	return func(h *Handler) {
		h.scanInterval = scanInterval
	}
}

func WithExec(exec executor.Exec) func(*Handler) {
	return func(h *Handler) {
		h.execer = exec
	}
}

func WithPersistenter(persistenter persistent.Persistenter) func(*Handler) {
	return func(h *Handler) {
		h.persistentor = persistenter
	}
}

func WithNotify(notify notif.Notify) func(*Handler) {
	return func(h *Handler) {
		h.notify = notify
	}
}

func (h *Handler) Run(ctx context.Context, cancel context.CancelFunc) {
	action.Watch(ctx, cancel, h.run)
}

func (h *Handler) handler(ctx context.Context) bool {
	select {
	case ent := <-h.sentinel.Exec():
		h.executor.Feed(ent)
		return false
	case ent := <-h.persistent:
		h.persistenter.Feed(ent)
		return false
	case ent := <-h.notif:
		h.notifier.Feed(ent)
		return false
	case <-ctx.Done():
		logger.Sugar().Infow(
			"handler",
			"State", "Done",
			"Error", ctx.Err(),
		)
		close(h.w.ClosedChan())
		return true
	case <-h.w.CloseChan():
		close(h.w.ClosedChan())
		return true
	}
}

func (h *Handler) run(ctx context.Context) {
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func (h *Handler) Finalize() {
	if b := config.SupportSubsystem(h.subsystem); !b {
		return
	}
	_ = redis2.Unlock(h.lockKey())
	if h.w != nil {
		h.w.Shutdown()
	}
	h.sentinel.Finalize()
	h.executor.Finalize()
	h.persistenter.Finalize()
	h.notifier.Finalize()
	close(h.persistent)
	close(h.notif)
	logger.Sugar().Infow(
		"Finalize",
		"Subsystem", h.subsystem,
	)
}
