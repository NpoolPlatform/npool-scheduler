package base

import (
	"context"
	"fmt"
	"sync"
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
	persistent     chan interface{}
	notif          chan interface{}
	done           chan interface{}
	w              *watcher.Watcher
	sentinel       sentinel.Sentinel
	scanner        sentinel.Scanner
	executors      []executor.Executor
	execer         executor.Exec
	executorNumber int
	executorIndex  int
	persistenter   persistent.Persistent
	persistentor   persistent.Persistenter
	notifier       notif.Notif
	notify         notif.Notify
	subsystem      string
	scanInterval   time.Duration
	running        *sync.Map
}

func (h *Handler) lockKey() string {
	return fmt.Sprintf("%v:%v", basetypes.Prefix_PrefixScheduler, h.subsystem)
}

func NewHandler(ctx context.Context, cancel context.CancelFunc, options ...func(*Handler)) (*Handler, error) {
	h := &Handler{
		executorNumber: 1,
	}
	for _, opt := range options {
		opt(h)
	}
	if b := config.SupportSubsystem(h.subsystem); !b {
		return nil, nil
	}
	if h.running == nil {
		return nil, fmt.Errorf("invalid running map")
	}

	h.persistent = make(chan interface{})
	h.notif = make(chan interface{})
	h.done = make(chan interface{})

	h.sentinel = sentinel.NewSentinel(ctx, cancel, h.scanner, h.scanInterval, h.subsystem)
	for i := 0; i < h.executorNumber; i++ {
		h.executors = append(h.executors, executor.NewExecutor(ctx, cancel, h.persistent, h.notif, h.execer, h.subsystem))
	}
	h.persistenter = persistent.NewPersistent(ctx, cancel, h.notif, h.done, h.persistentor, h.subsystem)
	h.notifier = notif.NewNotif(ctx, cancel, h.notify, h.subsystem)

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

func WithExecutorNumber(n int) func(*Handler) {
	return func(h *Handler) {
		h.executorNumber = n
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

func WithRunningMap(m *sync.Map) func(*Handler) {
	return func(h *Handler) {
		h.running = m
	}
}

func (h *Handler) Run(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(h.subsystem); !b {
		return
	}
	action.Watch(ctx, cancel, h.run)
}

func (h *Handler) execEnt(ent interface{}) {
	h.executors[h.executorIndex].Feed(ent)
	h.executorIndex += 1
	h.executorIndex %= len(h.executors)
}

func (h *Handler) handler(ctx context.Context) bool {
	select {
	case ent := <-h.sentinel.Exec():
		if _, loaded := h.running.LoadOrStore(h.scanner.ObjectID(ent), true); loaded {
			return false
		}
		h.execEnt(ent)
		return false
	case ent := <-h.persistent:
		h.persistenter.Feed(ent)
		return false
	case ent := <-h.notif:
		h.notifier.Feed(ent)
		return false
	case ent := <-h.done:
		h.running.Delete(h.scanner.ObjectID(ent))
		return false
	case <-h.w.CloseChan():
		logger.Sugar().Infow(
			"handler",
			"State", "Close",
			"Subsystem", h.subsystem,
			"Error", ctx.Err(),
		)
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

func (h *Handler) Trigger(cond interface{}) {
	h.sentinel.Trigger(cond)
}

func (h *Handler) Finalize() {
	if b := config.SupportSubsystem(h.subsystem); !b {
		return
	}
	_ = redis2.Unlock(h.lockKey())
	h.sentinel.Finalize()
	if h.w != nil {
		h.w.Shutdown()
	}
	for _, e := range h.executors {
		e.Finalize()
	}
	h.persistenter.Finalize()
	h.notifier.Finalize()
	logger.Sugar().Infow(
		"Finalize",
		"Subsystem", h.subsystem,
	)
}
