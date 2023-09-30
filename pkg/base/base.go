package base

import (
	"context"
	"fmt"
	"math"
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
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/retry"
	"github.com/NpoolPlatform/npool-scheduler/pkg/base/sentinel"
	"github.com/NpoolPlatform/npool-scheduler/pkg/config"
)

type idDesc struct {
	id        string
	subsystem string
	start     time.Time
}

type syncMap struct {
	*sync.Map
	count      int
	concurrent int
	subsystem  string
}

func (s *syncMap) Store(key, value interface{}) (bool, bool) { //nolint
	desc := &idDesc{
		id:        key.(string),
		subsystem: s.subsystem,
		start:     time.Now(),
	}
	_desc, loaded := s.Map.LoadOrStore(key, desc)
	if !loaded {
		if s.count >= s.concurrent && s.concurrent < math.MaxInt {
			s.Map.Delete(key)
			return false, true
		}
		s.count++
		return false, false
	}
	if time.Now().After(_desc.(*idDesc).start.Add(1 * time.Minute)) {
		desc.start = time.Now()
		s.Map.Store(key, desc)
		logger.Sugar().Warnw(
			"Store",
			"Ent", value,
			"ID", _desc.(*idDesc).id,
			"StoreSubsystem", _desc.(*idDesc).subsystem,
			"Start", _desc.(*idDesc).start,
			"Subsystem", s.subsystem,
			"Count", s.count,
			"State", "Processing",
		)
	}
	return true, false
}

func (s *syncMap) Delete(key interface{}) {
	desc, ok := s.Map.LoadAndDelete(key)
	if !ok {
		return
	}
	if time.Now().Sub(desc.(*idDesc).start).Seconds() > 10 { //nolint
		logger.Sugar().Warnw(
			"Delete",
			"ID", desc.(*idDesc).id,
			"StoreSubsystem", desc.(*idDesc).subsystem,
			"Start", desc.(*idDesc).start,
			"Elapsed", time.Since(desc.(*idDesc).start),
		)
	}
	s.count--
}

type Handler struct {
	persistent        chan interface{}
	notif             chan interface{}
	done              chan interface{}
	w                 *watcher.Watcher
	sentinel          sentinel.Sentinel
	scanner           sentinel.Scanner
	executors         []executor.Executor
	execer            executor.Exec
	executorNumber    int
	executorIndex     int
	persistenter      persistent.Persistent
	persistentor      persistent.Persistenter
	notifier          notif.Notif
	notify            notif.Notify
	subsystem         string
	scanInterval      time.Duration
	running           *syncMap
	runningConcurrent int
	locked            bool
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
	if h.runningConcurrent > 0 {
		h.running.concurrent = h.runningConcurrent
	}

	h.persistent = make(chan interface{})
	h.notif = make(chan interface{})
	h.done = make(chan interface{})

	h.sentinel = sentinel.NewSentinel(ctx, cancel, h.scanner, h.scanInterval, h.subsystem)
	for i := 0; i < h.executorNumber; i++ {
		h.executors = append(h.executors, executor.NewExecutor(ctx, cancel, h.persistent, h.notif, h.done, h.execer, h.subsystem))
	}
	h.persistenter = persistent.NewPersistent(ctx, cancel, h.notif, h.done, h.persistentor, h.subsystem)
	h.notifier = notif.NewNotif(ctx, cancel, h.notify, h.subsystem)

	h.w = watcher.NewWatcher()
	if err := redis2.TryLock(h.lockKey(), 0); err == nil {
		h.locked = true
	}

	logger.Sugar().Infow(
		"Initialize",
		"Subsystem", h.subsystem,
		"Locked", h.locked,
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
		h.running = &syncMap{
			Map:        m,
			concurrent: 3,
			subsystem:  h.subsystem,
		}
	}
}

func WithRunningConcurrent(concurrent int) func(*Handler) {
	return func(h *Handler) {
		h.runningConcurrent = concurrent
	}
}

func (h *Handler) Run(ctx context.Context, cancel context.CancelFunc) {
	if b := config.SupportSubsystem(h.subsystem); !b {
		return
	}
	action.Watch(ctx, cancel, h.run, h.paniced)
}

func (h *Handler) execEnt(ctx context.Context, ent interface{}) {
	h.executors[h.executorIndex].Feed(ctx, ent)
	h.executorIndex++
	h.executorIndex %= len(h.executors)
}

func (h *Handler) handler(ctx context.Context) bool {
	select {
	case ent := <-h.sentinel.Exec():
		if loaded, overflow := h.running.Store(h.scanner.ObjectID(ent), ent); loaded || overflow {
			if overflow {
				// Here is a bit strange, but let's use sentinel exec firstly
				retry.Retry(ctx, ent, h.sentinel.Exec())
			}
			return false
		}
		h.execEnt(ctx, ent)
		return false
	case ent := <-h.persistent:
		h.persistenter.Feed(ctx, ent)
		return false
	case ent := <-h.notif:
		h.notifier.Feed(ctx, ent)
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

func (h *Handler) retryLock(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			if err := redis2.TryLock(h.lockKey(), 0); err == nil {
				h.locked = true
				return
			}
		}
	}
}

func (h *Handler) run(ctx context.Context) {
	if !h.locked {
		h.retryLock(ctx)
	}
	if !h.locked {
		close(h.w.ClosedChan())
		return
	}
	for {
		if b := h.handler(ctx); b {
			break
		}
	}
}

func (h *Handler) paniced(ctx context.Context) {
	logger.Sugar().Errorw(
		"Paniced",
		"Subsystem", h.subsystem,
	)
	close(h.w.ClosedChan())
}

func (h *Handler) Trigger(cond interface{}) {
	h.sentinel.Trigger(cond)
}

func (h *Handler) Finalize(ctx context.Context) {
	if b := config.SupportSubsystem(h.subsystem); !b {
		return
	}
	if h.locked {
		_ = redis2.Unlock(h.lockKey()) //nolint
	}
	h.sentinel.Finalize(ctx)
	if h.w != nil {
		h.w.Shutdown(ctx)
	}
	for _, e := range h.executors {
		e.Finalize(ctx)
	}
	h.persistenter.Finalize(ctx)
	h.notifier.Finalize(ctx)
	logger.Sugar().Infow(
		"Finalize",
		"Subsystem", h.subsystem,
	)
}
