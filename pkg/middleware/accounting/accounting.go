package accounting

import (
	"context"
	"time"
	// goodspb "github.com/NpoolPlatform/cloud-hashing-goods/message/npool"
)

type accounting struct {
	ticker              *time.Ticker
	queryGoods          chan struct{}
	queryAccounts       chan struct{}
	queryBalance        chan struct{}
	queryOrders         chan struct{}
	caculateUserBenefit chan struct{}
	persistentResult    chan struct{}
	// goods               []*goodspb.GoodInfo
}

func (ac *accounting) onScheduleTick() {
	go func() { ac.queryGoods <- struct{}{} }()
}

func (ac *accounting) onQueryGoods(ctx context.Context) {
}

func (ac *accounting) onQueryAccounts(ctx context.Context) {
}

func (ac *accounting) onQueryBalance(ctx context.Context) {
}

func (ac *accounting) onQueryOrders(ctx context.Context) {
}

func (ac *accounting) onCaculateUserBenefit(ctx context.Context) {
}

func (ac *accounting) onPersistentResult(ctx context.Context) {
}

func Run(ctx context.Context) {
	ac := &accounting{
		ticker: time.NewTicker(30 * time.Second),
	}

	for {
		select {
		case <-ac.ticker.C:
			ac.onScheduleTick()
		case <-ac.queryGoods:
			ac.onQueryGoods(ctx)
		case <-ac.queryAccounts:
			ac.onQueryAccounts(ctx)
		case <-ac.queryBalance:
			ac.onQueryBalance(ctx)
		case <-ac.queryOrders:
			ac.onQueryOrders(ctx)
		case <-ac.caculateUserBenefit:
			ac.onCaculateUserBenefit(ctx)
		case <-ac.persistentResult:
			ac.onPersistentResult(ctx)
		}
	}
}
