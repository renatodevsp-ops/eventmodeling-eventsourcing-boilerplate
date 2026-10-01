package listwallets

import (
	"context"
	"sync"

	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"
	"github.com/terraskye/eventsourcing"
)

type Wallet struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
	Closed bool   `json:"closed"`
}

type Projector struct {
	mu      sync.RWMutex
	wallets map[string]*Wallet
}

func NewProjector() *Projector {
	return &Projector{wallets: make(map[string]*Wallet)}
}

func (p *Projector) All() []Wallet {

	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]Wallet, 0, len(p.wallets))
	for _, t := range p.wallets {
		result = append(result, *t)
	}
	return result
}

func (p *Projector) OnWalletOpened(_ context.Context, e *events.WalletOpened) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wallets[e.WalletID.String()] = &Wallet{
		ID:     e.WalletID.String(),
		Amount: e.Amount,
	}
	return nil
}

func (p *Projector) OnWalletMonthClosed(_ context.Context, e *events.WalletMonthClosed) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.wallets[e.WalletID.String()]; ok {
		t.Closed = true
	}
	return nil
}

func (p *Projector) EventHandlers() *eventsourcing.EventGroupProcessor {
	return eventsourcing.NewEventGroupProcessor(
		eventsourcing.OnEvent(p.OnWalletOpened),
		eventsourcing.OnEvent(p.OnWalletMonthClosed),
	)
}
