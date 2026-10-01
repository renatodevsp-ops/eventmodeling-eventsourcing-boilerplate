package listwallets

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
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
	db      *pgxpool.Pool
}

func NewProjector(db *pgxpool.Pool) *Projector {
	return &Projector{wallets: make(map[string]*Wallet), db: db}
}

func (p *Projector) All() []Wallet {
	rows, err := p.db.Query(context.Background(), "SELECT wallet_id FROM wallets ORDER BY id")
	if err != nil {
		return nil
	}

	defer rows.Close()

	result := make([]Wallet, 0)
	for rows.Next() {
		var wallet Wallet
		if err := rows.Scan(&wallet.ID); err != nil {
			return nil
		}
		result = append(result, wallet)
	}
	if err := rows.Err(); err != nil {
		return nil
	}

	println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", result)
	return result
}

func (p *Projector) OnWalletOpened(ctx context.Context, e *events.WalletOpened) error {
	if _, err := p.db.Exec(ctx, "INSERT INTO wallets (wallet_id) VALUES ($1) ON CONFLICT (wallet_id) DO NOTHING", e.WalletID.String()); err != nil {
		return err
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
