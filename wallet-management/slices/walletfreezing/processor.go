package walletfreezing

import (
	"context"
	"log"
	"time"

	"github.com/renatodevsp-ops/iron-ledger/wallet-management/events"
	"github.com/terraskye/eventsourcing"
)

type Processor struct {
	handler eventsourcing.CommandHandler[FreezeWallet]
	delay   time.Duration
}

func NewProcessor(handler eventsourcing.CommandHandler[FreezeWallet], delay time.Duration) *Processor {
	return &Processor{handler: handler, delay: delay}
}

func (p *Processor) OnWalletMonthClosed(ctx context.Context, e *events.WalletMonthClosed) error {
	go func() {

		select {
		case <-time.After(p.delay):
			cmd := FreezeWallet{WalletID: e.WalletID}
			if _, err := p.handler(context.Background(), cmd); err != nil {
				log.Printf("freeze wallet %s: %v", e.WalletID, err)
			}
		case <-ctx.Done():
			// Subscription cancelled; skip
		}
	}()

	return nil
}

func (p *Processor) EventHandlers() *eventsourcing.EventGroupProcessor {
	return eventsourcing.NewEventGroupProcessor(
		eventsourcing.OnEvent(p.OnWalletMonthClosed),
	)
}
