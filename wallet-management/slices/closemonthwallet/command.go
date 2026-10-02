package closemonthwallet

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"
	"github.com/terraskye/eventsourcing"
)

type CloseMonthWallet struct {
	WalletID uuid.UUID
	Month    int
	Year     int
	ClosedBy uuid.UUID `json:"completed_by"`
	ClosedAt time.Time `json:"completed_at"`
}

func (c CloseMonthWallet) AggregateID() string { return c.WalletID.String() }

func (c CloseMonthWallet) CommandType() string { return "CloseMonthWallet" }

type walletState struct {
	Exists      bool
	MonthClosed bool
}

var initialState = func() walletState { return walletState{} }

func evolve(state walletState, envelope *eventsourcing.Envelope) walletState {
	switch envelope.Event.(type) {
	case *events.WalletOpened:
		return walletState{Exists: true, MonthClosed: false}
	case *events.WalletMonthClosed:
		return walletState{Exists: true, MonthClosed: true}
	}
	return state
}

func decide(state walletState, cmd CloseMonthWallet) ([]eventsourcing.Event, error) {
	if !state.Exists {
		return nil, fmt.Errorf("wallet %s does not exist", cmd.WalletID)
	}
	if state.MonthClosed {
		return nil, fmt.Errorf("wallet %s is already closed", cmd.WalletID)
	}

	return []eventsourcing.Event{
		&events.WalletMonthClosed{
			WalletID: cmd.WalletID,
			Month:    cmd.Month,
			Year:     cmd.Year,
			ClosedBy: cmd.ClosedBy,
			ClosedAt: time.Now(),
		},
	}, nil
}

func NewHandler(store eventsourcing.EventStore) eventsourcing.CommandHandler[CloseMonthWallet] {
	return eventsourcing.NewCommandHandler(store,
		initialState,
		evolve,
		decide,
		eventsourcing.WithStreamState(eventsourcing.StreamExists{}),
		eventsourcing.WithRetryStrategy(
			backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3),
		))
}
