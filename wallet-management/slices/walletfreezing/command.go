package walletfreezing

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"
	"github.com/terraskye/eventsourcing"
)

type FreezeWallet struct {
	WalletID uuid.UUID
}

func (c FreezeWallet) AggregateID() string { return c.WalletID.String() }

func (c FreezeWallet) CommandType() string { return "FreezeWallet" }

type walletState struct {
	Closed   bool
	Archived bool
}

var initialState = func() walletState { return walletState{} }

func evolve(state walletState, envelope *eventsourcing.Envelope) walletState {
	switch envelope.Event.(type) {
	case *events.WalletMonthClosed:
		return walletState{Closed: true}
	case *events.WalletFrozen:
		return walletState{Closed: true, Archived: true}
	}
	return state
}

func decide(state walletState, cmd FreezeWallet) ([]eventsourcing.Event, error) {

	if !state.Closed {
		return nil, fmt.Errorf("wallet %s is not closed", cmd.WalletID)
	}
	if state.Archived {
		return nil, nil
	}

	return []eventsourcing.Event{
		&events.WalletFrozen{
			WalletID:   cmd.WalletID,
			ArchivedAt: time.Now(),
		},
	}, nil
}

func NewHandler(store eventsourcing.EventStore) eventsourcing.CommandHandler[FreezeWallet] {
	return eventsourcing.NewCommandHandler(store, initialState, evolve, decide)
}
