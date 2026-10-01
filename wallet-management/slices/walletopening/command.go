package walletopening

import (
	"fmt"
	"time"

	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"

	"github.com/google/uuid"
	"github.com/terraskye/eventsourcing"
)

type OpenWallet struct {
	WalletID  uuid.UUID
	Amount    int
	CreatedBy uuid.UUID
}

func (c OpenWallet) AggregateID() string { return c.WalletID.String() }

func (c OpenWallet) CommandType() string { return "OpenWallet" }

type walletState struct {
	Exists bool
}

var initialState = func() walletState {
	return walletState{}
}

func evolve(state walletState, envelope *eventsourcing.Envelope) walletState {
	switch envelope.Event.(type) {
	case *events.WalletOpened:
		return walletState{Exists: true}
	}
	return state
}

func decide(state walletState, cmd OpenWallet) ([]eventsourcing.Event, error) {
	if state.Exists {
		return nil, fmt.Errorf("wallet %s already exists", cmd.WalletID)
	}
	if cmd.Amount <= 0 {
		return nil, fmt.Errorf("amount must be greater than 0")
	}

	return []eventsourcing.Event{
		&events.WalletOpened{
			WalletID:  cmd.WalletID,
			Amount:    cmd.Amount,
			CreatedBy: cmd.CreatedBy,
			CreatedAt: time.Now(),
		},
	}, nil
}

func NewHandler(store eventsourcing.EventStore) eventsourcing.CommandHandler[OpenWallet] {
	return eventsourcing.NewCommandHandler(store, initialState, evolve, decide)
}
