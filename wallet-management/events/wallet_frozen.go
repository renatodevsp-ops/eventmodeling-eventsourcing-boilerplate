package events

import (
	"time"

	"github.com/google/uuid"
)

type WalletFrozen struct {
	WalletID   uuid.UUID `json:"wallet_id"`
	ArchivedAt time.Time `json:"archived_at"`
}

func (e *WalletFrozen) AggregateID() string { return e.WalletID.String() }

func (e *WalletFrozen) EventType() string { return "WalletFrozen" }
