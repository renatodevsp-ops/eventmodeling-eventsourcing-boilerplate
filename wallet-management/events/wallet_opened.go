package events

import (
	"time"

	"github.com/google/uuid"
)

type WalletOpened struct {
	WalletID  uuid.UUID `json:"wallet_id"`
	Amount    int       `json:"amount"`
	CreatedBy uuid.UUID `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}

func (e *WalletOpened) AggregateID() string { return e.WalletID.String() }

func (e *WalletOpened) EventType() string { return "WalletOpened" }
