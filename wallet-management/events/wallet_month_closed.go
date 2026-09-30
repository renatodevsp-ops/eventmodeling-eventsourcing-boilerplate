package events

import (
	"time"

	"github.com/google/uuid"
)

type WalletMonthClosed struct {
	WalletID uuid.UUID `json:"wallet_id"`
	Month    int       `json:"month"`
	Year     int       `json:"year"`
	ClosedBy uuid.UUID `json:"closed_by"`
	ClosedAt time.Time `json:"closed_at"`
}

func (e *WalletMonthClosed) AggregateID() string { return e.WalletID.String() }

func (e *WalletMonthClosed) EventType() string { return "WalletMonthClosed" }
