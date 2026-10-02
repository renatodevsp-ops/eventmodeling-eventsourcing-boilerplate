package walletopening

import (
	"context"
	"testing"

	"github.com/google/uuid"
	memstore "github.com/terraskye/eventsourcing/eventstore/memory"

	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"
)

func TestDecide_NewWallet(t *testing.T) {
	state := walletState{Exists: false}
	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    100,
		CreatedBy: uuid.New(),
	}
	evts, err := decide(state, cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	created, ok := evts[0].(*events.WalletOpened)
	if !ok {
		t.Fatal("expected WalletOpened")
	}
	if created.AggregateID() != cmd.WalletID.String() {
		t.Fatalf("expected aggregate ID %s, got %s", cmd.WalletID.String(), created.AggregateID())
	}
}

func TestDecide_DuplicateWallet(t *testing.T) {
	state := walletState{Exists: true}
	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    100,
		CreatedBy: uuid.New(),
	}
	_, err := decide(state, cmd)
	if err == nil {
		t.Fatal("expected error for duplicate wallet")
	}
}

func TestDecide_InvalidAmount(t *testing.T) {
	state := walletState{Exists: false}
	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    0,
		CreatedBy: uuid.New(),
	}
	_, err := decide(state, cmd)
	if err == nil {
		t.Fatal("expected error for invalid amount")
	}
}

func TestCommandHandler_Success(t *testing.T) {
	store := memstore.NewMemoryStore(10)
	defer store.Close()

	handler := NewHandler(store)
	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    100,
		CreatedBy: uuid.New(),
	}

	result, err := handler(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Successful {
		t.Error("expected successful result")
	}
	if result.NextExpectedVersion != 1 {
		t.Errorf("expected version 1, got %d", result.NextExpectedVersion)
	}
}

func TestCommandHandler_DuplicateWallet(t *testing.T) {
	store := memstore.NewMemoryStore(10)
	defer store.Close()

	handler := NewHandler(store)
	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    100,
		CreatedBy: uuid.New(),
	}

	if _, err := handler(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}

	_, err := handler(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected error on duplicate wallet")
	}
}

func TestCommandHandler_InvalidAmount(t *testing.T) {
	store := memstore.NewMemoryStore(10)
	defer store.Close()

	handler := NewHandler(store)
	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    -50,
		CreatedBy: uuid.New(),
	}

	_, err := handler(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected error for invalid amount")
	}
}
