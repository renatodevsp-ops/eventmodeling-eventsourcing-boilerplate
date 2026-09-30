package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/renatodevsp-ops/iron-ledger/wallet-management/slices/closemonthwallet"
	"github.com/renatodevsp-ops/iron-ledger/wallet-management/slices/listwallets"
	"github.com/renatodevsp-ops/iron-ledger/wallet-management/slices/walletfreezing"
	"github.com/renatodevsp-ops/iron-ledger/wallet-management/slices/walletopening"
	membus "github.com/terraskye/eventsourcing/eventbus/memory"
	memstore "github.com/terraskye/eventsourcing/eventstore/memory"
)

func main() {
	store := memstore.NewMemoryStore(100)
	defer store.Close()

	bus := membus.NewEventBus(100)
	defer bus.Close()

	projector := listwallets.NewProjector()
	if err := bus.Subscribe(context.Background(), "wallet-list-projector", projector.EventHandlers()); err != nil {
		log.Fatal(err)
	}
	go func() {
		for env := range store.Events() {
			bus.Dispatch(env)
		}
	}()

	openWalletHTTP := walletopening.NewHTTPHandler(walletopening.NewHandler(store))
	closeMonthWalletHTTP := closemonthwallet.NewHTTPHandler(closemonthwallet.NewHandler(store))
	listWalletsHTTP := listwallets.NewHTTPHandler(listwallets.NewQueryHandler(projector))

	r := gin.Default()
	wallets := r.Group("/api/v1/wallets")
	openWalletHTTP.RegisterRoutes(wallets)
	closeMonthWalletHTTP.RegisterRoutes(wallets)
	listWalletsHTTP.RegisterRoutes(wallets)

	freezeWalletHandler := walletfreezing.NewHandler(store)
	freezeWalletProcessor := walletfreezing.NewProcessor(freezeWalletHandler, 5*time.Second)

	bus.Subscribe(context.Background(), "freeze-wallet-processor", freezeWalletProcessor.EventHandlers())

	log.Fatal(r.Run(":8080"))
}
