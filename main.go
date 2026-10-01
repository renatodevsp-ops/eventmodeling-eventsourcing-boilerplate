package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/events"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/slices/closemonthwallet"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/slices/listwallets"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/slices/walletfreezing"
	"github.com/renatodevsp-ops/eventmodeling-eventsourcing-boilerplate/wallet-management/slices/walletopening"
	"github.com/terraskye/eventsourcing"
	bus "github.com/terraskye/eventsourcing/eventbus/postgres"
	store "github.com/terraskye/eventsourcing/eventstore/postgres"
)

func main() {
	ctx := context.Background()

	_ = godotenv.Load()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	eventStore := store.NewEventStore(pool)
	defer eventStore.Close()
	eventsourcing.RegisterEvent(&events.WalletOpened{})
	eventsourcing.RegisterEvent(&events.WalletMonthClosed{})
	eventsourcing.RegisterEvent(&events.WalletFrozen{})

	eventBus := bus.NewEventBus(pool, 3*time.Second)
	defer eventBus.Close()

	projector := listwallets.NewProjector()
	if err := eventBus.Subscribe(ctx, "wallet-list-projector", projector.EventHandlers()); err != nil {
		log.Fatal(err)
	}

	openWalletHTTP := walletopening.NewHTTPHandler(walletopening.NewHandler(eventStore))
	closeMonthWalletHTTP := closemonthwallet.NewHTTPHandler(closemonthwallet.NewHandler(eventStore))
	listWalletsHTTP := listwallets.NewHTTPHandler(listwallets.NewQueryHandler(projector))

	r := gin.Default()
	wallets := r.Group("/api/v1/wallets")
	openWalletHTTP.RegisterRoutes(wallets)
	closeMonthWalletHTTP.RegisterRoutes(wallets)
	listWalletsHTTP.RegisterRoutes(wallets)

	freezeWalletHandler := walletfreezing.NewHandler(eventStore)
	freezeWalletProcessor := walletfreezing.NewProcessor(freezeWalletHandler, 5*time.Second)

	if err := eventBus.Subscribe(ctx, "freeze-wallet-processor", freezeWalletProcessor.EventHandlers()); err != nil {
		log.Fatal(err)
	}

	log.Fatal(r.Run(":8080"))
}
