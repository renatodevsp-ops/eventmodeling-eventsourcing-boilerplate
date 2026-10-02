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
	"github.com/terraskye/eventsourcing/otel"
	otelglobal "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func main() {
	ctx := context.Background()

	_ = godotenv.Load()

	tp, err := initTracer()
	if err != nil {
		log.Fatal(err)
	}
	otelglobal.SetTracerProvider(tp)
	otelglobal.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	eventStore := store.NewEventStore(pool)
	tracedEventStore := otel.WithEventStoreTelemetry(eventStore, otel.WithOperation("EventStore"))
	defer eventStore.Close()

	eventsourcing.RegisterEvent(&events.WalletOpened{})
	eventsourcing.RegisterEvent(&events.WalletMonthClosed{})
	eventsourcing.RegisterEvent(&events.WalletFrozen{})

	eventBus := bus.NewEventBus(pool, 3*time.Second)
	defer eventBus.Close()

	projector := listwallets.NewProjector(pool)

	tracedProjectorHandlers := otel.WithEventTelemetry(projector.EventHandlers(), otel.WithOperation("WalletListProjector"))
	if err := eventBus.Subscribe(ctx, "wallet-list-projector", tracedProjectorHandlers); err != nil {
		log.Fatal(err)
	}

	openWalletHandler := walletopening.NewHandler(tracedEventStore)
	tracedOpenWalletHandler := otel.WithCommandTelemetry(openWalletHandler, otel.WithOperation("OpenWallet"))
	openWalletHTTP := walletopening.NewHTTPHandler(tracedOpenWalletHandler)

	closeMonthWalletHandler := closemonthwallet.NewHandler(tracedEventStore)
	tracedCloseMonthWalletHandler := otel.WithCommandTelemetry(closeMonthWalletHandler, otel.WithOperation("CloseMonthWallet"))
	closeMonthWalletHTTP := closemonthwallet.NewHTTPHandler(tracedCloseMonthWalletHandler)

	listWalletsQueryHandler := listwallets.NewQueryHandler(projector)
	tracedListWalletsQueryHandler := otel.WithQueryTelemetry(listWalletsQueryHandler, otel.WithOperation("ListWallets"))
	listWalletsHTTP := listwallets.NewHTTPHandler(tracedListWalletsQueryHandler)

	r := gin.Default()
	wallets := r.Group("/api/v1/wallets")
	openWalletHTTP.RegisterRoutes(wallets)
	closeMonthWalletHTTP.RegisterRoutes(wallets)
	listWalletsHTTP.RegisterRoutes(wallets)

	freezeWalletHandler := walletfreezing.NewHandler(tracedEventStore)
	tracedFreezeWalletHandler := otel.WithCommandTelemetry(freezeWalletHandler, otel.WithOperation("FreezeWallet"))
	freezeWalletProcessor := walletfreezing.NewProcessor(tracedFreezeWalletHandler, 5*time.Second)

	tracedProcessorHandlers := otel.WithEventTelemetry(freezeWalletProcessor.EventHandlers(), otel.WithOperation("FreezeWalletProcessor"))
	if err := eventBus.Subscribe(ctx, "freeze-wallet-processor", tracedProcessorHandlers); err != nil {
		log.Fatal(err)
	}

	log.Fatal(r.Run(":8080"))
}

func initTracer() (*sdktrace.TracerProvider, error) {
	var exporter sdktrace.SpanExporter
	var err error

	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelEndpoint != "" {
		ctx := context.Background()
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(otelEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, err
		}
		log.Printf("Using OTLP exporter with endpoint: %s", otelEndpoint)
	} else {
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, err
		}
		log.Println("Using stdout exporter (pretty print)")
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName("wallet-management"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	return tp, nil
}
