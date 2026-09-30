package listwallets

import "context"

type ListWallets struct{}

func (q ListWallets) ID() []byte { return []byte("wallet-list") }

type QueryHandler struct {
	projector *Projector
}

func NewQueryHandler(p *Projector) *QueryHandler {
	return &QueryHandler{projector: p}
}

func (h *QueryHandler) HandleQuery(_ context.Context, _ ListWallets) ([]Wallet, error) {
	return h.projector.All(), nil
}
