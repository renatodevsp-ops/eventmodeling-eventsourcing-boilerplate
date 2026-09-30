package walletopening

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/terraskye/eventsourcing"
)

type HTTPHandler struct {
	handler eventsourcing.CommandHandler[OpenWallet]
}

func NewHTTPHandler(handler eventsourcing.CommandHandler[OpenWallet]) *HTTPHandler {
	return &HTTPHandler{handler: handler}
}

type request struct {
	Amount int `json:"amount" binding:"required"`
}

func (h *HTTPHandler) Handle(c *gin.Context) {
	var req request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cmd := OpenWallet{
		WalletID:  uuid.New(),
		Amount:    req.Amount,
		CreatedBy: uuid.New(),
	}

	result, err := h.handler(c.Request.Context(), cmd)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"wallet_id": cmd.WalletID,
		"version":   result.NextExpectedVersion,
	})
}

func (h *HTTPHandler) RegisterRoutes(g *gin.RouterGroup) {
	g.POST("", h.Handle)
}
