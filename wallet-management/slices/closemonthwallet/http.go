package closemonthwallet

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/terraskye/eventsourcing"
)

type HTTPHandler struct {
	handler eventsourcing.CommandHandler[CloseMonthWallet]
}

func NewHTTPHandler(h eventsourcing.CommandHandler[CloseMonthWallet]) *HTTPHandler {
	return &HTTPHandler{handler: h}
}

type request struct {
	Amount int `json:"amount" binding:"required"`
	Month  int `json:"month" binding:"required"`
	Year   int `json:"year" binding:"required"`
}

func (h *HTTPHandler) Handle(c *gin.Context) {
	walletID, err := uuid.Parse(c.Param("walletID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet ID"})
		return
	}

	var req request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cmd := CloseMonthWallet{
		WalletID: walletID,
		Month:    req.Month,
		Year:     req.Year,
		ClosedBy: uuid.New(),
	}

	if _, err := h.handler(c.Request.Context(), cmd); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *HTTPHandler) RegisterRoutes(g *gin.RouterGroup) {
	g.POST("/:walletID/close-month", h.Handle)
}
