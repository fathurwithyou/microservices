package httptransport

import (
	"net/http"

	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"github.com/gin-gonic/gin"
)

type fieldReportRequest struct {
	ReportID    string  `json:"report_id" binding:"required"`
	HazardType  string  `json:"hazard_type" binding:"required"`
	Severity    string  `json:"severity" binding:"required"`
	AreaName    string  `json:"area_name" binding:"required"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	OccurredAt  string  `json:"occurred_at" binding:"required"`
	Description string  `json:"description"`
}

func (h handler) submitFieldReport(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	request := fieldReportRequest{}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "report_id, hazard_type, severity, area_name, and occurred_at are required"})
		return
	}
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	report, err := h.api.SubmitFieldReport(ctx, principal, ports.FieldReportInput{
		ReportID:    request.ReportID,
		HazardType:  request.HazardType,
		Severity:    request.Severity,
		AreaName:    request.AreaName,
		Latitude:    request.Latitude,
		Longitude:   request.Longitude,
		OccurredAt:  request.OccurredAt,
		Description: request.Description,
	})
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, report)
}
