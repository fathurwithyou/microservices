package httptransport

import (
	"net/http"
	"strings"
	"time"

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
	if message, ok := normalizeFieldReport(&request); !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": message})
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

var (
	fieldReportHazardTypes = map[string]bool{"SEISMIC": true, "VOLCANIC": true, "WILDFIRE": true, "COASTAL": true}
	fieldReportSeverities  = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}
)

// normalizeFieldReport upper-cases enum values and rejects bad input here, so
// a client mistake is a fast 400 instead of an upstream error.
func normalizeFieldReport(request *fieldReportRequest) (string, bool) {
	request.HazardType = strings.ToUpper(strings.TrimSpace(request.HazardType))
	request.Severity = strings.ToUpper(strings.TrimSpace(request.Severity))
	if !fieldReportHazardTypes[request.HazardType] {
		return "hazard_type must be one of SEISMIC, VOLCANIC, WILDFIRE, COASTAL", false
	}
	if !fieldReportSeverities[request.Severity] {
		return "severity must be one of NORMAL, WASPADA, SIAGA, AWAS", false
	}
	if _, err := time.Parse(time.RFC3339, request.OccurredAt); err != nil {
		return "occurred_at must be RFC3339 with a timezone, for example 2026-11-15T10:00:00+07:00", false
	}
	if request.Latitude < -90 || request.Latitude > 90 || request.Longitude < -180 || request.Longitude > 180 {
		return "latitude must be within [-90, 90] and longitude within [-180, 180]", false
	}
	return "", true
}
