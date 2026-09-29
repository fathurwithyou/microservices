package httptransport

import (
	"net/http"

	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"github.com/gin-gonic/gin"
)

func (h handler) listHazards(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	filter := listFilter(c)
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	page, err := h.api.ListHazards(ctx, principal, filter)
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"hazards": page.Hazards, "next_page_token": page.NextPageToken})
}

func (h handler) getHazard(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	raw := requestsRawData(c)
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	event, err := h.api.GetHazard(ctx, principal, c.Param("hazard_id"), raw)
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusOK, event)
}

func (h handler) sourceStatus(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	status, err := h.api.SourceStatus(ctx, principal, requestedStatusSource(c))
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusOK, status)
}

// listFilter builds the aggregator filter from query parameters.
func listFilter(c *gin.Context) ports.HazardFilter {
	raw := requestsRawData(c)
	return ports.HazardFilter{
		Source:     requestedSource(c),
		HazardType: requestedHazardType(c),
		Severity:   c.Query("severity"),
		PageSize:   int32(pageSize(c.Query("page_size"))),
		PageToken:  c.Query("page_token"),
		Raw:        raw,
	}
}

// requestedSource returns the source filter, or an empty string when the
// caller did not restrict it.
func requestedSource(c *gin.Context) string {
	switch c.Query("source") {
	case "BMKG", "PVMBG":
		return c.Query("source")
	default:
		return ""
	}
}

// requestedHazardType returns the hazard type filter, or an empty string
// when the caller did not restrict it.
func requestedHazardType(c *gin.Context) string {
	switch c.Query("hazard_type") {
	case "SEISMIC", "VOLCANIC":
		return c.Query("hazard_type")
	default:
		return ""
	}
}

// requestedStatusSource returns the source for the status endpoint. Unlike
// the list filter, an absent parameter means "unspecified" rather than
// silently querying a single feed.
func requestedStatusSource(c *gin.Context) string {
	return requestedSource(c)
}
