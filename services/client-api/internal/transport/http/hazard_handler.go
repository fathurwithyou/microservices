package httptransport

import (
	"net/http"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	grpcadapter "github.com/fathurwithyou/microservices/services/client-api/internal/adapters/grpc"
	"github.com/fathurwithyou/microservices/services/client-api/internal/authorization"
	"github.com/fathurwithyou/microservices/services/client-api/internal/projection"
	"github.com/gin-gonic/gin"
)

func (h handler) listHazards(c *gin.Context) {
	raw := requestsRawData(c)
	if raw && !authorization.Has(requestScopes(c), authorization.RawScope) {
		c.JSON(http.StatusForbidden, gin.H{"error": "raw scope required"})
		return
	}
	ctx, cancel := h.requestContext(c)
	defer cancel()
	response, err := h.api.ListHazards(grpcadapter.WithCorrelationID(ctx, c.GetString("correlation_id")), listRequest(c, raw))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "aggregator unavailable"})
		return
	}
	c.JSON(http.StatusOK, hazardListResponse(response.GetHazards(), response.GetNextPageToken(), raw))
}

func (h handler) getHazard(c *gin.Context) {
	raw := requestsRawData(c)
	if raw && !authorization.Has(requestScopes(c), authorization.RawScope) {
		c.JSON(http.StatusForbidden, gin.H{"error": "raw scope required"})
		return
	}
	ctx, cancel := h.requestContext(c)
	defer cancel()
	event, err := h.api.GetHazard(grpcadapter.WithCorrelationID(ctx, c.GetString("correlation_id")), &aggregatorpb.GetHazardRequest{HazardId: c.Param("hazard_id"), Raw: raw})
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "hazard not found"})
		return
	}
	if raw {
		c.JSON(http.StatusOK, projection.ToRaw(event))
		return
	}
	c.JSON(http.StatusOK, projection.ToSummary(event))
}

func (h handler) sourceStatus(c *gin.Context) {
	ctx, cancel := h.requestContext(c)
	defer cancel()
	status, err := h.api.SourceStatus(grpcadapter.WithCorrelationID(ctx, c.GetString("correlation_id")), &aggregatorpb.GetSourceStatusRequest{Source: requestedStatusSource(c)})
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "aggregator unavailable"})
		return
	}
	c.JSON(http.StatusOK, status)
}

func listRequest(c *gin.Context, raw bool) *aggregatorpb.ListHazardsRequest {
	request := &aggregatorpb.ListHazardsRequest{Raw: raw, PageSize: int32(pageSize(c.Query("page_size"))), PageToken: c.Query("page_token"), Source: requestedSource(c)}
	switch c.Query("hazard_type") {
	case "SEISMIC":
		request.HazardType = eventspb.HazardType_HAZARD_TYPE_SEISMIC
	case "VOLCANIC":
		request.HazardType = eventspb.HazardType_HAZARD_TYPE_VOLCANIC
	}
	return request
}

func requestedSource(c *gin.Context) eventspb.Source {
	if c.Query("source") == "PVMBG" {
		return eventspb.Source_SOURCE_PVMBG
	}
	if c.Query("source") == "BMKG" {
		return eventspb.Source_SOURCE_BMKG
	}
	return eventspb.Source_SOURCE_UNSPECIFIED
}

func requestedStatusSource(c *gin.Context) eventspb.Source {
	if c.Query("source") == "PVMBG" {
		return eventspb.Source_SOURCE_PVMBG
	}
	return eventspb.Source_SOURCE_BMKG
}

func hazardListResponse(events []*eventspb.HazardEvent, nextPageToken string, raw bool) gin.H {
	if raw {
		values := make([]map[string]any, 0, len(events))
		for _, event := range events {
			values = append(values, projection.ToRaw(event))
		}
		return gin.H{"hazards": values, "next_page_token": nextPageToken}
	}
	values := make([]projection.Summary, 0, len(events))
	for _, event := range events {
		values = append(values, projection.ToSummary(event))
	}
	return gin.H{"hazards": values, "next_page_token": nextPageToken}
}
