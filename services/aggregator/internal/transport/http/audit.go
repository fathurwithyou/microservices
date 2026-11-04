package httptransport

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/failures"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type auditHandler struct {
	queries  *audit.Queries
	replayer *failures.Replayer
	node     string
	unit     string
}

func newAuditHandler(queries *audit.Queries, replayer *failures.Replayer) *auditHandler {
	node := os.Getenv("STORE_NODE")
	if node == "" {
		node, _ = os.Hostname()
	}
	unit := os.Getenv("STORE_UNIT")
	if unit == "" {
		unit = "primary"
	}
	return &auditHandler{queries: queries, replayer: replayer, node: node, unit: unit}
}

func auditAuth(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if token == "" {
			writeAuditError(response, http.StatusServiceUnavailable, "audit_disabled", "audit token is not configured")
			return
		}
		expected := "Bearer " + token
		if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte(expected)) != 1 {
			writeAuditError(response, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		next(response, request)
	}
}

func (h *auditHandler) events(response http.ResponseWriter, request *http.Request) {
	source, err := auditSource(request.URL.Query().Get("source"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	since, err := auditSince(request.URL.Query().Get("since"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	limit, err := auditLimit(request.URL.Query().Get("limit"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	items, next, err := h.queries.List(request.Context(), audit.Filter{Source: source, Since: since, Limit: limit, Cursor: request.URL.Query().Get("cursor")})
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	payload := make([]auditEvent, 0, len(items))
	for _, item := range items {
		payload = append(payload, auditEvent{
			HazardID: item.HazardID, Source: string(item.Source), SourceRefID: item.SourceRefID,
			HazardType: string(item.HazardType), Severity: string(item.Severity), AreaName: item.AreaName,
			Latitude: item.Latitude, Longitude: item.Longitude,
			OccurredAt: item.OccurredAt.UTC().Format(time.RFC3339), IngestedAt: item.IngestedAt.UTC().Format(time.RFC3339),
			Attributes: item.Attributes,
		})
	}
	writeJSON(response, http.StatusOK, map[string]any{
		"items": payload, "next_cursor": nullableString(next), "partial": false, "unavailable_units": []string{},
	})
}

func (h *auditHandler) checksum(response http.ResponseWriter, request *http.Request) {
	source, err := auditSource(request.URL.Query().Get("source"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	count, checksum, err := h.queries.Checksum(request.Context(), source)
	if err != nil {
		writeAuditError(response, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{
		"source": string(source), "count": count, "checksum": checksum, "algorithm": "sha256",
		"as_of": time.Now().UTC().Format(time.RFC3339), "partial": false, "unavailable_units": []string{},
	})
}

func (h *auditHandler) replay(response http.ResponseWriter, request *http.Request) {
	if h.replayer == nil {
		writeAuditError(response, http.StatusServiceUnavailable, "replay_disabled", "replay is not configured")
		return
	}
	source, err := auditSource(request.URL.Query().Get("source"))
	if err != nil || source == "" {
		writeAuditError(response, http.StatusBadRequest, "bad_request", "source is required")
		return
	}
	limit, err := auditLimit(request.URL.Query().Get("limit"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	recovered, err := h.replayer.Replay(request.Context(), source, limit)
	if err != nil {
		writeAuditError(response, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"source": source, "recovered": recovered})
}

func (h *auditHandler) unprocessed(response http.ResponseWriter, request *http.Request) {
	source, err := auditSource(request.URL.Query().Get("source"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	limit, err := auditLimit(request.URL.Query().Get("limit"))
	if err != nil {
		writeAuditError(response, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	items, err := h.queries.Unprocessed(request.Context(), source, limit)
	if err != nil {
		writeAuditError(response, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	payload := make([]map[string]any, 0, len(items))
	for _, item := range items {
		payload = append(payload, map[string]any{
			"message_id": item.MessageID, "source": item.Source, "received_at": item.ReceivedAt.UTC().Format(time.RFC3339),
			"reason": item.Reason, "retryable": item.Retryable, "attempts": item.Attempts, "status": item.Status,
		})
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": payload, "next_cursor": nil})
}

func (h *auditHandler) location(response http.ResponseWriter, request *http.Request) {
	hazardID := request.URL.Query().Get("hazard_id")
	if hazardID == "" {
		writeAuditError(response, http.StatusBadRequest, "bad_request", "hazard_id is required")
		return
	}
	exists, err := h.queries.Exists(request.Context(), hazardID)
	if err != nil {
		writeAuditError(response, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !exists {
		writeAuditError(response, http.StatusNotFound, "not_found", "hazard_id not found")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{
		"hazard_id": hazardID,
		"locations": []map[string]string{{"node": h.node, "unit": h.unit, "role": "primary"}},
		"partial":   false,
	})
}

type auditEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

func auditSource(value string) (domain.Source, error) {
	switch domain.Source(value) {
	case "", domain.SourceBMKG, domain.SourcePVMBG, domain.SourceSatellite, domain.SourceCoastalSensor:
		return domain.Source(value), nil
	default:
		return "", &auditError{message: "unknown source"}
	}
}

func auditSince(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, &auditError{message: "since must be RFC3339"}
	}
	return parsed, nil
}

func auditLimit(value string) (int, error) {
	if value == "" {
		return 100, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 1000 {
		return 0, &auditError{message: "limit must be between 1 and 1000"}
	}
	return limit, nil
}

type auditError struct{ message string }

func (e *auditError) Error() string { return e.message }

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func writeAuditError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}
