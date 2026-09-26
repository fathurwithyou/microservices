package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type environment struct {
	apiBaseURL      string
	bmkgBaseURL     string
	pvmbgBaseURL    string
	bmkgAPIKey      string
	pvmbgToken      string
	pvmbgAdminKey   string
	mediaSecret     string
	fieldTeamSecret string
	bnpbOpsSecret   string
}

type tokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type httpStatusObservation struct {
	Step       string `json:"step"`
	StatusCode int    `json:"status_code"`
}

func TestP1SchemaEvolutionKeepsPVMBGHazardsValid(t *testing.T) {
	env := loadEnvironment(t)
	fieldToken := issueToken(t, env, "field-team-client", env.fieldTeamSecret)
	postJSON(t, env.pvmbgBaseURL+"/admin/schema-version", map[string]any{"enabled": true}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)

	var sample map[string]any
	eventually(t, 15*time.Second, func() bool {
		response := getJSON(t, env.apiBaseURL+"/hazards?source=PVMBG&raw=true", fieldToken.AccessToken, http.StatusOK)
		sample = findAttributeSample(response, "confidence_level")
		return sample != nil
	})
	writeEvidence(t, "p1-schema-sample.json", map[string]any{
		"problem":       "P1",
		"observation":   "raw PVMBG HazardEvent after runtime schema change",
		"sample_record": sample,
	})
}

func TestP1CanonicalMappingForBothSources(t *testing.T) {
	env := loadEnvironment(t)

	var bmkgSample, pvmbgSample map[string]any
	eventually(t, 10*time.Second, func() bool {
		fieldToken := issueToken(t, env, "field-team-client", env.fieldTeamSecret)
		bmkg := getJSON(t, env.apiBaseURL+"/hazards?source=BMKG&raw=true&page_size=50", fieldToken.AccessToken, http.StatusOK)
		pvmbg := getJSON(t, env.apiBaseURL+"/hazards?source=PVMBG&raw=true&page_size=50", fieldToken.AccessToken, http.StatusOK)
		bmkgSample = findCanonicalMapping(bmkg, "SOURCE_BMKG", "HAZARD_TYPE_SEISMIC", "magnitude", "depth_km", "potential_tsunami")
		pvmbgSample = findCanonicalMapping(pvmbg, "SOURCE_PVMBG", "HAZARD_TYPE_VOLCANIC", "eruption_count_24h", "ash_column_height_m")
		return bmkgSample != nil && pvmbgSample != nil
	})
	writeEvidence(t, "p1-canonical-samples.json", map[string]any{
		"problem": "P1",
		"samples": map[string]any{
			"BMKG":  bmkgSample,
			"PVMBG": pvmbgSample,
		},
	})
}

func findCanonicalMapping(response map[string]any, source, hazardType string, requiredAttributes ...string) map[string]any {
	hazards, ok := response["hazards"].([]any)
	if !ok {
		return nil
	}
	for _, value := range hazards {
		hazard, ok := value.(map[string]any)
		if !ok || hazard["source"] != source || hazard["hazard_type"] != hazardType {
			continue
		}
		validCanonicalFields := true
		for _, key := range []string{"hazard_id", "source_ref_id", "area_name", "occurred_at", "ingested_at"} {
			field, ok := hazard[key].(string)
			if !ok || strings.TrimSpace(field) == "" {
				validCanonicalFields = false
				break
			}
		}
		if !validCanonicalFields {
			continue
		}
		severity, ok := hazard["severity"].(string)
		if !ok || !strings.HasPrefix(severity, "SEVERITY_") || severity == "SEVERITY_UNSPECIFIED" {
			continue
		}
		if _, ok := hazard["latitude"]; !ok {
			continue
		}
		if _, ok := hazard["longitude"]; !ok {
			continue
		}
		attributes, ok := hazard["attributes"].(map[string]any)
		if !ok {
			continue
		}
		allAttributesPresent := true
		for _, key := range requiredAttributes {
			if _, ok := attributes[key]; !ok {
				allAttributesPresent = false
				break
			}
		}
		if allAttributesPresent {
			sample := make(map[string]any)
			for _, key := range []string{
				"hazard_id", "source", "source_ref_id", "hazard_type", "severity",
				"area_name", "latitude", "longitude", "occurred_at", "ingested_at",
			} {
				sample[key] = hazard[key]
			}
			sampleAttributes := make(map[string]any, len(requiredAttributes))
			for _, key := range requiredAttributes {
				sampleAttributes[key] = attributes[key]
			}
			sample["attributes"] = sampleAttributes
			return sample
		}
	}
	return nil
}

func TestP2PVMBGOutageKeepsBMKGAvailable(t *testing.T) {
	env := loadEnvironment(t)
	fieldToken := issueToken(t, env, "field-team-client", env.fieldTeamSecret)
	observations := map[string]any{
		"problem":  "P2",
		"scenario": "PVMBG outage and recovery",
	}
	t.Cleanup(func() { writeEvidence(t, "p2-outage-sequence.json", observations) })
	statusSequence := make([]httpStatusObservation, 0, 6)
	_, enabledStatus := postJSONWithStatus(t, env.pvmbgBaseURL+"/admin/outage", map[string]any{"enabled": true}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "enable PVMBG outage", enabledStatus)
	t.Cleanup(func() {
		postJSON(t, env.pvmbgBaseURL+"/admin/outage", map[string]any{"enabled": false}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)
	})

	var staleStatus map[string]any
	var staleStatusCode int
	eventually(t, 10*time.Second, func() bool {
		status, code := getJSONWithStatus(t, env.apiBaseURL+"/sources/status?source=PVMBG", fieldToken.AccessToken, http.StatusOK)
		if status["status"] != "stale" || status["stale_since"] == "" {
			return false
		}
		staleStatus = map[string]any{"status": status["status"], "stale_since": status["stale_since"]}
		staleStatusCode = code
		return true
	})
	recordHTTPStatus(observations, &statusSequence, "PVMBG source status is stale", staleStatusCode)
	bmkgResponse, bmkgStatus := getJSONWithStatus(t, env.apiBaseURL+"/hazards?source=BMKG", fieldToken.AccessToken, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "read BMKG hazards during outage", bmkgStatus)
	pvmbgResponse, pvmbgStatus := getJSONWithStatus(t, env.apiBaseURL+"/hazards?source=PVMBG", fieldToken.AccessToken, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "read stored PVMBG hazards during outage", pvmbgStatus)

	_, disabledStatus := postJSONWithStatus(t, env.pvmbgBaseURL+"/admin/outage", map[string]any{"enabled": false}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "disable PVMBG outage", disabledStatus)
	var healthyStatus map[string]any
	var healthyStatusCode int
	eventually(t, 10*time.Second, func() bool {
		status, code := getJSONWithStatus(t, env.apiBaseURL+"/sources/status?source=PVMBG", fieldToken.AccessToken, http.StatusOK)
		if status["status"] != "healthy" {
			return false
		}
		healthyStatus = map[string]any{"status": status["status"]}
		healthyStatusCode = code
		return true
	})
	recordHTTPStatus(observations, &statusSequence, "PVMBG source status recovered", healthyStatusCode)
	observations["stale_observation"] = staleStatus
	observations["recovered_observation"] = healthyStatus
	observations["bmkg_hazards_returned"] = hazardCount(bmkgResponse)
	observations["pvmbg_hazards_returned"] = hazardCount(pvmbgResponse)
}

func TestP3IndependentCredentialsAndRefreshRotation(t *testing.T) {
	env := loadEnvironment(t)
	observations := map[string]any{"problem": "P3"}
	statusSequence := make([]httpStatusObservation, 0, 10)
	t.Cleanup(func() { writeEvidence(t, "p3-auth-statuses.json", observations) })
	_, bmkgWithPVMBGStatus := getWithStatus(t, env.bmkgBaseURL+"/seismic-events", map[string]string{"X-BMKG-Key": env.pvmbgToken}, http.StatusUnauthorized)
	recordHTTPStatus(observations, &statusSequence, "PVMBG credential presented to BMKG", bmkgWithPVMBGStatus)
	_, pvmbgWithBMKGStatus := getWithStatus(t, env.pvmbgBaseURL+"/volcanic-reports", map[string]string{"Authorization": "Bearer " + env.bmkgAPIKey}, http.StatusUnauthorized)
	recordHTTPStatus(observations, &statusSequence, "BMKG credential presented to PVMBG", pvmbgWithBMKGStatus)

	mediaToken, mediaTokenStatus := issueTokenWithStatus(t, env, "media-client", env.mediaSecret)
	recordHTTPStatus(observations, &statusSequence, "issue Media token", mediaTokenStatus)
	_, mediaRawStatus := getWithStatus(t, env.apiBaseURL+"/hazards?raw=true", map[string]string{"Authorization": "Bearer " + mediaToken.AccessToken}, http.StatusForbidden)
	recordHTTPStatus(observations, &statusSequence, "Media requests raw hazards", mediaRawStatus)
	summary, summaryStatus := getWithStatus(t, env.apiBaseURL+"/hazards", map[string]string{"Authorization": "Bearer " + mediaToken.AccessToken}, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "Media reads summary hazards", summaryStatus)
	mediaSummaryLeaksRawFields := leakedRawFields(summary)
	if mediaSummaryLeaksRawFields {
		t.Fatal("media summary payload leaked raw fields")
	}

	opsToken, opsTokenStatus := issueTokenWithStatus(t, env, "bnpb-ops-client", env.bnpbOpsSecret)
	recordHTTPStatus(observations, &statusSequence, "issue BNPB Ops token", opsTokenStatus)
	raw, opsRawStatus := getJSONWithStatus(t, env.apiBaseURL+"/hazards?raw=true&page_size=1", opsToken.AccessToken, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "BNPB Ops reads raw hazards", opsRawStatus)
	opsRawFieldsPresent := containsRawShape(raw)
	if !opsRawFieldsPresent {
		t.Fatal("bnpb-ops raw payload must include coordinates or attributes")
	}

	fieldToken, fieldTokenStatus := issueTokenWithStatus(t, env, "field-team-client", env.fieldTeamSecret)
	recordHTTPStatus(observations, &statusSequence, "issue Field Team token", fieldTokenStatus)
	accessTokenTTL, err := time.ParseDuration(requiredEnvironment(t, "ACCESS_TOKEN_TTL"))
	if err != nil {
		t.Fatalf("parse ACCESS_TOKEN_TTL: %v", err)
	}
	tokenIssuedAt := time.Now()
	time.Sleep(accessTokenTTL + 100*time.Millisecond)
	_, expiredAccessStatus := getWithStatus(t, env.apiBaseURL+"/hazards", map[string]string{"Authorization": "Bearer " + fieldToken.AccessToken}, http.StatusUnauthorized)
	recordHTTPStatus(observations, &statusSequence, "expired Field Team access token", expiredAccessStatus)
	rotated, refreshStatus := refreshTokenWithStatus(t, env, fieldToken.RefreshToken, http.StatusOK)
	recordHTTPStatus(observations, &statusSequence, "refresh Field Team token", refreshStatus)
	if rotated.AccessToken == "" || rotated.RefreshToken == "" || rotated.RefreshToken == fieldToken.RefreshToken {
		t.Fatal("refresh must issue a distinct usable token pair")
	}
	_, oldRefreshStatus := refreshTokenWithStatus(t, env, fieldToken.RefreshToken, http.StatusUnauthorized)
	recordHTTPStatus(observations, &statusSequence, "reuse rotated Field Team refresh token", oldRefreshStatus)
	observations["access_token_ttl"] = accessTokenTTL.String()
	observations["elapsed_since_field_token_issue_ms"] = time.Since(tokenIssuedAt).Milliseconds()
	observations["media_summary_contains_raw_fields"] = mediaSummaryLeaksRawFields
	observations["bnpb_ops_raw_fields_present"] = opsRawFieldsPresent
	observations["refresh_pair_rotated"] = rotated.RefreshToken != fieldToken.RefreshToken
}

func recordHTTPStatus(observations map[string]any, sequence *[]httpStatusObservation, step string, statusCode int) {
	*sequence = append(*sequence, httpStatusObservation{Step: step, StatusCode: statusCode})
	observations["status_sequence"] = *sequence
}

func loadEnvironment(t *testing.T) environment {
	t.Helper()
	if os.Getenv("CLIENT_API_URL") == "" {
		t.Skip("set CLIENT_API_URL and E2E credentials to run against a live Compose stack")
	}
	return environment{
		apiBaseURL:      requiredEnvironment(t, "CLIENT_API_URL"),
		bmkgBaseURL:     requiredEnvironment(t, "BMKG_E2E_URL"),
		pvmbgBaseURL:    requiredEnvironment(t, "PVMBG_E2E_URL"),
		bmkgAPIKey:      requiredEnvironment(t, "BMKG_API_KEY"),
		pvmbgToken:      requiredEnvironment(t, "PVMBG_TOKEN"),
		pvmbgAdminKey:   requiredEnvironment(t, "PVMBG_ADMIN_KEY"),
		mediaSecret:     requiredEnvironment(t, "MEDIA_CLIENT_SECRET"),
		fieldTeamSecret: requiredEnvironment(t, "FIELD_TEAM_CLIENT_SECRET"),
		bnpbOpsSecret:   requiredEnvironment(t, "BNPB_OPS_CLIENT_SECRET"),
	}
}

func requiredEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for e2e tests", name)
	}
	return value
}

func issueToken(t *testing.T, env environment, clientID, secret string) tokenPair {
	pair, _ := issueTokenWithStatus(t, env, clientID, secret)
	return pair
}

func issueTokenWithStatus(t *testing.T, env environment, clientID, secret string) (tokenPair, int) {
	t.Helper()
	response, status := postJSONWithStatus(t, env.apiBaseURL+"/auth/token", map[string]any{"client_id": clientID, "client_secret": secret}, nil, http.StatusOK)
	return decodeTokenPair(t, response), status
}

func refreshTokenWithStatus(t *testing.T, env environment, refreshToken string, expectedStatus int) (tokenPair, int) {
	t.Helper()
	response, status := postJSONWithStatus(t, env.apiBaseURL+"/auth/refresh", map[string]any{"refresh_token": refreshToken}, nil, expectedStatus)
	if expectedStatus != http.StatusOK {
		return tokenPair{}, status
	}
	return decodeTokenPair(t, response), status
}

func decodeTokenPair(t *testing.T, value map[string]any) tokenPair {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var pair tokenPair
	if err := json.Unmarshal(encoded, &pair); err != nil {
		t.Fatal(err)
	}
	return pair
}

func getJSON(t *testing.T, url, accessToken string, expectedStatus int) map[string]any {
	response, _ := getJSONWithStatus(t, url, accessToken, expectedStatus)
	return response
}

func getJSONWithStatus(t *testing.T, url, accessToken string, expectedStatus int) (map[string]any, int) {
	t.Helper()
	headers := map[string]string{}
	if accessToken != "" {
		headers["Authorization"] = "Bearer " + accessToken
	}
	return getWithStatus(t, url, headers, expectedStatus)
}

func get(t *testing.T, url string, headers map[string]string, expectedStatus int) map[string]any {
	response, _ := getWithStatus(t, url, headers, expectedStatus)
	return response
}

func getWithStatus(t *testing.T, url string, headers map[string]string, expectedStatus int) (map[string]any, int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return executeJSONWithStatus(t, request, expectedStatus)
}

func postJSON(t *testing.T, url string, payload map[string]any, headers map[string]string, expectedStatus int) map[string]any {
	response, _ := postJSONWithStatus(t, url, payload, headers, expectedStatus)
	return response
}

func postJSONWithStatus(t *testing.T, url string, payload map[string]any, headers map[string]string, expectedStatus int) (map[string]any, int) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return executeJSONWithStatus(t, request, expectedStatus)
}

func executeJSONWithStatus(t *testing.T, request *http.Request, expectedStatus int) (map[string]any, int) {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expectedStatus {
		t.Fatalf("%s %s: status = %d, want %d", request.Method, request.URL, response.StatusCode, expectedStatus)
	}
	if len(body) == 0 {
		return map[string]any{}, response.StatusCode
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return result, response.StatusCode
}

func TestFindAttributeSampleKeepsOnlyTheObservedAttribute(t *testing.T) {
	response := map[string]any{
		"hazards": []any{map[string]any{
			"hazard_id":    "sample-event",
			"source":       "SOURCE_PVMBG",
			"access_token": "must-not-be-captured",
			"attributes": map[string]any{
				"confidence_level": 0.8,
				"unrelated":        "must-not-be-captured",
			},
		}},
	}
	sample := findAttributeSample(response, "confidence_level")
	if sample == nil {
		t.Fatal("expected a sample containing confidence_level")
	}
	attributes, ok := sample["attributes"].(map[string]any)
	if !ok || attributes["confidence_level"] != 0.8 {
		t.Fatalf("sample attributes = %#v, want only the observed confidence_level", sample["attributes"])
	}
	if _, exists := attributes["unrelated"]; exists {
		t.Fatal("sample must omit unrelated attributes")
	}
	if _, exists := sample["access_token"]; exists {
		t.Fatal("sample must omit fields outside the selected evidence")
	}
}

func findAttributeSample(response map[string]any, name string) map[string]any {
	hazards, ok := response["hazards"].([]any)
	if !ok {
		return nil
	}
	for _, value := range hazards {
		hazard, ok := value.(map[string]any)
		if !ok {
			continue
		}
		attributes, ok := hazard["attributes"].(map[string]any)
		if ok && attributes[name] != nil {
			return map[string]any{
				"hazard_id":  hazard["hazard_id"],
				"source":     hazard["source"],
				"attributes": map[string]any{name: attributes[name]},
			}
		}
	}
	return nil
}

func hazardCount(response map[string]any) int {
	hazards, ok := response["hazards"].([]any)
	if !ok {
		return 0
	}
	return len(hazards)
}

func writeEvidence(t *testing.T, name string, value any) {
	t.Helper()
	evidenceDir := os.Getenv("E2E_EVIDENCE_DIR")
	if evidenceDir == "" {
		return
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Errorf("encode E2E evidence %s: %v", name, err)
		return
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Errorf("create E2E evidence directory: %v", err)
		return
	}
	if err := os.WriteFile(filepath.Join(evidenceDir, name), append(data, '\n'), 0o600); err != nil {
		t.Errorf("write E2E evidence %s: %v", name, err)
	}
}

func leakedRawFields(response map[string]any) bool {
	hazards, ok := response["hazards"].([]any)
	if !ok {
		return false
	}
	for _, value := range hazards {
		hazard, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if _, exists := hazard["latitude"]; exists {
			return true
		}
		if _, exists := hazard["longitude"]; exists {
			return true
		}
		if _, exists := hazard["attributes"]; exists {
			return true
		}
		if _, exists := hazard["source_ref_id"]; exists {
			return true
		}
	}
	return false
}

func containsRawShape(response map[string]any) bool {
	hazards, ok := response["hazards"].([]any)
	if !ok {
		return false
	}
	for _, value := range hazards {
		hazard, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if _, exists := hazard["latitude"]; exists {
			return true
		}
		if _, exists := hazard["attributes"]; exists {
			return true
		}
	}
	return false
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}
