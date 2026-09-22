package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
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

func TestP1SchemaEvolutionKeepsPVMBGHazardsValid(t *testing.T) {
	env := loadEnvironment(t)
	fieldToken := issueToken(t, env, "field-team-client", env.fieldTeamSecret)
	postJSON(t, env.pvmbgBaseURL+"/admin/schema-version", map[string]any{"enabled": true}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)

	eventually(t, 15*time.Second, func() bool {
		response := getJSON(t, env.apiBaseURL+"/hazards?source=PVMBG&raw=true", fieldToken.AccessToken, http.StatusOK)
		return containsAttribute(response, "confidence_level")
	})
}

func TestP2PVMBGOutageKeepsBMKGAvailable(t *testing.T) {
	env := loadEnvironment(t)
	fieldToken := issueToken(t, env, "field-team-client", env.fieldTeamSecret)
	postJSON(t, env.pvmbgBaseURL+"/admin/outage", map[string]any{"enabled": true}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)
	t.Cleanup(func() {
		postJSON(t, env.pvmbgBaseURL+"/admin/outage", map[string]any{"enabled": false}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)
	})

	eventually(t, 10*time.Second, func() bool {
		status := getJSON(t, env.apiBaseURL+"/sources/status?source=PVMBG", fieldToken.AccessToken, http.StatusOK)
		return status["status"] == "stale" && status["stale_since"] != ""
	})
	getJSON(t, env.apiBaseURL+"/hazards?source=BMKG", fieldToken.AccessToken, http.StatusOK)
	getJSON(t, env.apiBaseURL+"/hazards?source=PVMBG", fieldToken.AccessToken, http.StatusOK)

	postJSON(t, env.pvmbgBaseURL+"/admin/outage", map[string]any{"enabled": false}, map[string]string{"X-PVMBG-Admin-Key": env.pvmbgAdminKey}, http.StatusOK)
	eventually(t, 10*time.Second, func() bool {
		status := getJSON(t, env.apiBaseURL+"/sources/status?source=PVMBG", fieldToken.AccessToken, http.StatusOK)
		return status["status"] == "healthy"
	})
}

func TestP3IndependentCredentialsAndRefreshRotation(t *testing.T) {
	env := loadEnvironment(t)
	get(t, env.bmkgBaseURL+"/seismic-events", map[string]string{"X-BMKG-Key": env.pvmbgToken}, http.StatusUnauthorized)
	get(t, env.pvmbgBaseURL+"/volcanic-reports", map[string]string{"Authorization": "Bearer " + env.bmkgAPIKey}, http.StatusUnauthorized)

	mediaToken := issueToken(t, env, "media-client", env.mediaSecret)
	get(t, env.apiBaseURL+"/hazards?raw=true", map[string]string{"Authorization": "Bearer " + mediaToken.AccessToken}, http.StatusForbidden)
	summary := get(t, env.apiBaseURL+"/hazards", map[string]string{"Authorization": "Bearer " + mediaToken.AccessToken}, http.StatusOK)
	if leakedRawFields(summary) {
		t.Fatal("media summary payload leaked raw fields")
	}

	opsToken := issueToken(t, env, "bnpb-ops-client", env.bnpbOpsSecret)
	raw := getJSON(t, env.apiBaseURL+"/hazards?raw=true&page_size=1", opsToken.AccessToken, http.StatusOK)
	if !containsRawShape(raw) {
		t.Fatal("bnpb-ops raw payload must include coordinates or attributes")
	}

	fieldToken := issueToken(t, env, "field-team-client", env.fieldTeamSecret)
	time.Sleep(3 * time.Second)
	get(t, env.apiBaseURL+"/hazards", map[string]string{"Authorization": "Bearer " + fieldToken.AccessToken}, http.StatusUnauthorized)
	rotated := refreshToken(t, env, fieldToken.RefreshToken, http.StatusOK)
	if rotated.AccessToken == "" || rotated.RefreshToken == "" || rotated.RefreshToken == fieldToken.RefreshToken {
		t.Fatal("refresh must issue a distinct usable token pair")
	}
	refreshToken(t, env, fieldToken.RefreshToken, http.StatusUnauthorized)
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
		bnpbOpsSecret:    requiredEnvironment(t, "BNPB_OPS_CLIENT_SECRET"),
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
	t.Helper()
	response := postJSON(t, env.apiBaseURL+"/auth/token", map[string]any{"client_id": clientID, "client_secret": secret}, nil, http.StatusOK)
	return decodeTokenPair(t, response)
}

func refreshToken(t *testing.T, env environment, refreshToken string, expectedStatus int) tokenPair {
	t.Helper()
	response := postJSON(t, env.apiBaseURL+"/auth/refresh", map[string]any{"refresh_token": refreshToken}, nil, expectedStatus)
	if expectedStatus != http.StatusOK {
		return tokenPair{}
	}
	return decodeTokenPair(t, response)
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
	t.Helper()
	headers := map[string]string{}
	if accessToken != "" {
		headers["Authorization"] = "Bearer " + accessToken
	}
	return get(t, url, headers, expectedStatus)
}

func get(t *testing.T, url string, headers map[string]string, expectedStatus int) map[string]any {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return executeJSON(t, request, expectedStatus)
}

func postJSON(t *testing.T, url string, payload map[string]any, headers map[string]string, expectedStatus int) map[string]any {
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
	return executeJSON(t, request, expectedStatus)
}

func executeJSON(t *testing.T, request *http.Request, expectedStatus int) map[string]any {
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
		t.Fatalf("%s %s: status = %d, want %d; body = %s", request.Method, request.URL, response.StatusCode, expectedStatus, body)
	}
	if len(body) == 0 {
		return map[string]any{}
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, body)
	}
	return result
}

func containsAttribute(response map[string]any, name string) bool {
	hazards, ok := response["hazards"].([]any)
	if !ok {
		return false
	}
	for _, value := range hazards {
		hazard, ok := value.(map[string]any)
		if !ok {
			continue
		}
		attributes, ok := hazard["attributes"].(map[string]any)
		if ok && attributes[name] != nil {
			return true
		}
	}
	return false
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
