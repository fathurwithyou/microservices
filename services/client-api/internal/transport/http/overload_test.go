package httptransport

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestOverloadGateRejectsPastLimitAndKeepsHealthUp(t *testing.T) {
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	router := gin.New()
	router.Use(overloadGate(2, nil))
	router.GET("/slow", func(c *gin.Context) { entered <- struct{}{}; <-release; c.Status(http.StatusOK) })
	router.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/slow", nil))
			if recorder.Code != http.StatusOK {
				t.Errorf("admitted request = %d", recorder.Code)
			}
		}()
	}
	<-entered
	<-entered

	over := httptest.NewRecorder()
	router.ServeHTTP(over, httptest.NewRequest(http.MethodGet, "/slow", nil))
	if over.Code != http.StatusTooManyRequests || over.Header().Get("Retry-After") == "" {
		t.Fatalf("third request = %d (Retry-After %q), want 429 with Retry-After", over.Code, over.Header().Get("Retry-After"))
	}
	health := httptest.NewRecorder()
	router.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("/health while overloaded = %d, want 200", health.Code)
	}

	close(release)
	wg.Wait()
	after := httptest.NewRecorder()
	router.ServeHTTP(after, httptest.NewRequest(http.MethodGet, "/health", nil))
	ok := httptest.NewRecorder()
	go func() { <-entered }()
	router.ServeHTTP(ok, httptest.NewRequest(http.MethodGet, "/slow", nil))
	if ok.Code == http.StatusTooManyRequests {
		t.Fatal("gate did not release its slots")
	}
}

func TestOverloadGateLetsRecognisedFieldCredentialsThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	priority := newPriorityTokens()
	priority.mark("Bearer field")
	release := make(chan struct{})
	router := gin.New()
	router.Use(overloadGate(1, priority))
	router.GET("/hazards", func(c *gin.Context) {
		if c.Query("hold") == "1" {
			<-release
		}
		c.Status(http.StatusOK)
	})
	server := httptest.NewServer(router)
	defer server.Close()
	held := make(chan struct{})
	go func() {
		response, err := http.Get(server.URL + "/hazards?hold=1")
		if err == nil {
			response.Body.Close()
		}
		close(held)
	}()
	time.Sleep(100 * time.Millisecond) // the gate is now at its limit

	call := func(authorization string) int {
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/hazards", nil)
		request.Header.Set("Authorization", authorization)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	if got := call("Bearer media"); got != http.StatusTooManyRequests {
		t.Errorf("unknown credential past the limit = %d, want 429", got)
	}
	if got := call("Bearer field"); got != http.StatusOK {
		t.Errorf("recognised Field Team credential past the limit = %d, want 200", got)
	}
	close(release)
	<-held
}
