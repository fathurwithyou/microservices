package httptransport

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

type queueSnapshot struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
	Ready     int    `json:"ready"`
}

func (h handler) queues(c *gin.Context) {
	names := h.cfg.QueueNames
	ctx := c.Request.Context()
	correlationID := c.GetString(correlation.GinKey)
	snapshots := make([]queueSnapshot, len(names))
	var waitGroup sync.WaitGroup
	for index, name := range names {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			snapshot, err := h.readQueue(ctx, correlationID, name)
			if err != nil {
				snapshot = queueSnapshot{Name: name}
			}
			snapshots[index] = snapshot
		}()
	}
	waitGroup.Wait()
	c.JSON(http.StatusOK, gin.H{"checked_at": h.now().UTC().Format(time.RFC3339), "queues": snapshots})
}

func (h handler) readQueue(ctx context.Context, correlationID, name string) (queueSnapshot, error) {
	url := strings.TrimRight(h.cfg.RabbitManagementURL, "/") + "/api/queues/%2F/" + name
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return queueSnapshot{}, err
	}
	request.SetBasicAuth(h.cfg.RabbitUser, h.cfg.RabbitPassword)
	request.Header.Set(correlation.Header, correlationID)
	response, err := h.client.Do(request)
	if err != nil {
		return queueSnapshot{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return queueSnapshot{}, errStatus(response.StatusCode)
	}
	var payload struct {
		Name                   string `json:"name"`
		Messages               int    `json:"messages"`
		Consumers              int    `json:"consumers"`
		MessagesReady          int    `json:"messages_ready"`
		MessagesUnacknowledged int    `json:"messages_unacknowledged"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return queueSnapshot{}, err
	}
	return queueSnapshot{Name: payload.Name, Messages: payload.Messages, Consumers: payload.Consumers, Ready: payload.MessagesReady}, nil
}

type statusError int

func errStatus(code int) error { return statusError(code) }
func (s statusError) Error() string {
	return http.StatusText(int(s))
}
