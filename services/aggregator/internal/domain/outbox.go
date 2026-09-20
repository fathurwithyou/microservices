package domain

type OutboxMessage struct {
	ID            int64
	HazardID      string
	CorrelationID string
	RoutingKey    string
	Payload       []byte
	Attempts      int
	LeaseToken    string
}
