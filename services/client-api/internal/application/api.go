// Package application coordinates client authentication, authorization,
// upstream error handling, and hazard projections for the HTTP gateway.
package application

import (
	"context"
	"errors"

	"github.com/fathurwithyou/microservices/contracts/scopes"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"github.com/fathurwithyou/microservices/services/client-api/internal/projection"
)

type API struct {
	auth       ports.Auth
	aggregator ports.Aggregator
}

// Principal is the authenticated identity used by application authorization.
type Principal struct {
	ClientID string
	Scopes   []string
}

// HazardList is the HTTP-facing page after summary or raw projection.
type HazardList struct {
	Hazards       []any  `json:"hazards"`
	NextPageToken string `json:"next_page_token"`
}

// NewAPI returns the application service backed by the auth and aggregator
// boundaries.
func NewAPI(auth ports.Auth, aggregator ports.Aggregator) *API {
	return &API{auth: auth, aggregator: aggregator}
}

func (a *API) IssueToken(ctx context.Context, clientID, secret string) (*ports.TokenPair, error) {
	pair, err := a.auth.Issue(ctx, clientID, secret)
	if err != nil {
		return nil, upstreamError(err)
	}
	return pair, nil
}

func (a *API) RefreshToken(ctx context.Context, refreshToken string) (*ports.TokenPair, error) {
	pair, err := a.auth.Refresh(ctx, refreshToken)
	if err != nil {
		return nil, upstreamError(err)
	}
	return pair, nil
}

// Authenticate validates an access token and returns its principal.
func (a *API) Authenticate(ctx context.Context, accessToken string) (Principal, error) {
	validated, err := a.auth.Validate(ctx, accessToken)
	if err != nil {
		return Principal{}, upstreamError(err)
	}
	if validated == nil || !validated.Valid {
		return Principal{}, ports.ErrInvalidToken
	}
	return Principal{ClientID: validated.ClientID, Scopes: append([]string(nil), validated.Scopes...)}, nil
}

func (a *API) ListHazards(ctx context.Context, principal Principal, filter ports.HazardFilter) (HazardList, error) {
	if err := authorize(principal, filter.Raw); err != nil {
		return HazardList{}, err
	}
	page, err := a.aggregator.List(ctx, filter)
	if err != nil {
		return HazardList{}, upstreamError(err)
	}
	hazards := make([]any, 0, len(page.Hazards))
	for _, hazard := range page.Hazards {
		if filter.Raw {
			hazards = append(hazards, projection.ToRaw(hazard))
		} else {
			hazards = append(hazards, projection.ToSummary(hazard))
		}
	}
	return HazardList{Hazards: hazards, NextPageToken: page.NextPageToken}, nil
}

func (a *API) GetHazard(ctx context.Context, principal Principal, hazardID string, raw bool) (any, error) {
	if err := authorize(principal, raw); err != nil {
		return nil, err
	}
	hazard, err := a.aggregator.GetHazard(ctx, hazardID, raw)
	if err != nil {
		return nil, upstreamError(err)
	}
	if raw {
		return projection.ToRaw(hazard), nil
	}
	return projection.ToSummary(hazard), nil
}

func (a *API) SourceStatus(ctx context.Context, principal Principal, source string) (ports.SourceStatus, error) {
	if err := authorize(principal, false); err != nil {
		return ports.SourceStatus{}, err
	}
	status, err := a.aggregator.Status(ctx, source)
	if err != nil {
		return ports.SourceStatus{}, upstreamError(err)
	}
	return status, nil
}

func authorize(principal Principal, raw bool) error {
	required := scopes.HazardReadSummary
	if raw {
		required = scopes.HazardReadRaw
	}
	for _, scope := range principal.Scopes {
		if scope == required {
			return nil
		}
	}
	return ErrForbidden
}

var ErrForbidden = errors.New("required scope is missing")

func upstreamError(err error) error {
	if errors.Is(err, ports.ErrInvalidCredentials) || errors.Is(err, ports.ErrInvalidToken) || errors.Is(err, ports.ErrNotFound) || errors.Is(err, ports.ErrUnavailable) || errors.Is(err, ports.ErrUpstreamFailure) {
		return err
	}
	return errors.Join(ports.ErrUpstreamFailure, err)
}
