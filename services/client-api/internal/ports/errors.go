package ports

import "errors"

var (
	ErrInvalidCredentials = errors.New("invalid client credentials")
	ErrInvalidToken       = errors.New("invalid token")
	ErrNotFound           = errors.New("not found")
	ErrUnavailable        = errors.New("upstream unavailable")
	ErrUpstreamFailure    = errors.New("upstream request failed")
	// ErrInvalidArgument is a client mistake reported by an upstream service;
	// it must surface as 400, never as a 5xx.
	ErrInvalidArgument = errors.New("invalid request")
)
