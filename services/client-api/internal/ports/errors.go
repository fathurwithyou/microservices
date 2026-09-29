package ports

import "errors"

var (
	ErrInvalidCredentials = errors.New("invalid client credentials")
	ErrInvalidToken       = errors.New("invalid token")
	ErrNotFound           = errors.New("not found")
	ErrUnavailable        = errors.New("upstream unavailable")
	ErrUpstreamFailure    = errors.New("upstream request failed")
)
