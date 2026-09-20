package domain

import "time"

type AccessToken struct {
	Hash      []byte
	ClientID  string
	Scopes    []string
	ExpiresAt time.Time
}

type RefreshToken struct {
	Hash      []byte
	ClientID  string
	Scopes    []string
	ExpiresAt *time.Time
}

type TokenOwner struct {
	ClientID string
	Scopes   []string
}
