package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
	"github.com/fathurwithyou/microservices/services/auth/internal/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) EnsureClients(ctx context.Context, clients map[string]domain.Client) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin client seed transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	clientIDs := make([]string, 0, len(clients))
	for clientID := range clients {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Strings(clientIDs)
	for _, clientID := range clientIDs {
		client := clients[clientID]
		secretHash, err := bcrypt.GenerateFromPassword([]byte(client.Secret), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("hash secret for %s: %w", clientID, err)
		}
		scopes, err := json.Marshal(client.Scopes)
		if err != nil {
			return fmt.Errorf("encode scopes for %s: %w", clientID, err)
		}
		_, err = tx.Exec(ctx, `
INSERT INTO auth_clients (client_id, secret_hash, scopes)
VALUES ($1, $2, $3)
ON CONFLICT (client_id) DO UPDATE
SET secret_hash = EXCLUDED.secret_hash,
    scopes = EXCLUDED.scopes`, clientID, secretHash, scopes)
		if err != nil {
			return fmt.Errorf("seed client %s: %w", clientID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit client seed transaction: %w", err)
	}
	return nil
}

func (s *Store) AuthenticateClient(ctx context.Context, clientID, secret string) (domain.Client, error) {
	var storedHash []byte
	var scopesJSON []byte
	if err := s.pool.QueryRow(ctx, `
SELECT secret_hash, scopes
FROM auth_clients
WHERE client_id = $1`, clientID).Scan(&storedHash, &scopesJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Client{}, ports.ErrInvalidCredentials
		}
		return domain.Client{}, fmt.Errorf("load client %s: %w", clientID, err)
	}
	if err := bcrypt.CompareHashAndPassword(storedHash, []byte(secret)); err != nil {
		return domain.Client{}, ports.ErrInvalidCredentials
	}
	var scopes []string
	if err := json.Unmarshal(scopesJSON, &scopes); err != nil {
		return domain.Client{}, fmt.Errorf("decode scopes for %s: %w", clientID, err)
	}
	return domain.Client{ID: clientID, Scopes: scopes}, nil
}

func (s *Store) PersistTokenPair(ctx context.Context, access domain.AccessToken, refresh domain.RefreshToken) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin token pair transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := insertAccessToken(ctx, tx, access); err != nil {
		return err
	}
	if err := insertRefreshToken(ctx, tx, refresh); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit token pair transaction: %w", err)
	}
	return nil
}

func (s *Store) ValidateAccessToken(ctx context.Context, tokenHash []byte, now time.Time) (domain.AccessToken, error) {
	var token domain.AccessToken
	var scopesJSON []byte
	if err := s.pool.QueryRow(ctx, `
SELECT client_id, scopes, expires_at
FROM access_tokens
WHERE token_hash = $1
  AND revoked_at IS NULL
  AND expires_at > $2`, tokenHash, now).Scan(&token.ClientID, &scopesJSON, &token.ExpiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.AccessToken{}, ports.ErrInvalidToken
		}
		return domain.AccessToken{}, fmt.Errorf("validate access token: %w", err)
	}
	if err := json.Unmarshal(scopesJSON, &token.Scopes); err != nil {
		return domain.AccessToken{}, fmt.Errorf("decode access token scopes: %w", err)
	}
	token.Hash = append([]byte(nil), tokenHash...)
	return token, nil
}

func (s *Store) RotateRefreshToken(ctx context.Context, oldHash []byte, access domain.AccessToken, refresh domain.RefreshToken) (domain.TokenOwner, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.TokenOwner{}, fmt.Errorf("begin refresh rotation transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var owner domain.TokenOwner
	var scopesJSON []byte
	var expiresAt *time.Time
	var revokedAt *time.Time
	if err := tx.QueryRow(ctx, `
SELECT client_id, scopes, expires_at, revoked_at
FROM refresh_tokens
WHERE token_hash = $1
FOR UPDATE`, oldHash).Scan(&owner.ClientID, &scopesJSON, &expiresAt, &revokedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.TokenOwner{}, ports.ErrInvalidToken
		}
		return domain.TokenOwner{}, fmt.Errorf("load refresh token: %w", err)
	}
	if revokedAt != nil || expiresAt != nil && !expiresAt.After(time.Now()) {
		return domain.TokenOwner{}, ports.ErrInvalidToken
	}
	if err := json.Unmarshal(scopesJSON, &owner.Scopes); err != nil {
		return domain.TokenOwner{}, fmt.Errorf("decode refresh token scopes: %w", err)
	}

	result, err := tx.Exec(ctx, `
UPDATE refresh_tokens
SET revoked_at = now()
WHERE token_hash = $1 AND revoked_at IS NULL`, oldHash)
	if err != nil {
		return domain.TokenOwner{}, fmt.Errorf("revoke refresh token: %w", err)
	}
	if result.RowsAffected() != 1 {
		return domain.TokenOwner{}, ports.ErrInvalidToken
	}
	access.ClientID = owner.ClientID
	access.Scopes = append([]string(nil), owner.Scopes...)
	refresh.ClientID = owner.ClientID
	refresh.Scopes = append([]string(nil), owner.Scopes...)
	if err := insertAccessToken(ctx, tx, access); err != nil {
		return domain.TokenOwner{}, err
	}
	if err := insertRefreshToken(ctx, tx, refresh); err != nil {
		return domain.TokenOwner{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.TokenOwner{}, fmt.Errorf("commit refresh rotation transaction: %w", err)
	}
	return owner, nil
}

func insertAccessToken(ctx context.Context, tx pgx.Tx, token domain.AccessToken) error {
	scopes, err := json.Marshal(token.Scopes)
	if err != nil {
		return fmt.Errorf("encode access token scopes: %w", err)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO access_tokens (token_hash, client_id, scopes, expires_at)
VALUES ($1, $2, $3, $4)`, token.Hash, token.ClientID, scopes, token.ExpiresAt)
	if err != nil {
		return fmt.Errorf("store access token: %w", err)
	}
	return nil
}

func insertRefreshToken(ctx context.Context, tx pgx.Tx, token domain.RefreshToken) error {
	scopes, err := json.Marshal(token.Scopes)
	if err != nil {
		return fmt.Errorf("encode refresh token scopes: %w", err)
	}
	var expiresAt any
	if token.ExpiresAt != nil {
		expiresAt = *token.ExpiresAt
	}
	_, err = tx.Exec(ctx, `
INSERT INTO refresh_tokens (token_hash, client_id, scopes, expires_at)
VALUES ($1, $2, $3, $4)`, token.Hash, token.ClientID, scopes, expiresAt)
	if err != nil {
		return fmt.Errorf("store refresh token: %w", err)
	}
	return nil
}
