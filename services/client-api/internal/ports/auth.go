package ports

import (
	"context"

	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
)

type Auth interface {
	Issue(context.Context, string, string) (*authpb.TokenPair, error)
	Refresh(context.Context, string) (*authpb.TokenPair, error)
	Validate(context.Context, string) (*authpb.ValidatedToken, error)
}
