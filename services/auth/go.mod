module github.com/fathurwithyou/microservices/services/auth

go 1.26.0

require (
	github.com/fathurwithyou/microservices/contracts v0.0.0
	github.com/fathurwithyou/microservices/pkg v0.0.0
	github.com/jackc/pgx/v5 v5.11.0
	golang.org/x/crypto v0.57.0
	google.golang.org/grpc v1.83.2
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260831171406-18b4a7587f8a // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/fathurwithyou/microservices/contracts => ../../contracts

replace github.com/fathurwithyou/microservices/pkg => ../../pkg
